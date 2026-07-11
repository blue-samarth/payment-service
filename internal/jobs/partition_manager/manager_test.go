package partitionmanager

import (
	"context"
	"testing"
	"time"

	"samarth/payment-service/internal/ports"
)

type fakeStore struct {
	lockOK       bool
	created      []string
	partitions   []string
	unpublished  map[string]int
	detached     []string
	dropped      []string
	logged       [][2]string
	detachedList []string
}

func (f *fakeStore) AcquireLock(context.Context) (bool, error) { return f.lockOK, nil }
func (f *fakeStore) ReleaseLock(context.Context) error         { return nil }
func (f *fakeStore) CreatePartition(_ context.Context, name string, _, _ time.Time) error {
	f.created = append(f.created, name)
	return nil
}
func (f *fakeStore) ListPartitions(context.Context) ([]string, error) { return f.partitions, nil }
func (f *fakeStore) CountUnpublished(_ context.Context, p string) (int, error) {
	return f.unpublished[p], nil
}
func (f *fakeStore) DetachPartition(_ context.Context, name string) error {
	f.detached = append(f.detached, name)
	return nil
}
func (f *fakeStore) DropPartition(_ context.Context, name string) error {
	f.dropped = append(f.dropped, name)
	return nil
}
func (f *fakeStore) LogAction(_ context.Context, name, action string) error {
	f.logged = append(f.logged, [2]string{name, action})
	return nil
}
func (f *fakeStore) PartitionsDetachedBefore(context.Context, time.Time) ([]string, error) {
	return f.detachedList, nil
}

type noopLogger struct{}

func (noopLogger) Info(string, map[string]any)         {}
func (noopLogger) Warn(string, map[string]any)         {}
func (noopLogger) Error(string, map[string]any, error) {}
func (noopLogger) Debug(string, map[string]any)        {}
func (noopLogger) Trace(string, map[string]any)        {}
func (l noopLogger) With(map[string]any) ports.Logger  { return l }

func newManager(store Store, cfg Config, now time.Time) *Manager {
	m := New(store, noopLogger{}, cfg)
	m.now = func() time.Time { return now }
	return m
}

func TestWeekStart_MondayAligned(t *testing.T) {
	// 2026-06-27 is a Saturday; its week starts Monday 2026-06-22.
	got := weekStart(time.Date(2026, 6, 27, 15, 0, 0, 0, time.UTC))
	want := time.Date(2026, 6, 22, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("weekStart = %s, want %s", got, want)
	}
}

func TestPartitionNameRoundTrip(t *testing.T) {
	start := weekStart(time.Date(2026, 6, 27, 0, 0, 0, 0, time.UTC))
	name := partitionName(start)
	end, ok := weekEndFromName(name)
	if !ok {
		t.Fatalf("could not parse %q", name)
	}
	if !end.Equal(start.AddDate(0, 0, 7)) {
		t.Errorf("weekEndFromName(%s)=%s, want %s", name, end, start.AddDate(0, 0, 7))
	}
}

func TestPreCreate_CreatesWindow(t *testing.T) {
	store := &fakeStore{lockOK: true}
	m := newManager(store, Config{WeeksAhead: 2}, time.Date(2026, 6, 27, 0, 0, 0, 0, time.UTC))

	if err := m.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.created) != 3 {
		t.Errorf("expected 3 partitions pre-created (current + 2 ahead), got %v", store.created)
	}
	if store.created[0] != "outbox_2026_W26" {
		t.Errorf("expected current week outbox_2026_W26 first, got %s", store.created[0])
	}
}

func TestRunOnce_SkipsWithoutLock(t *testing.T) {
	store := &fakeStore{lockOK: false}
	m := newManager(store, Config{}, time.Now())
	if err := m.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.created) != 0 {
		t.Error("expected no work when advisory lock not acquired")
	}
}

func TestDetachStale_OnlyEmptyAndOld(t *testing.T) {
	now := time.Date(2026, 6, 27, 0, 0, 0, 0, time.UTC)
	store := &fakeStore{
		lockOK:     true,
		partitions: []string{"outbox_2026_W10", "outbox_2026_W11", "outbox_2026_W26"},
		unpublished: map[string]int{
			"outbox_2026_W11": 3, // old but still has unpublished -> skip
		},
	}
	m := newManager(store, Config{WeeksAhead: 0, RetentionWeeks: 2}, now)
	if err := m.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(store.detached) != 1 || store.detached[0] != "outbox_2026_W10" {
		t.Errorf("expected only the old empty partition detached, got %v", store.detached)
	}
}

func TestDropDetached(t *testing.T) {
	store := &fakeStore{lockOK: true, detachedList: []string{"outbox_2025_W50"}}
	m := newManager(store, Config{WeeksAhead: 0}, time.Now())
	if err := m.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.dropped) != 1 || store.dropped[0] != "outbox_2025_W50" {
		t.Errorf("expected the detached partition dropped, got %v", store.dropped)
	}
}
