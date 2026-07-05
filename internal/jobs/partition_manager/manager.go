package partitionmanager

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"samarth/payment-service/internal/ports"
)

type Partition struct {
	Name    string
	WeekEnd time.Time
}

type Store interface {
	AcquireLock(ctx context.Context) (bool, error)
	ReleaseLock(ctx context.Context) error
	CreatePartition(ctx context.Context, name string, start, end time.Time) error
	ListPartitions(ctx context.Context) ([]string, error)
	CountUnpublished(ctx context.Context, partition string) (int, error)
	DetachPartition(ctx context.Context, name string) error
	DropPartition(ctx context.Context, name string) error
	LogAction(ctx context.Context, name, action string) error
	PartitionsDetachedBefore(ctx context.Context, cutoff time.Time) ([]string, error)
}

type Config struct {
	WeeksAhead     int
	RetentionWeeks int
	DropAfter      time.Duration
}

type Manager struct {
	store Store
	log   ports.Logger
	cfg   Config
	now   func() time.Time
}

func New(store Store, log ports.Logger, cfg Config) *Manager {
	if cfg.WeeksAhead <= 0 {
		cfg.WeeksAhead = 2
	}
	if cfg.RetentionWeeks <= 0 {
		cfg.RetentionWeeks = 2
	}
	if cfg.DropAfter <= 0 {
		cfg.DropAfter = 14 * 24 * time.Hour
	}
	return &Manager{store: store, log: log, cfg: cfg, now: func() time.Time { return time.Now().UTC() }}
}

func (m *Manager) RunOnce(ctx context.Context) error {
	ok, err := m.store.AcquireLock(ctx)
	if err != nil {
		return fmt.Errorf("partition_manager: acquire lock: %w", err)
	}
	if !ok {
		m.log.Debug(ports.LogEventPartitionManagementSkipped, map[string]any{"reason": "lock_held"})
		return nil
	}
	defer func() { _ = m.store.ReleaseLock(ctx) }()

	m.preCreate(ctx)
	m.detachStale(ctx)
	m.dropDetached(ctx)
	return nil
}

func (m *Manager) preCreate(ctx context.Context) {
	base := weekStart(m.now())
	for i := 0; i <= m.cfg.WeeksAhead; i++ {
		start := base.AddDate(0, 0, 7*i)
		end := start.AddDate(0, 0, 7)
		name := partitionName(start)
		if err := m.store.CreatePartition(ctx, name, start, end); err != nil {
			m.log.Error(ports.LogEventPartitionCreated, map[string]any{
				ports.FieldErrorCode:     "partition_precreate_failed",
				ports.FieldTraceID:       "",
				ports.FieldTransactionID: "",
				ports.FieldPartitionName: name,
			}, err)
			continue
		}
		m.log.Info(ports.LogEventPartitionCreated, map[string]any{ports.FieldPartitionName: name})
	}
}

func (m *Manager) detachStale(ctx context.Context) {
	names, err := m.store.ListPartitions(ctx)
	if err != nil {
		m.log.Error(ports.LogEventPartitionDetached, map[string]any{
			ports.FieldErrorCode: "list_partitions_failed", ports.FieldTraceID: "", ports.FieldTransactionID: "",
		}, err)
		return
	}

	cutoff := weekStart(m.now()).AddDate(0, 0, -7*m.cfg.RetentionWeeks)
	for _, name := range names {
		weekEnd, ok := weekEndFromName(name)
		if !ok || !weekEnd.Before(cutoff) {
			continue
		}

		unpublished, err := m.store.CountUnpublished(ctx, name)
		if err != nil {
			m.log.Error(ports.LogEventPartitionDetached, map[string]any{
				ports.FieldErrorCode: "count_unpublished_failed", ports.FieldTraceID: "", ports.FieldTransactionID: "", ports.FieldPartitionName: name,
			}, err)
			continue
		}
		if unpublished > 0 {
			m.log.Warn(ports.LogEventPartitionDetached, map[string]any{
				ports.FieldPartitionName: name, "skipped": true, "unpublished": unpublished,
			})
			continue
		}

		if err := m.store.DetachPartition(ctx, name); err != nil {
			m.log.Error(ports.LogEventPartitionDetached, map[string]any{
				ports.FieldErrorCode: "detach_failed", ports.FieldTraceID: "", ports.FieldTransactionID: "", ports.FieldPartitionName: name,
			}, err)
			continue
		}
		_ = m.store.LogAction(ctx, name, "detach")
		m.log.Info(ports.LogEventPartitionDetached, map[string]any{ports.FieldPartitionName: name})
	}
}

func (m *Manager) dropDetached(ctx context.Context) {
	names, err := m.store.PartitionsDetachedBefore(ctx, m.now().Add(-m.cfg.DropAfter))
	if err != nil {
		m.log.Error(ports.LogEventPartitionDropped, map[string]any{
			ports.FieldErrorCode: "list_detached_failed", ports.FieldTraceID: "", ports.FieldTransactionID: "",
		}, err)
		return
	}
	for _, name := range names {
		if err := m.store.DropPartition(ctx, name); err != nil {
			m.log.Error(ports.LogEventPartitionDropped, map[string]any{
				ports.FieldErrorCode: "drop_failed", ports.FieldTraceID: "", ports.FieldTransactionID: "", ports.FieldPartitionName: name,
			}, err)
			continue
		}
		_ = m.store.LogAction(ctx, name, "drop")
		m.log.Info(ports.LogEventPartitionDropped, map[string]any{ports.FieldPartitionName: name})
	}
}

var partitionNameRe = regexp.MustCompile(`^outbox_\d{4}_W\d{2}$`)

func ValidPartitionName(name string) bool {
	return partitionNameRe.MatchString(name)
}

func weekStart(t time.Time) time.Time {
	t = t.UTC()
	daysSinceMonday := (int(t.Weekday()) + 6) % 7
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return d.AddDate(0, 0, -daysSinceMonday)
}

func partitionName(weekStart time.Time) string {
	y, w := weekStart.ISOWeek()
	return fmt.Sprintf("outbox_%04d_W%02d", y, w)
}

func weekEndFromName(name string) (time.Time, bool) {
	var y, w int
	if _, err := fmt.Sscanf(name, "outbox_%d_W%d", &y, &w); err != nil {
		return time.Time{}, false
	}
	return isoWeekStart(y, w).AddDate(0, 0, 7), true
}

func isoWeekStart(year, week int) time.Time {
	jan4 := time.Date(year, 1, 4, 0, 0, 0, 0, time.UTC)
	daysSinceMonday := (int(jan4.Weekday()) + 6) % 7
	week1Monday := jan4.AddDate(0, 0, -daysSinceMonday)
	return week1Monday.AddDate(0, 0, (week-1)*7)
}
