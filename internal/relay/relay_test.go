package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/ports"
)

type markRecord struct {
	published []uuid.UUID
	failed    []uuid.UUID
	exhausted []uuid.UUID
}

type fakeOutbox struct {
	pending []ports.PendingEvent
	polls   int
	marks   markRecord
}

func (f *fakeOutbox) PollPending(ctx context.Context, shardMin, shardMax, batchSize int) ([]ports.PendingEvent, error) {
	f.polls++
	if f.polls > 1 {
		return nil, nil
	}
	return f.pending, nil
}
func (f *fakeOutbox) MarkPublished(ctx context.Context, id uuid.UUID, createdAt time.Time) error {
	f.marks.published = append(f.marks.published, id)
	return nil
}
func (f *fakeOutbox) MarkFailed(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string, nextAttempt time.Time) error {
	f.marks.failed = append(f.marks.failed, id)
	return nil
}
func (f *fakeOutbox) MarkExhausted(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string) error {
	f.marks.exhausted = append(f.marks.exhausted, id)
	return nil
}

type fakePublisher struct {
	err       error
	published []uuid.UUID
}

func (p *fakePublisher) Publish(ctx context.Context, event ports.PendingEvent) error {
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, event.ID)
	return nil
}

type noopLogger struct{}

func (noopLogger) Info(string, map[string]any)         {}
func (noopLogger) Warn(string, map[string]any)         {}
func (noopLogger) Error(string, map[string]any, error) {}
func (noopLogger) Debug(string, map[string]any)        {}
func (noopLogger) Trace(string, map[string]any)        {}
func (l noopLogger) With(map[string]any) ports.Logger  { return l }

type noopMetrics struct{}

func (noopMetrics) Increment(string, map[string]string)          {}
func (noopMetrics) Histogram(string, float64, map[string]string) {}
func (noopMetrics) Gauge(string, float64, map[string]string)     {}

func event(attempts int) ports.PendingEvent {
	return ports.PendingEvent{ID: uuid.New(), AggregateID: uuid.New(), EventType: "PAYMENT_CREATED", Attempts: attempts, CreatedAt: time.Now()}
}

func newWorker(outbox OutboxReader, pub Publisher, cfg Config) *Worker {
	return NewWorker(outbox, pub, noopLogger{}, noopMetrics{}, cfg)
}

func TestRunOnce_PublishesAndMarks(t *testing.T) {
	outbox := &fakeOutbox{pending: []ports.PendingEvent{event(0), event(0)}}
	pub := &fakePublisher{}
	w := newWorker(outbox, pub, Config{ShardMin: 0, ShardMax: 63, BatchSize: 50, MaxAttempts: 5})

	n, published, err := w.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("expected 2 events processed, got %d", n)
	}
	if published != 2 {
		t.Errorf("expected 2 events reported as published progress, got %d", published)
	}
	if len(pub.published) != 2 {
		t.Errorf("expected 2 published, got %d", len(pub.published))
	}
	if len(outbox.marks.published) != 2 {
		t.Errorf("expected 2 marked published, got %d", len(outbox.marks.published))
	}
}

func TestRunOnce_FailureMarksFailedWithRetriesRemaining(t *testing.T) {
	outbox := &fakeOutbox{pending: []ports.PendingEvent{event(0)}}
	pub := &fakePublisher{err: errors.New("sns down")}
	w := newWorker(outbox, pub, Config{MaxAttempts: 5})

	if _, _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(outbox.marks.failed) != 1 {
		t.Errorf("expected 1 marked failed, got %d", len(outbox.marks.failed))
	}
	if len(outbox.marks.exhausted) != 0 {
		t.Errorf("expected 0 exhausted, got %d", len(outbox.marks.exhausted))
	}
}

func TestRunOnce_FailureOnLastAttemptExhausts(t *testing.T) {
	outbox := &fakeOutbox{pending: []ports.PendingEvent{event(4)}}
	pub := &fakePublisher{err: errors.New("sns down")}
	w := newWorker(outbox, pub, Config{MaxAttempts: 5})

	if _, _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(outbox.marks.exhausted) != 1 {
		t.Errorf("expected 1 exhausted (attempt 4+1 == maxAttempts 5), got %d", len(outbox.marks.exhausted))
	}
	if len(outbox.marks.failed) != 0 {
		t.Errorf("expected 0 marked failed, got %d", len(outbox.marks.failed))
	}
}

func TestRunOnce_PollError(t *testing.T) {
	w := newWorker(&errOutbox{}, &fakePublisher{}, Config{})
	if _, _, err := w.RunOnce(context.Background()); err == nil {
		t.Fatal("expected poll error to propagate")
	}
}

type errOutbox struct{ fakeOutbox }

func (errOutbox) PollPending(ctx context.Context, a, b, c int) ([]ports.PendingEvent, error) {
	return nil, errors.New("db down")
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	outbox := &fakeOutbox{}
	w := newWorker(outbox, &fakePublisher{}, Config{PollInterval: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := w.Run(ctx); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// alwaysFullOutbox returns a full batch on every poll and counts the polls.
type alwaysFullOutbox struct {
	fakeOutbox
	batch  int
	nPolls int
}

func (f *alwaysFullOutbox) PollPending(ctx context.Context, a, b, c int) ([]ports.PendingEvent, error) {
	f.nPolls++
	out := make([]ports.PendingEvent, f.batch)
	for i := range out {
		out[i] = event(0)
	}
	return out, nil
}

func TestRun_ThrottlesWhenFullBatchMakesNoProgress(t *testing.T) {
	outbox := &alwaysFullOutbox{batch: 3}
	pub := &fakePublisher{err: errors.New("sns down")}
	w := newWorker(outbox, pub, Config{BatchSize: 3, MaxAttempts: 5, PollInterval: 50 * time.Millisecond})

	// A full batch that published nothing must fall through to PollInterval, so
	// within one interval window only the first poll runs. A busy-drain bug would
	// re-poll immediately and rack up many polls against the dead publisher.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	_ = w.Run(ctx)

	if outbox.nPolls > 3 {
		t.Errorf("full-batch-no-progress must throttle to PollInterval; polled %d times in 25ms", outbox.nPolls)
	}
}

func TestBackoff_Caps(t *testing.T) {
	w := newWorker(&fakeOutbox{}, &fakePublisher{}, Config{BaseBackoff: time.Second, MaxBackoff: 10 * time.Second})

	// Equal jitter: each attempt returns a value in [d/2, d], where d is the
	// capped exponential delay, so a batch of failures does not re-poll in lockstep.
	cases := []struct {
		attempt  int
		min, max time.Duration
	}{
		{0, 500 * time.Millisecond, time.Second}, // d = 1s
		{2, 2 * time.Second, 4 * time.Second},    // d = 4s
		{20, 5 * time.Second, 10 * time.Second},  // d capped to MaxBackoff
		{100, 5 * time.Second, 10 * time.Second}, // exponent capped, no shift overflow
	}
	for _, c := range cases {
		for i := 0; i < 50; i++ {
			got := w.backoff(c.attempt)
			if got < c.min || got > c.max {
				t.Fatalf("attempt %d: backoff %v outside [%v, %v]", c.attempt, got, c.min, c.max)
			}
		}
	}
}
