package leaseexpiry_test

import . "samarth/payment-service/internal/jobs/lease_expiry"

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type fakeLister struct {
	ids []uuid.UUID
	err error
}

func (f fakeLister) ListExpiredLeaseIDs(context.Context) ([]uuid.UUID, error) {
	return f.ids, f.err
}

type fakeRecoverer struct {
	called  []uuid.UUID
	failFor map[uuid.UUID]bool
}

func (f *fakeRecoverer) RecoverExpiredLease(_ context.Context, id uuid.UUID) (*transaction.Transaction, error) {
	f.called = append(f.called, id)
	if f.failFor[id] {
		return nil, errors.New("recover failed")
	}
	return &transaction.Transaction{ID: id, Status: transaction.StatusSucceeded}, nil
}

type fakeSweeper struct {
	staleArg    time.Duration
	sweepCalls  int
	expireCalls int
	stale       int64
	expired     int64
	sweepErr    error
	expireErr   error
}

func (f *fakeSweeper) SweepStaleProcessing(_ context.Context, olderThan time.Duration) (int64, error) {
	f.sweepCalls++
	f.staleArg = olderThan
	return f.stale, f.sweepErr
}

func (f *fakeSweeper) DeleteExpired(context.Context) (int64, error) {
	f.expireCalls++
	return f.expired, f.expireErr
}

type noopLogger struct{}

func (noopLogger) Info(string, map[string]any)         {}
func (noopLogger) Warn(string, map[string]any)         {}
func (noopLogger) Error(string, map[string]any, error) {}
func (noopLogger) Debug(string, map[string]any)        {}
func (noopLogger) Trace(string, map[string]any)        {}
func (l noopLogger) With(map[string]any) ports.Logger  { return l }

func TestReaper_RecoversEachExpiredLease(t *testing.T) {
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	rec := &fakeRecoverer{}
	sweeper := &fakeSweeper{}
	r := New(fakeLister{ids: ids}, rec, sweeper, noopLogger{}, Config{})

	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rec.called) != len(ids) {
		t.Errorf("expected every expired lease recovered, got %d of %d", len(rec.called), len(ids))
	}
}

func TestReaper_PerItemFailureDoesNotAbortSweep(t *testing.T) {
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	rec := &fakeRecoverer{failFor: map[uuid.UUID]bool{ids[1]: true}}
	sweeper := &fakeSweeper{}
	r := New(fakeLister{ids: ids}, rec, sweeper, noopLogger{}, Config{})

	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rec.called) != 3 {
		t.Errorf("a single recovery failure must not stop the others, recovered %d", len(rec.called))
	}
	if sweeper.sweepCalls != 1 || sweeper.expireCalls != 1 {
		t.Errorf("idempotency sweep should still run after lease recovery, sweep=%d expire=%d", sweeper.sweepCalls, sweeper.expireCalls)
	}
}

func TestReaper_ListErrorStillSweepsIdempotency(t *testing.T) {
	rec := &fakeRecoverer{}
	sweeper := &fakeSweeper{}
	r := New(fakeLister{err: errors.New("db down")}, rec, sweeper, noopLogger{}, Config{})

	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rec.called) != 0 {
		t.Error("no recovery should be attempted when listing fails")
	}
	if sweeper.sweepCalls != 1 {
		t.Error("idempotency sweep must still run even if lease listing fails")
	}
}

func TestReaper_DefaultIdempotencyTimeout(t *testing.T) {
	sweeper := &fakeSweeper{}
	r := New(fakeLister{}, &fakeRecoverer{}, sweeper, noopLogger{}, Config{})

	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sweeper.staleArg != 5*time.Minute {
		t.Errorf("expected default 5m stale-processing timeout, got %s", sweeper.staleArg)
	}
}

func TestReaper_HonoursConfiguredTimeout(t *testing.T) {
	sweeper := &fakeSweeper{}
	r := New(fakeLister{}, &fakeRecoverer{}, sweeper, noopLogger{}, Config{IdempotencyProcessingTimeout: 90 * time.Second})

	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sweeper.staleArg != 90*time.Second {
		t.Errorf("expected configured 90s timeout, got %s", sweeper.staleArg)
	}
}

type fakeReviewCounter struct {
	pending int64
	err     error
}

func (f fakeReviewCounter) CountPendingReview(context.Context) (int64, error) {
	return f.pending, f.err
}

type gaugeMetrics struct {
	gauges map[string]float64
}

func (m *gaugeMetrics) Increment(string, map[string]string)          {}
func (m *gaugeMetrics) Histogram(string, float64, map[string]string) {}
func (m *gaugeMetrics) Gauge(name string, v float64, _ map[string]string) {
	m.gauges[name] = v
}

func TestReaper_ReportsPendingReviewsAsAGauge(t *testing.T) {
	r := New(fakeLister{}, &fakeRecoverer{}, &fakeSweeper{}, noopLogger{}, Config{})
	metrics := &gaugeMetrics{gauges: map[string]float64{}}
	r.SetReviewGauge(fakeReviewCounter{pending: 3}, metrics)

	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, ok := metrics.gauges[ports.MetricTransactionReviewPending]; !ok || got != 3 {
		t.Fatalf("expected the pending-review gauge at 3 so an alert can fire on rows awaiting ops, got %v (set=%v)", got, ok)
	}
}

func TestReaper_PendingReviewCountFailureDoesNotStopTheSweep(t *testing.T) {
	sweeper := &fakeSweeper{}
	r := New(fakeLister{}, &fakeRecoverer{}, sweeper, noopLogger{}, Config{})
	metrics := &gaugeMetrics{gauges: map[string]float64{}}
	r.SetReviewGauge(fakeReviewCounter{err: errors.New("db down")}, metrics)

	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := metrics.gauges[ports.MetricTransactionReviewPending]; ok {
		t.Error("a failed count must not publish a misleading value")
	}
	if sweeper.sweepCalls != 1 {
		t.Error("the idempotency sweep must still run")
	}
}
