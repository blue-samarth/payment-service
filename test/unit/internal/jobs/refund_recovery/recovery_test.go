package refundrecovery_test

import . "samarth/payment-service/internal/jobs/refund_recovery"

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/refund"
	"samarth/payment-service/internal/ports"
)

type fakeLister struct {
	ids     []uuid.UUID
	minAge  time.Duration
	limit   int
	pending int64
}

func (f *fakeLister) ListRecoverableRefundIDs(_ context.Context, olderThan time.Duration, limit int) ([]uuid.UUID, error) {
	f.minAge, f.limit = olderThan, limit
	return f.ids, nil
}
func (f *fakeLister) CountPendingReview(context.Context) (int64, error) { return f.pending, nil }

type fakeRecoverer struct {
	fail  map[uuid.UUID]bool
	calls []uuid.UUID
}

func (f *fakeRecoverer) RecoverRefund(_ context.Context, id uuid.UUID) (*refund.Refund, error) {
	f.calls = append(f.calls, id)
	if f.fail[id] {
		return nil, errors.New("gateway down")
	}
	return &refund.Refund{ID: id, Status: refund.StatusRefunded}, nil
}

type noopLogger struct{}

func (noopLogger) Info(string, map[string]any)         {}
func (noopLogger) Warn(string, map[string]any)         {}
func (noopLogger) Error(string, map[string]any, error) {}
func (noopLogger) Debug(string, map[string]any)        {}
func (noopLogger) Trace(string, map[string]any)        {}
func (l noopLogger) With(map[string]any) ports.Logger  { return l }

type recordingMetrics struct {
	counts map[string]int
	gauges map[string]float64
}

func (m *recordingMetrics) Increment(name string, _ map[string]string)   { m.counts[name]++ }
func (m *recordingMetrics) Histogram(string, float64, map[string]string) {}
func (m *recordingMetrics) Gauge(name string, v float64, _ map[string]string) {
	m.gauges[name] = v
}

func TestRunOnce_RecoversEachListedRefundAndKeepsGoingPastFailures(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	lister := &fakeLister{ids: []uuid.UUID{a, b, c}, pending: 2}
	recoverer := &fakeRecoverer{fail: map[uuid.UUID]bool{b: true}}
	metrics := &recordingMetrics{counts: map[string]int{}, gauges: map[string]float64{}}

	job := New(lister, recoverer, noopLogger{}, metrics, Config{MinAge: 10 * time.Minute, BatchSize: 50})
	if err := job.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	if len(recoverer.calls) != 3 {
		t.Fatalf("one failing refund must not stop the rest, got %d calls", len(recoverer.calls))
	}
	if lister.minAge != 10*time.Minute || lister.limit != 50 {
		t.Fatalf("the configured age and batch must reach the listing, got %s %d", lister.minAge, lister.limit)
	}
	if metrics.counts[ports.MetricRefundRecoveryFailed] != 1 {
		t.Fatalf("expected one recovery failure counted, got %v", metrics.counts)
	}
	if metrics.gauges[ports.MetricRefundReviewPending] != 2 {
		t.Fatalf("expected the review gauge to report 2, got %v", metrics.gauges)
	}
}

func TestNew_DefaultsKeepRecoveryAwayFromLiveCalls(t *testing.T) {
	lister := &fakeLister{}
	job := New(lister, &fakeRecoverer{}, noopLogger{}, &recordingMetrics{counts: map[string]int{}, gauges: map[string]float64{}}, Config{})
	_ = job.RunOnce(context.Background())
	if lister.minAge != 5*time.Minute || lister.limit != 100 {
		t.Fatalf("unset config must fall back to 5m and 100, got %s %d", lister.minAge, lister.limit)
	}
}
