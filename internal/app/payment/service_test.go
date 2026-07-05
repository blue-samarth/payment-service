package payment

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/routing"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type fakeRepo struct {
	inserted []*transaction.Transaction
	store    map[uuid.UUID]*transaction.Transaction
	updates  int
	failOn   bool
}

func (r *fakeRepo) Insert(ctx context.Context, t *transaction.Transaction) error {
	if r.failOn {
		return errors.New("insert failed")
	}
	r.inserted = append(r.inserted, t)
	if r.store == nil {
		r.store = map[uuid.UUID]*transaction.Transaction{}
	}
	r.store[t.ID] = t
	return nil
}

func (r *fakeRepo) GetByID(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error) {
	t, ok := r.store[id]
	if !ok {
		return nil, errors.New("not found")
	}
	clone := *t
	return &clone, nil
}

func (r *fakeRepo) UpdateStatus(ctx context.Context, t *transaction.Transaction) error {
	r.updates++
	t.Version++
	if r.store == nil {
		r.store = map[uuid.UUID]*transaction.Transaction{}
	}
	r.store[t.ID] = t
	return nil
}

type fakeLease struct {
	acquired bool
	cached   []byte
	written  []byte
}

func (l *fakeLease) Acquire(ctx context.Context, leaseKey, paymentIntentID uuid.UUID, ttlSec int) (bool, []byte, error) {
	return l.acquired, l.cached, nil
}

func (l *fakeLease) WriteCachedResponse(ctx context.Context, leaseKey uuid.UUID, response []byte) error {
	l.written = response
	return nil
}

type fakeAdapter struct {
	resp *ports.GatewayPaymentResponse
	err  error
}

func (a *fakeAdapter) InitiatePayment(ctx context.Context, req ports.GatewayPaymentRequest) (*ports.GatewayPaymentResponse, error) {
	return a.resp, a.err
}
func (a *fakeAdapter) CheckStatus(ctx context.Context, req ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	return a.resp, a.err
}
func (a *fakeAdapter) Refund(ctx context.Context, req ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	return nil, nil
}
func (a *fakeAdapter) Cancel(ctx context.Context, req ports.GatewayCancelRequest) (*ports.GatewayCancelResponse, error) {
	return nil, nil
}
func (a *fakeAdapter) Capabilities() ports.GatewayCapabilities { return ports.GatewayCapabilities{} }

type fakeRegistry struct {
	adapter ports.GatewayAdapter
	err     error
}

func (r *fakeRegistry) Get(gatewayID string) (ports.GatewayAdapter, error) {
	return r.adapter, r.err
}

type fakeOutbox struct {
	events []ports.OutboxEvent
	failOn bool
}

func (o *fakeOutbox) Write(ctx context.Context, event ports.OutboxEvent) error {
	if o.failOn {
		return errors.New("outbox write failed")
	}
	o.events = append(o.events, event)
	return nil
}

type fakeRouter struct {
	decision *routing.Decision
	err      error
}

func (r *fakeRouter) Route(ctx context.Context, in RouteInput) (*routing.Decision, error) {
	return r.decision, r.err
}

type fakeConfig struct {
	timeout time.Duration
	err     error
}

func (c *fakeConfig) GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error) {
	return c.timeout, c.err
}

type fakeTransactor struct{ committed bool }

func (t *fakeTransactor) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := fn(ctx); err != nil {
		return err
	}
	t.committed = true
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

func newTestService(repo *fakeRepo, outbox *fakeOutbox, router *fakeRouter, config *fakeConfig, tx *fakeTransactor) *Service {
	return NewService(repo, outbox, router, config, tx, &fakeLease{acquired: true}, &fakeRegistry{}, noopLogger{}, noopMetrics{})
}

func validInput() CreatePaymentInput {
	return CreatePaymentInput{
		MerchantID:    uuid.New(),
		Amount:        150000,
		Currency:      "INR",
		PaymentMethod: transaction.PaymentMethodCard,
		CustomerID:    uuid.New(),
		CustomerEmail: "buyer@example.com",
		Description:   "order #42",
		MerchantTier:  "standard",
		IsDomestic:    true,
	}
}

func TestCreatePayment_Success(t *testing.T) {
	repo := &fakeRepo{}
	outbox := &fakeOutbox{}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}
	config := &fakeConfig{timeout: 30 * time.Second}
	tx := &fakeTransactor{}
	svc := newTestService(repo, outbox, router, config, tx)

	res, err := svc.CreatePayment(context.Background(), validInput())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	txn := res.Transaction

	if txn.Status != transaction.StatusPending {
		t.Errorf("expected status PENDING, got %s", txn.Status)
	}
	if txn.GatewayID != "razorpay" || txn.AttemptedGateway != "razorpay" {
		t.Errorf("expected gateway razorpay, got gateway=%q attempted=%q", txn.GatewayID, txn.AttemptedGateway)
	}
	if txn.EstimatedTimeoutSeconds != 30 {
		t.Errorf("expected timeout 30s, got %d", txn.EstimatedTimeoutSeconds)
	}
	if !tx.committed {
		t.Error("expected transaction to be committed")
	}
	if len(repo.inserted) != 1 {
		t.Fatalf("expected 1 inserted transaction, got %d", len(repo.inserted))
	}
	if len(outbox.events) != 1 {
		t.Fatalf("expected 1 outbox event, got %d", len(outbox.events))
	}
	ev := outbox.events[0]
	if ev.EventType != ports.EventTypePaymentCreated {
		t.Errorf("expected event %s, got %s", ports.EventTypePaymentCreated, ev.EventType)
	}
	if ev.AggregateID != txn.ID {
		t.Errorf("expected event aggregate %s, got %s", txn.ID, ev.AggregateID)
	}
}

func TestCreatePayment_NoCandidate(t *testing.T) {
	repo := &fakeRepo{}
	outbox := &fakeOutbox{}
	router := &fakeRouter{err: routing.ErrNoCandidate{}}
	config := &fakeConfig{timeout: 30 * time.Second}
	tx := &fakeTransactor{}
	svc := newTestService(repo, outbox, router, config, tx)

	_, err := svc.CreatePayment(context.Background(), validInput())
	if !errors.Is(err, ErrNoGateway) {
		t.Fatalf("expected ErrNoGateway, got %v", err)
	}
	if len(repo.inserted) != 0 || len(outbox.events) != 0 {
		t.Error("expected no writes when routing yields no candidate")
	}
}

func TestCreatePayment_OutboxFailureRollsBack(t *testing.T) {
	repo := &fakeRepo{}
	outbox := &fakeOutbox{failOn: true}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "stripe"}}
	config := &fakeConfig{timeout: 30 * time.Second}
	tx := &fakeTransactor{}
	svc := newTestService(repo, outbox, router, config, tx)

	_, err := svc.CreatePayment(context.Background(), validInput())
	if err == nil {
		t.Fatal("expected error when outbox write fails")
	}
	if tx.committed {
		t.Error("expected transaction not to commit when outbox write fails")
	}
}

func TestCreatePayment_InvalidTimeout(t *testing.T) {
	repo := &fakeRepo{}
	outbox := &fakeOutbox{}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "payu"}}
	config := &fakeConfig{timeout: 0}
	tx := &fakeTransactor{}
	svc := newTestService(repo, outbox, router, config, tx)

	if _, err := svc.CreatePayment(context.Background(), validInput()); err == nil {
		t.Fatal("expected error for non-positive processing timeout")
	}
}
