package payment

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/app/authz"
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/domain/routing"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type fakeRepo struct {
	inserted  []*transaction.Transaction
	store     map[uuid.UUID]*transaction.Transaction
	updates   int
	snapshots []transaction.Transaction
	reviewed  map[uuid.UUID]string
	failOn    bool
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
	r.snapshots = append(r.snapshots, *t)
	t.Version++
	if r.store == nil {
		r.store = map[uuid.UUID]*transaction.Transaction{}
	}
	r.store[t.ID] = t
	return nil
}

func (r *fakeRepo) MarkReviewRequired(ctx context.Context, id uuid.UUID, reason string) (bool, error) {
	if r.reviewed == nil {
		r.reviewed = map[uuid.UUID]string{}
	}
	if _, done := r.reviewed[id]; done {
		return false, nil
	}
	r.reviewed[id] = reason
	return true, nil
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

	initiated    []ports.GatewayPaymentRequest
	statusChecks int
	ctxErrs      []error
	hadDeadline  []bool
	deadlines    []time.Time
	onInitiate   func()

	notIdempotent bool
}

func (a *fakeAdapter) InitiatePayment(ctx context.Context, req ports.GatewayPaymentRequest) (*ports.GatewayPaymentResponse, error) {
	a.initiated = append(a.initiated, req)
	a.ctxErrs = append(a.ctxErrs, ctx.Err())
	deadline, ok := ctx.Deadline()
	a.hadDeadline = append(a.hadDeadline, ok)
	a.deadlines = append(a.deadlines, deadline)
	if a.onInitiate != nil {
		a.onInitiate()
	}
	return a.resp, a.err
}
func (a *fakeAdapter) CheckStatus(ctx context.Context, req ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	a.statusChecks++
	return a.resp, a.err
}
func (a *fakeAdapter) Refund(ctx context.Context, req ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	return nil, nil
}
func (a *fakeAdapter) Cancel(ctx context.Context, req ports.GatewayCancelRequest) (*ports.GatewayCancelResponse, error) {
	return nil, nil
}
func (a *fakeAdapter) Capabilities() ports.GatewayCapabilities {
	return ports.GatewayCapabilities{IdempotencyCapable: !a.notIdempotent}
}

type countingMetrics struct {
	noopMetrics
	counts map[string]int
	tags   map[string]map[string]string
}

func newCountingMetrics() *countingMetrics {
	return &countingMetrics{counts: map[string]int{}, tags: map[string]map[string]string{}}
}

func (m *countingMetrics) Increment(name string, tags map[string]string) {
	m.counts[name]++
	m.tags[name] = tags
}

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
	inputs   []RouteInput
}

func (r *fakeRouter) Route(ctx context.Context, in RouteInput) (*routing.Decision, error) {
	r.inputs = append(r.inputs, in)
	return r.decision, r.err
}

type fakeConfig struct {
	timeout time.Duration
	err     error
}

func (c *fakeConfig) GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error) {
	return c.timeout, c.err
}

type fakeTransactor struct {
	committed bool
	calls     int
	failAt    int
}

func (t *fakeTransactor) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	t.calls++
	if t.failAt != 0 && t.calls == t.failAt {
		return errors.New("transaction failed")
	}
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
		Scope:         authz.AllMerchants(),
		MerchantID:    uuid.New(),
		Amount:        150000,
		Currency:      "INR",
		PaymentMethod: transaction.PaymentMethodCard,
		CustomerID:    uuid.New(),
		CustomerEmail: "buyer@example.com",
		Description:   "order #42",
	}
}

type trackingTx struct{ open bool }

func (t *trackingTx) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	t.open = true
	defer func() { t.open = false }()
	return fn(ctx)
}

type outsideTxRouter struct {
	tx       *trackingTx
	calls    int
	insideTx bool
}

func (r *outsideTxRouter) Route(context.Context, RouteInput) (*routing.Decision, error) {
	r.calls++
	r.insideTx = r.insideTx || r.tx.open
	return &routing.Decision{SelectedGateway: "razorpay"}, nil
}

type outsideTxConfig struct {
	tx       *trackingTx
	calls    int
	insideTx bool
}

func (c *outsideTxConfig) GetProcessingTimeout(context.Context, string, string) (time.Duration, error) {
	c.calls++
	c.insideTx = c.insideTx || c.tx.open
	return 30 * time.Second, nil
}

type reservingStore struct{}

func (reservingStore) Reserve(context.Context, string, string) (bool, error) { return true, nil }
func (reservingStore) Lookup(context.Context, string) (bool, string, string, []byte, error) {
	return false, "", "", nil, nil
}
func (reservingStore) Complete(context.Context, string, []byte) error { return nil }

func TestCreatePayment_RoutingAndConfigReadsRunOutsideTheReservation(t *testing.T) {
	tx := &trackingTx{}
	router := &outsideTxRouter{tx: tx}
	config := &outsideTxConfig{tx: tx}
	svc := NewService(&fakeRepo{}, &fakeOutbox{}, router, config, tx, &fakeLease{acquired: true}, &fakeRegistry{}, noopLogger{}, noopMetrics{})
	svc.SetIdempotency(idempotency.NewGuard(reservingStore{}, tx))

	in := validInput()
	in.IdempotencyKey = "k1"
	if _, err := svc.CreatePayment(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if router.calls == 0 || config.calls == 0 {
		t.Fatalf("expected routing and config reads, got %d and %d", router.calls, config.calls)
	}
	if router.insideTx || config.insideTx {
		t.Fatal("routing and config reads take their own pool connections; inside the reservation transaction, concurrent creates each hold one connection while waiting for a second, and the pool deadlocks")
	}
}

func TestCreatePayment_RoutingTierAndDomesticComeFromTheServer(t *testing.T) {
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "stripe"}}
	svc := newTestService(&fakeRepo{}, &fakeOutbox{}, router, &fakeConfig{timeout: 30 * time.Second}, &fakeTransactor{})
	in := validInput()
	in.Currency = "USD"
	svc.SetMerchantTiers(StaticMerchantTiers{in.MerchantID.String(): "enterprise"})

	if _, err := svc.CreatePayment(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	got := router.inputs[0]
	if got.MerchantTier != "enterprise" {
		t.Errorf("the tier must be the merchant's configured tier, got %q", got.MerchantTier)
	}
	if got.IsDomestic {
		t.Error("a USD payment is not domestic; the flag must be derived from the currency, not the client")
	}
}

func TestCreatePayment_UnconfiguredMerchantGetsTheDefaultTier(t *testing.T) {
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "stripe"}}
	svc := newTestService(&fakeRepo{}, &fakeOutbox{}, router, &fakeConfig{timeout: 30 * time.Second}, &fakeTransactor{})
	svc.SetMerchantTiers(StaticMerchantTiers{})

	if _, err := svc.CreatePayment(context.Background(), validInput()); err != nil {
		t.Fatal(err)
	}
	if router.inputs[0].MerchantTier != "" || !router.inputs[0].IsDomestic {
		t.Fatalf("an unlisted merchant routes with the default weights, and INR is domestic; got %+v", router.inputs[0])
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
