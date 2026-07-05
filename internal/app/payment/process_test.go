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

func processingExpiredTxn() *transaction.Transaction {
	t, _ := transaction.New(uuid.New(), 150000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@example.com", "order", nil, 30)
	t.AttemptedGateway = "stripe"
	t.ActualGateway = "stripe"
	t.Status = transaction.StatusProcessing
	t.GatewayReferenceID = "pi_stuck"
	past := time.Now().UTC().Add(-time.Hour)
	d := time.Second
	t.ProcessingStartedAt = &past
	t.ProcessingTimeout = &d
	return t
}

type multiRegistry struct {
	adapters map[string]ports.GatewayAdapter
}

func (r *multiRegistry) Get(gatewayID string) (ports.GatewayAdapter, error) {
	a, ok := r.adapters[gatewayID]
	if !ok {
		return nil, errors.New("no adapter for " + gatewayID)
	}
	return a, nil
}

func pendingTxn() *transaction.Transaction {
	t, _ := transaction.New(uuid.New(), 150000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@example.com", "order", nil, 30)
	t.AttemptedGateway = "stripe"
	return t
}

func processService(repo *fakeRepo, lease *fakeLease, reg *fakeRegistry) *Service {
	return NewService(repo, &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, lease, reg, noopLogger{}, noopMetrics{})
}

func seedRepo(txn *transaction.Transaction) *fakeRepo {
	return &fakeRepo{store: map[uuid.UUID]*transaction.Transaction{txn.ID: txn}}
}

type fakeCancelResolver struct {
	called bool
	gotID  uuid.UUID
	gotAmt int64
}

func (f *fakeCancelResolver) ResolveCancelRefund(_ context.Context, id uuid.UUID, amount int64) error {
	f.called = true
	f.gotID = id
	f.gotAmt = amount
	return nil
}

func TestProcessPayment_CancelResolutionHookFiresOnSucceededWithIntent(t *testing.T) {
	txn := pendingTxn()
	txn.CancelIntent = true
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusSucceeded}}}
	resolver := &fakeCancelResolver{}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetCancelResolver(resolver)

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", got.Status)
	}
	if !resolver.called {
		t.Fatal("expected cancel-resolution hook to fire")
	}
	if resolver.gotID != txn.ID || resolver.gotAmt != txn.Amount {
		t.Errorf("hook called with wrong args: id=%s amt=%d", resolver.gotID, resolver.gotAmt)
	}
}

func TestProcessPayment_NoCancelResolutionWithoutIntent(t *testing.T) {
	txn := pendingTxn() // CancelIntent false
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusSucceeded}}}
	resolver := &fakeCancelResolver{}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetCancelResolver(resolver)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if resolver.called {
		t.Error("hook should not fire without cancel intent")
	}
}

type fakeBreaker struct {
	failures  int
	successes int
}

func (f *fakeBreaker) RecordFailure(context.Context, string) error { f.failures++; return nil }
func (f *fakeBreaker) RecordSuccess(context.Context, string) error { f.successes++; return nil }

func TestProcessPayment_RecordsBreakerFailureOnNetworkError(t *testing.T) {
	txn := pendingTxn()
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout}}}
	breaker := &fakeBreaker{}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetCircuitBreaker(breaker)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if breaker.failures != 1 || breaker.successes != 0 {
		t.Errorf("network error should record a breaker failure, got failures=%d successes=%d", breaker.failures, breaker.successes)
	}
}

func TestProcessPayment_RecordsBreakerSuccessOnDecline(t *testing.T) {
	txn := pendingTxn()
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryHardDecline}}}
	breaker := &fakeBreaker{}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetCircuitBreaker(breaker)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if breaker.successes != 1 || breaker.failures != 0 {
		t.Errorf("a decline means the gateway is healthy; expected a success record, got failures=%d successes=%d", breaker.failures, breaker.successes)
	}
}

func TestProcessPayment_RecordsBreakerSuccessOnSuccess(t *testing.T) {
	txn := pendingTxn()
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusSucceeded}}}
	breaker := &fakeBreaker{}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetCircuitBreaker(breaker)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if breaker.successes != 1 {
		t.Errorf("expected a breaker success on a successful payment, got %d", breaker.successes)
	}
}

func TestProcessPayment_GatewaySucceeded(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	lease := &fakeLease{acquired: true}
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{
		GatewayReferenceID: "pi_1",
		Status:             ports.GatewayPaymentStatusSucceeded,
	}}}
	outbox := &fakeOutbox{}
	svc := NewService(repo, outbox, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, lease, reg, noopLogger{}, noopMetrics{})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusSucceeded {
		t.Errorf("expected SUCCEEDED, got %s", got.Status)
	}
	if got.GatewayReferenceID != "pi_1" {
		t.Errorf("expected gateway reference recorded, got %q", got.GatewayReferenceID)
	}
	if got.ActualGateway != "stripe" {
		t.Errorf("expected actual gateway stripe, got %q", got.ActualGateway)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypePaymentSucceeded {
		t.Errorf("expected one PAYMENT_SUCCEEDED event, got %+v", outbox.events)
	}
	if lease.written == nil {
		t.Error("expected cached response written to lease")
	}
}

func TestProcessPayment_GatewayHardDecline(t *testing.T) {
	txn := pendingTxn()
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{
		Category:       ports.ErrorCategoryHardDecline,
		Code:           "hard_decline",
		GatewayMessage: "declined",
	}}}
	outbox := &fakeOutbox{}
	svc := NewService(seedRepo(txn), outbox, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusFailed {
		t.Errorf("expected FAILED, got %s", got.Status)
	}
	if got.FailureReason == nil || got.FailureReason.Category != string(ports.ErrorCategoryHardDecline) {
		t.Errorf("expected failure reason recorded, got %+v", got.FailureReason)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypePaymentFailed {
		t.Errorf("expected one PAYMENT_FAILED event, got %+v", outbox.events)
	}
}

func TestProcessPayment_NetworkTimeoutStaysProcessing(t *testing.T) {
	txn := pendingTxn()
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{
		Category:  ports.ErrorCategoryNetworkTimeout,
		Code:      "network_error",
		Retryable: true,
	}}}
	outbox := &fakeOutbox{}
	svc := NewService(seedRepo(txn), outbox, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Errorf("indeterminate outcome should remain PROCESSING, got %s", got.Status)
	}
	if len(outbox.events) != 0 {
		t.Errorf("no terminal event expected for indeterminate outcome, got %+v", outbox.events)
	}
}

func TestProcessPayment_LeaseNotAcquired(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusSucceeded}}}
	svc := processService(repo, &fakeLease{acquired: false}, reg)

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusPending {
		t.Errorf("without the lease the transaction should be returned untouched (PENDING), got %s", got.Status)
	}
	if repo.updates != 0 {
		t.Errorf("expected no status updates when lease not acquired, got %d", repo.updates)
	}
}

func TestProcessPayment_NonPendingIsNoop(t *testing.T) {
	txn := pendingTxn()
	_ = transaction.TransitionState(txn, transaction.StatusProcessing, transaction.ActorSystem)
	repo := seedRepo(txn)
	svc := processService(repo, &fakeLease{acquired: true}, &fakeRegistry{adapter: &fakeAdapter{}})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Errorf("expected unchanged PROCESSING, got %s", got.Status)
	}
	if repo.updates != 0 {
		t.Error("expected no work for a non-PENDING transaction")
	}
}

func TestProcessPayment_UnknownGateway(t *testing.T) {
	txn := pendingTxn()
	svc := processService(seedRepo(txn), &fakeLease{acquired: true}, &fakeRegistry{err: errors.New("no adapter")})

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err == nil {
		t.Error("expected error when gateway adapter is not registered")
	}
}

func TestProcessPayment_FallsBackToSecondGatewayOnSoftDecline(t *testing.T) {
	txn := pendingTxn() // GatewayID "stripe"
	repo := seedRepo(txn)
	first := &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategorySoftDecline, Code: "soft_decline"}}
	second := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_fb", Status: ports.GatewayPaymentStatusSucceeded}}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": first, "razorpay": second}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}

	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetMaxGatewayAttempts(2)

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusSucceeded {
		t.Fatalf("expected fallback to succeed, got %s", got.Status)
	}
	if got.ActualGateway != "razorpay" || got.GatewayReferenceID != "pi_fb" {
		t.Errorf("expected the second gateway to settle the payment, got gateway=%q ref=%q", got.ActualGateway, got.GatewayReferenceID)
	}
}

func TestProcessPayment_NoFallbackWhenSingleAttempt(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	first := &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategorySoftDecline, Code: "soft_decline"}}
	second := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_fb", Status: ports.GatewayPaymentStatusSucceeded}}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": first, "razorpay": second}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}

	// Default attempt budget is 1: a soft decline must finalize as FAILED, never re-route.
	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusFailed {
		t.Errorf("expected FAILED with no fallback budget, got %s", got.Status)
	}
	if got.ActualGateway != "stripe" {
		t.Errorf("expected to stay on stripe, got %q", got.ActualGateway)
	}
}

func TestProcessPayment_FallbackExhaustedStaysFailed(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	first := &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategorySoftDecline, Code: "soft_decline"}}
	second := &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryHardDecline, Code: "hard_decline"}}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": first, "razorpay": second}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}

	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetMaxGatewayAttempts(2)

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusFailed {
		t.Errorf("expected FAILED after the fallback also declines, got %s", got.Status)
	}
	if got.ActualGateway != "razorpay" {
		t.Errorf("expected the failure attributed to the last gateway tried, got %q", got.ActualGateway)
	}
}

func TestProcessPayment_NoFallbackOnGatewayError(t *testing.T) {
	txn := pendingTxn() // GatewayID "stripe"
	repo := seedRepo(txn)
	// A GatewayError is a "maybe charged" outcome (5xx / post-send failure), so
	// re-attempting on another gateway risks a double charge.
	first := &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "gateway_error"}}
	second := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_fb", Status: ports.GatewayPaymentStatusSucceeded}}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": first, "razorpay": second}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}

	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetMaxGatewayAttempts(2)

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ActualGateway != "stripe" {
		t.Errorf("a GatewayError must not re-attempt on another gateway, got %q", got.ActualGateway)
	}
	if got.GatewayReferenceID == "pi_fb" {
		t.Error("the second gateway must not have been called after a GatewayError")
	}
}

func recoverService(repo *fakeRepo, reg *fakeRegistry, outbox *fakeOutbox) *Service {
	return NewService(repo, outbox, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
}

func TestRecoverExpiredLease_FinalizesSucceeded(t *testing.T) {
	txn := processingExpiredTxn()
	repo := seedRepo(txn)
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{
		GatewayReferenceID: "pi_stuck", Status: ports.GatewayPaymentStatusSucceeded,
	}}}
	svc := recoverService(repo, reg, outbox)

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusSucceeded {
		t.Errorf("status check found the payment SUCCEEDED; expected the stuck txn finalized, got %s", got.Status)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypePaymentSucceeded {
		t.Errorf("expected one PAYMENT_SUCCEEDED event on recovery, got %+v", outbox.events)
	}
}

func TestRecoverExpiredLease_FinalizesFailed(t *testing.T) {
	txn := processingExpiredTxn()
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{
		GatewayReferenceID: "pi_stuck", Status: ports.GatewayPaymentStatusFailed, ErrorCode: "declined",
	}}}
	svc := recoverService(seedRepo(txn), reg, outbox)

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusFailed {
		t.Errorf("expected FAILED, got %s", got.Status)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypePaymentFailed {
		t.Errorf("expected one PAYMENT_FAILED event, got %+v", outbox.events)
	}
}

func TestRecoverExpiredLease_StillPendingAtGatewayStaysProcessing(t *testing.T) {
	txn := processingExpiredTxn()
	repo := seedRepo(txn)
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{
		GatewayReferenceID: "pi_stuck", Status: ports.GatewayPaymentStatusProcessing,
	}}}
	svc := recoverService(repo, reg, outbox)

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Errorf("an indeterminate gateway status must leave the txn PROCESSING for the next sweep, got %s", got.Status)
	}
	if repo.updates != 0 || len(outbox.events) != 0 {
		t.Errorf("no write should happen while the outcome is unknown, updates=%d events=%d", repo.updates, len(outbox.events))
	}
}

func TestRecoverExpiredLease_NetworkErrorStaysProcessing(t *testing.T) {
	txn := processingExpiredTxn()
	repo := seedRepo(txn)
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout}}}
	svc := recoverService(repo, reg, &fakeOutbox{})

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusProcessing || repo.updates != 0 {
		t.Errorf("a status-check network error must not finalize, got status=%s updates=%d", got.Status, repo.updates)
	}
}

func TestRecoverExpiredLease_LeaseNotExpiredIsNoop(t *testing.T) {
	txn := processingExpiredTxn()
	future := time.Now().UTC().Add(time.Hour)
	txn.ProcessingStartedAt = &future // lease still valid
	repo := seedRepo(txn)
	svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{}}, &fakeOutbox{})

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusProcessing || repo.updates != 0 {
		t.Errorf("a still-valid lease must be left alone, got status=%s updates=%d", got.Status, repo.updates)
	}
}

func TestRecoverExpiredLease_NonProcessingIsNoop(t *testing.T) {
	txn := pendingTxn() // PENDING, not stuck
	repo := seedRepo(txn)
	svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{}}, &fakeOutbox{})

	if _, err := svc.RecoverExpiredLease(context.Background(), txn.ID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.updates != 0 {
		t.Errorf("a non-PROCESSING transaction needs no recovery, updates=%d", repo.updates)
	}
}
