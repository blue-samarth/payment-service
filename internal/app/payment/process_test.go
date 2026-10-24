package payment

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	domaingateway "samarth/payment-service/internal/domain/gateway"
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

func TestProcessPayment_TerminalEventCarriesPostTransitionVersion(t *testing.T) {
	txn := pendingTxn() // version 1
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
	if len(outbox.events) != 1 {
		t.Fatalf("expected one terminal event, got %d", len(outbox.events))
	}

	var p paymentTerminalPayload
	if err := json.Unmarshal(outbox.events[0].Payload, &p); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	// The event must carry the version AFTER the terminal transition committed
	// (PENDING->PROCESSING->SUCCEEDED bumps 1->2->3). Built before the bump it
	// would read the stale PROCESSING version and consumers couldn't order it.
	if p.AggregateVersion != got.Version {
		t.Errorf("payload aggregate_version = %d, want post-transition version %d", p.AggregateVersion, got.Version)
	}
	if p.AggregateVersion <= 1 {
		t.Errorf("aggregate_version %d should reflect the terminal transition, not creation", p.AggregateVersion)
	}
	// The envelope carries it too, so it lands in the outbox column and (opt-in)
	// the SNS attribute — not just the JSON body.
	if outbox.events[0].AggregateVersion != got.Version {
		t.Errorf("envelope AggregateVersion = %d, want %d", outbox.events[0].AggregateVersion, got.Version)
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
	svc.SetMerchantTiers(StaticMerchantTiers{txn.MerchantID.String(): "enterprise"})

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
	if len(router.inputs) != 1 || router.inputs[0].MerchantTier != "enterprise" {
		t.Errorf("the fallback must route with the merchant's tier too, got %+v", router.inputs)
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
	if got.Status != transaction.StatusProcessing {
		t.Errorf("a 5xx may have charged the customer, so it must stay PROCESSING for the reaper rather than end FAILED, got %s", got.Status)
	}
}

func TestRecoverExpiredLease_StatusCheck5xxStaysProcessing(t *testing.T) {
	txn := processingExpiredTxn()
	repo := seedRepo(txn)
	svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "api_error"}}}, &fakeOutbox{})

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Fatalf("a 5xx on a status check says nothing about the payment; it used to finalize FAILED, got %s", got.Status)
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

func TestProcessPayment_RecordsGatewayBeforeCallingIt(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}
	svc := processService(repo, &fakeLease{acquired: true}, &fakeRegistry{adapter: adapter})

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	first := repo.snapshots[0]
	if first.Status != transaction.StatusProcessing || first.ActualGateway != "stripe" {
		t.Fatalf("the PROCESSING write must name the gateway about to be called, got status=%s gateway=%q", first.Status, first.ActualGateway)
	}
}

func TestProcessPayment_FallbackRecordsNextGatewayBeforeCallingIt(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	first := &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategorySoftDecline, Code: "soft_decline"}}
	var updatesBeforeSecondCall int
	second := &fakeAdapter{
		resp:       &ports.GatewayPaymentResponse{GatewayReferenceID: "pay_fb", Status: ports.GatewayPaymentStatusSucceeded},
		onInitiate: func() { updatesBeforeSecondCall = repo.updates },
	}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": first, "razorpay": second}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}
	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetMaxGatewayAttempts(2)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if updatesBeforeSecondCall != 2 || repo.snapshots[1].ActualGateway != "razorpay" {
		t.Fatalf("the switch to razorpay must be persisted before razorpay is called, updates=%d snapshots=%+v", updatesBeforeSecondCall, repo.snapshots)
	}
}

func TestProcessPayment_CustomerActionIsSavedAndGetsItsOwnWindow(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	action := &transaction.NextAction{Type: transaction.NextActionStripePaymentElement, Params: map[string]string{"client_secret": "pi_1_secret"}}
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusPending, NextAction: action}}
	svc := processService(repo, &fakeLease{acquired: true}, &fakeRegistry{adapter: adapter})
	svc.SetCustomerActionWindow(30 * time.Minute)

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusProcessing || !got.AwaitingCustomer() || got.NextAction.Params["client_secret"] != "pi_1_secret" {
		t.Fatalf("the payment must wait on the customer with the action saved, got %s %+v", got.Status, got.NextAction)
	}
	if got.ProcessingTimeout == nil || *got.ProcessingTimeout != 30*time.Minute {
		t.Fatalf("the reaper must leave it alone for the customer window, got %v", got.ProcessingTimeout)
	}
	if got.GatewayReferenceID != "pi_1" {
		t.Fatalf("the reference must be saved for status checks and webhooks, got %q", got.GatewayReferenceID)
	}
}

func TestProcessPayment_WindowNeverShortensTheLease(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusPending, NextAction: &transaction.NextAction{Type: "x"}}}
	svc := processService(repo, &fakeLease{acquired: true}, &fakeRegistry{adapter: adapter})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProcessingTimeout == nil || *got.ProcessingTimeout <= 0 {
		t.Fatalf("with no window configured the original lease must stand, got %v", got.ProcessingTimeout)
	}
}

func awaitingExpiredTxn() *transaction.Transaction {
	t := processingExpiredTxn()
	t.NextAction = &transaction.NextAction{Type: transaction.NextActionRazorpayCheckout, Params: map[string]string{"order_id": "order_1"}}
	return t
}

func TestRecoverExpiredLease_AbandonedCustomerActionFails(t *testing.T) {
	txn := awaitingExpiredTxn()
	repo := seedRepo(txn)
	outbox := &fakeOutbox{}
	svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_stuck", Status: ports.GatewayPaymentStatusPending}}}, outbox)

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusFailed || got.FailureReason == nil || got.FailureReason.Category != "customer_action_expired" {
		t.Fatalf("a customer who never paid within the window ends the payment, instead of a status check every sweep forever; got %s %+v", got.Status, got.FailureReason)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypePaymentFailed {
		t.Fatalf("expected PAYMENT_FAILED, got %+v", outbox.events)
	}
}

func TestRecoverExpiredLease_CustomerActionInProgressKeepsWaiting(t *testing.T) {
	for name, tc := range map[string]struct {
		status ports.GatewayPaymentStatus
		want   transaction.Status
	}{
		"bank still processing": {ports.GatewayPaymentStatusProcessing, transaction.StatusProcessing},
		"paid in the meantime":  {ports.GatewayPaymentStatusSucceeded, transaction.StatusSucceeded},
	} {
		txn := awaitingExpiredTxn()
		repo := seedRepo(txn)
		svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_stuck", Status: tc.status}}}, &fakeOutbox{})
		got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.Status != tc.want {
			t.Errorf("%s: got %s, want %s", name, got.Status, tc.want)
		}
	}
}

func TestRecoverExpiredLease_PendingWithoutACustomerActionIsNotAbandoned(t *testing.T) {
	txn := processingExpiredTxn()
	repo := seedRepo(txn)
	svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_stuck", Status: ports.GatewayPaymentStatusPending}}}, &fakeOutbox{})
	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Fatalf("only a payment that handed the customer an action can be abandoned, got %s", got.Status)
	}
}

type fakeGate struct {
	refuse      map[string]string
	probe       map[string]bool
	err         error
	ttls        []time.Duration
	noDeadlines int
	released    []string
}

func (g *fakeGate) AdmitCall(ctx context.Context, gatewayID string, ttl time.Duration) (domaingateway.Admission, error) {
	g.ttls = append(g.ttls, ttl)
	if _, ok := ctx.Deadline(); !ok {
		g.noDeadlines++
	}
	if g.err != nil {
		return domaingateway.Admission{}, g.err
	}
	if reason, ok := g.refuse[gatewayID]; ok {
		return domaingateway.Admission{Refusal: reason}, nil
	}
	return domaingateway.Admission{Admitted: true, HoldsProbe: g.probe[gatewayID]}, nil
}

func (g *fakeGate) ReleaseProbe(_ context.Context, gatewayID string) error {
	g.released = append(g.released, gatewayID)
	return nil
}

func TestProcessPayment_RefusedGatewayIsReroutedBeforeAnyCall(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	first := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}
	second := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pay_2", Status: ports.GatewayPaymentStatusSucceeded}}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": first, "razorpay": second}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}
	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	gate := &fakeGate{refuse: map[string]string{"stripe": domaingateway.RefusalOpen}}
	svc.SetCallGate(gate)
	svc.SetGatewayHTTPTimeout(30 * time.Second)

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.initiated) != 0 {
		t.Fatal("a gateway the breaker refused must not be called")
	}
	if got.Status != transaction.StatusSucceeded || got.ActualGateway != "razorpay" {
		t.Fatalf("a refusal is not an attempt, so even with one attempt the payment must move to razorpay, got %s on %q", got.Status, got.ActualGateway)
	}
	for _, ttl := range gate.ttls {
		if ttl < 30*time.Second {
			t.Fatalf("the probe TTL must cover a whole gateway call, got %s", ttl)
		}
	}
}

func TestProcessPayment_SoftDeclineThenRefusalStaysOnTheDecliningGateway(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	first := &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategorySoftDecline, Code: "soft_decline"}}
	second := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pay_2", Status: ports.GatewayPaymentStatusSucceeded}}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": first, "razorpay": second}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}
	metrics := newCountingMetrics()
	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, metrics)
	svc.SetMaxGatewayAttempts(2)
	svc.SetCallGate(&fakeGate{refuse: map[string]string{"razorpay": domaingateway.RefusalOpen}})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.initiated) != 0 {
		t.Fatal("the refused gateway must not be called")
	}
	if got.Status != transaction.StatusFailed || got.ActualGateway != "stripe" || got.GatewayID != "stripe" {
		t.Fatalf("stripe's soft decline must be recorded against stripe, not the refused razorpay; got %s on gateway=%q actual=%q", got.Status, got.GatewayID, got.ActualGateway)
	}
	for _, snap := range repo.snapshots {
		if snap.ActualGateway == "razorpay" {
			t.Fatal("a refused gateway must never be persisted as the payment's gateway")
		}
	}
	if metrics.counts[ports.MetricGatewayFallbackTriggered] != 0 {
		t.Error("no fallback happened, so none may be counted")
	}
}

func TestProcessPayment_RefusedWithNoAlternativeFailsWithoutACharge(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	only := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}
	router := &fakeRouter{err: routing.ErrNoCandidate{}}
	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, &fakeRegistry{adapter: only}, noopLogger{}, noopMetrics{})
	gate := &fakeGate{refuse: map[string]string{"stripe": domaingateway.RefusalProbeInFlight}}
	svc.SetCallGate(gate)

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(only.initiated) != 0 {
		t.Fatal("no gateway may be called")
	}
	if got.Status != transaction.StatusFailed || got.FailureReason == nil || got.FailureReason.Category != "gateway_unavailable" {
		t.Fatalf("with nothing to call, the payment fails as unavailable (nothing was charged), got %s %+v", got.Status, got.FailureReason)
	}
	if got.FailureReason.Code != domaingateway.RefusalProbeInFlight {
		t.Fatalf("the failure must carry the real refusal reason, got %q", got.FailureReason.Code)
	}
	if gate.noDeadlines != 0 {
		t.Fatal("the gate must be called with a short deadline so a dead Redis cannot stall a payment")
	}
}

func TestProcessPayment_ProbeReleasedWhenTheSwitchCannotBePersisted(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	first := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}
	second := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pay_2", Status: ports.GatewayPaymentStatusSucceeded}}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": first, "razorpay": second}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}
	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{failAt: 2}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	gate := &fakeGate{
		refuse: map[string]string{"stripe": domaingateway.RefusalOpen},
		probe:  map[string]bool{"razorpay": true},
	}
	svc.SetCallGate(gate)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if len(first.initiated)+len(second.initiated) != 0 {
		t.Fatal("no gateway may be called when the switch is not recorded")
	}
	if len(gate.released) != 1 || gate.released[0] != "razorpay" {
		t.Fatalf("a probe taken for a call that never happens must be released, not left to its TTL; released %v", gate.released)
	}
}

func TestProcessPayment_ProbeNotReleasedWhenNoneWasTaken(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": &fakeAdapter{}, "razorpay": &fakeAdapter{}}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}
	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{failAt: 2}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	gate := &fakeGate{refuse: map[string]string{"stripe": domaingateway.RefusalOpen}}
	svc.SetCallGate(gate)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if len(gate.released) != 0 {
		t.Fatalf("razorpay was CLOSED, so its probe belongs to nobody here and must not be deleted; released %v", gate.released)
	}
}

func TestProcessPayment_GateErrorAdmitsTheCallAndIsCounted(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}
	metrics := newCountingMetrics()
	svc := NewService(repo, &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, &fakeRegistry{adapter: adapter}, noopLogger{}, metrics)
	svc.SetCallGate(&fakeGate{err: errors.New("redis down")})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusSucceeded || len(adapter.initiated) != 1 {
		t.Fatalf("a probe-store error must not block payments, got %s with %d calls", got.Status, len(adapter.initiated))
	}
	if metrics.counts[ports.MetricGatewayProbeGateError] != 1 {
		t.Fatalf("the fail-open must be visible, got %v", metrics.counts)
	}
}

func TestProcessPayment_FallbackNotCalledWhenSwitchCannotBePersisted(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	first := &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategorySoftDecline, Code: "soft_decline"}}
	second := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pay_fb", Status: ports.GatewayPaymentStatusSucceeded}}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": first, "razorpay": second}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}
	metrics := newCountingMetrics()
	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{failAt: 2}, &fakeLease{acquired: true}, reg, noopLogger{}, metrics)
	svc.SetMaxGatewayAttempts(2)

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.counts[ports.MetricGatewayFallbackTriggered] != 0 {
		t.Error("a fallback that never happened must not be counted")
	}
	if len(second.initiated) != 0 {
		t.Fatal("a gateway whose selection is not recorded must never be charged: recovery would query the wrong gateway")
	}
	if got.ActualGateway != "stripe" || got.Status != transaction.StatusFailed {
		t.Errorf("expected the soft decline on stripe to stand, got gateway=%q status=%s", got.ActualGateway, got.Status)
	}
}

func TestProcessPayment_CallerCancellationDoesNotAbortGatewayCall(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}
	svc := processService(repo, &fakeLease{acquired: true}, &fakeRegistry{adapter: adapter})

	got, err := svc.ProcessPayment(ctx, txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if adapter.ctxErrs[0] != nil {
		t.Fatalf("the gateway call must not inherit the request's cancellation, got %v", adapter.ctxErrs[0])
	}
	if got.Status != transaction.StatusSucceeded || got.GatewayReferenceID != "pi_1" {
		t.Errorf("a client that disconnects mid-charge must not prevent the result being recorded, got %s %q", got.Status, got.GatewayReferenceID)
	}
}

func TestRecoverExpiredLease_NoReferenceReplaysTheOriginalRequest(t *testing.T) {
	txn := processingExpiredTxn()
	txn.GatewayReferenceID = ""
	recent := time.Now().UTC().Add(-time.Hour)
	txn.ProcessingStartedAt = &recent
	repo := seedRepo(txn)
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_replayed", Status: ports.GatewayPaymentStatusSucceeded}}
	svc := recoverService(repo, &fakeRegistry{adapter: adapter}, &fakeOutbox{})

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if adapter.statusChecks != 0 {
		t.Fatal("a status check by an empty reference returns an error that would finalize a possibly-charged payment as FAILED")
	}
	if len(adapter.initiated) != 1 {
		t.Fatalf("expected one idempotent replay, got %d", len(adapter.initiated))
	}
	req := adapter.initiated[0]
	if req.TransactionID != txn.ID || req.AttemptNumber != 1 || req.Amount != txn.Amount || req.IdempotencyKey != txn.GatewayIdempotencyKey {
		t.Errorf("the replay must be the original request so the gateway derives the same idempotency key, got %+v", req)
	}
	if got.Status != transaction.StatusSucceeded || got.GatewayReferenceID != "pi_replayed" {
		t.Errorf("expected the replayed result recorded, got %s %q", got.Status, got.GatewayReferenceID)
	}
}

func TestRecoverExpiredLease_ReplayErrorsOtherThanDeclinesStayProcessing(t *testing.T) {
	cases := map[string]struct {
		err      *ports.GatewayError
		terminal bool
	}{
		"original still in flight": {&ports.GatewayError{Category: ports.ErrorCategoryIdempotencyConflict, Code: "idempotency_key_in_use"}, false},
		"gateway 5xx":              {&ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "api_error"}, false},
		"replayed hard decline":    {&ports.GatewayError{Category: ports.ErrorCategoryHardDecline, Code: "card_declined"}, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			txn := processingExpiredTxn()
			txn.GatewayReferenceID = ""
			recent := time.Now().UTC().Add(-time.Hour)
			txn.ProcessingStartedAt = &recent
			repo := seedRepo(txn)
			svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{err: tc.err}}, &fakeOutbox{})

			got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.terminal {
				if got.Status != transaction.StatusFailed {
					t.Fatalf("a decline replayed by the gateway is a real outcome, got %s", got.Status)
				}
				return
			}
			if got.Status != transaction.StatusProcessing || repo.updates != 0 {
				t.Fatalf("a replay error says nothing about whether the first call charged, so the txn must stay PROCESSING, got %s updates=%d", got.Status, repo.updates)
			}
		})
	}
}

func TestRecoverExpiredLease_NoReferenceOutsideReplayWindowIsLeftForReview(t *testing.T) {
	txn := processingExpiredTxn()
	txn.GatewayReferenceID = ""
	old := time.Now().UTC().Add(-48 * time.Hour)
	txn.ProcessingStartedAt = &old
	repo := seedRepo(txn)
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusSucceeded}}
	svc := recoverService(repo, &fakeRegistry{adapter: adapter}, &fakeOutbox{})

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(adapter.initiated) != 0 || adapter.statusChecks != 0 {
		t.Fatal("past the gateway's idempotency window a replay could charge again, so nothing may be sent")
	}
	if got.Status != transaction.StatusProcessing || repo.updates != 0 {
		t.Errorf("expected the transaction left PROCESSING for manual review, got %s updates=%d", got.Status, repo.updates)
	}
	if repo.reviewed[txn.ID] != reviewReasonReplayWindowPassed {
		t.Errorf("the transaction must be marked for review so the reaper stops re-listing it, got %q", repo.reviewed[txn.ID])
	}
}

func TestRecoverExpiredLease_NonIdempotentGatewayIsNeverReplayed(t *testing.T) {
	txn := processingExpiredTxn()
	txn.GatewayReferenceID = ""
	txn.ActualGateway = "razorpay"
	recent := time.Now().UTC().Add(-time.Hour)
	txn.ProcessingStartedAt = &recent
	repo := seedRepo(txn)
	razorpay := &fakeAdapter{notIdempotent: true, resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "order_2", Status: ports.GatewayPaymentStatusPending}}
	metrics := newCountingMetrics()
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"razorpay": razorpay}}
	svc := NewService(repo, &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, metrics)

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(razorpay.initiated) != 0 || razorpay.statusChecks != 0 {
		t.Fatal("a gateway that does not dedupe by key would create a second order (or charge) on replay")
	}
	if got.Status != transaction.StatusProcessing || repo.reviewed[txn.ID] != reviewReasonNotIdempotent {
		t.Fatalf("expected PROCESSING marked %q, got %s %q", reviewReasonNotIdempotent, got.Status, repo.reviewed[txn.ID])
	}
	if metrics.counts[ports.MetricTransactionReviewRequired] != 1 || metrics.tags[ports.MetricTransactionReviewRequired]["reason"] != reviewReasonNotIdempotent {
		t.Errorf("a review must be counted for alerting, got %v %v", metrics.counts, metrics.tags)
	}

	if _, err := svc.RecoverExpiredLease(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if metrics.counts[ports.MetricTransactionReviewRequired] != 1 {
		t.Errorf("an already-marked transaction must not be counted again, got %d", metrics.counts[ports.MetricTransactionReviewRequired])
	}
}

func TestProcessPayment_LeaseCoversTheGatewayBudget(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}
	svc := processService(repo, &fakeLease{acquired: true}, &fakeRegistry{adapter: adapter})
	svc.SetMaxGatewayAttempts(3)
	svc.SetGatewayHTTPTimeout(30 * time.Second)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	got := repo.snapshots[0].ProcessingTimeout
	if got == nil || *got < 90*time.Second+leaseMargin {
		t.Fatalf("a lease shorter than the gateway budget lets the reaper act while the handler is still calling the gateway, got %v", got)
	}
	if !adapter.hadDeadline[0] {
		t.Fatal("the detached gateway call needs its own deadline as a backstop")
	}
	leaseEnd := repo.snapshots[0].ProcessingStartedAt.Add(*got)
	if gap := leaseEnd.Sub(adapter.deadlines[0]); gap < leaseMargin-workMargin-time.Second {
		t.Errorf("the work deadline must fall well before the lease expires, gap=%s", gap)
	}
}

func TestProcessPayment_SavesTheReferenceAsSoonAsTheGatewayAnswers(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}
	svc := processService(repo, &fakeLease{acquired: true}, &fakeRegistry{adapter: adapter})

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if len(repo.snapshots) != 3 {
		t.Fatalf("expected begin, reference and finalize writes, got %d", len(repo.snapshots))
	}
	if s := repo.snapshots[1]; s.Status != transaction.StatusProcessing || s.GatewayReferenceID != "pi_1" {
		t.Errorf("the reference must be persisted before finalize, so an interruption leaves a status-checkable row, got %s %q", s.Status, s.GatewayReferenceID)
	}
}

func TestProcessPayment_ReferenceSaveFailureDoesNotBlockFinalize(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}
	svc := NewService(repo, &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{failAt: 2}, &fakeLease{acquired: true}, &fakeRegistry{adapter: adapter}, noopLogger{}, noopMetrics{})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusSucceeded || got.GatewayReferenceID != "pi_1" {
		t.Errorf("finalize still records the outcome and reference, got %s %q", got.Status, got.GatewayReferenceID)
	}
}

func TestProcessPayment_FallbackClearsThePreviousGatewaysReference(t *testing.T) {
	txn := pendingTxn()
	txn.GatewayReferenceID = "pi_from_stripe"
	repo := seedRepo(txn)
	first := &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategorySoftDecline, Code: "soft_decline"}}
	second := &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusPending}}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": first, "razorpay": second}}
	router := &fakeRouter{decision: &routing.Decision{SelectedGateway: "razorpay"}}
	svc := NewService(repo, &fakeOutbox{}, router, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetMaxGatewayAttempts(2)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if s := repo.snapshots[1]; s.ActualGateway != "razorpay" || s.GatewayReferenceID != "" {
		t.Errorf("a stripe reference must not be left on a row now pointing at razorpay, got gateway=%q ref=%q", s.ActualGateway, s.GatewayReferenceID)
	}
}

func reviewTxn() *transaction.Transaction {
	t := processingExpiredTxn()
	t.GatewayReferenceID = ""
	marked := time.Now().UTC().Add(-time.Minute)
	t.ReviewRequiredAt = &marked
	t.ReviewReason = reviewReasonNotIdempotent
	return t
}

func TestResolveReview_Succeeded(t *testing.T) {
	txn := reviewTxn()
	repo := seedRepo(txn)
	outbox := &fakeOutbox{}
	svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{}}, outbox)

	got, err := svc.ResolveReview(context.Background(), txn.ID, ResolveInput{Status: transaction.StatusSucceeded, GatewayReferenceID: "pay_123"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusSucceeded || got.GatewayReferenceID != "pay_123" {
		t.Errorf("got %s %q", got.Status, got.GatewayReferenceID)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypePaymentSucceeded {
		t.Errorf("a resolution must publish the terminal event like any settlement, got %+v", outbox.events)
	}
}

func TestResolveReview_Failed(t *testing.T) {
	txn := reviewTxn()
	outbox := &fakeOutbox{}
	svc := recoverService(seedRepo(txn), &fakeRegistry{adapter: &fakeAdapter{}}, outbox)

	got, err := svc.ResolveReview(context.Background(), txn.ID, ResolveInput{Status: transaction.StatusFailed, Note: "no charge in dashboard"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusFailed || got.FailureReason == nil || got.FailureReason.Category != "manual_review" || got.FailureReason.GatewayMessage != "no charge in dashboard" {
		t.Errorf("got %s %+v", got.Status, got.FailureReason)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypePaymentFailed {
		t.Errorf("expected PAYMENT_FAILED, got %+v", outbox.events)
	}
}

func TestResolveReview_Refusals(t *testing.T) {
	notMarked := processingExpiredTxn()
	settled := reviewTxn()
	settled.Status = transaction.StatusSucceeded
	cases := map[string]struct {
		txn  *transaction.Transaction
		in   ResolveInput
		want error
	}{
		"not marked for review": {notMarked, ResolveInput{Status: transaction.StatusFailed}, ErrNotUnderReview},
		"already settled":       {settled, ResolveInput{Status: transaction.StatusFailed}, ErrNotUnderReview},
		"succeeded without ref": {reviewTxn(), ResolveInput{Status: transaction.StatusSucceeded}, ErrInvalidResolution},
		"non-terminal status":   {reviewTxn(), ResolveInput{Status: transaction.StatusProcessing}, ErrInvalidResolution},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			repo := seedRepo(tc.txn)
			svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{}}, &fakeOutbox{})
			if _, err := svc.ResolveReview(context.Background(), tc.txn.ID, tc.in); !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
			if repo.updates != 0 {
				t.Errorf("a refused resolution must not write, updates=%d", repo.updates)
			}
		})
	}
}

func TestProcessPayment_FinalizeLostToRecoveryReturnsTheRecoveredState(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusSucceeded}}
	adapter.onInitiate = func() {
		recovered := *repo.store[txn.ID]
		recovered.Status = transaction.StatusSucceeded
		repo.store[txn.ID] = &recovered
	}
	svc := NewService(repo, &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{failAt: 2}, &fakeLease{acquired: true}, &fakeRegistry{adapter: adapter}, noopLogger{}, noopMetrics{})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("the payment was settled by recovery; the merchant must get that state, not an error: %v", err)
	}
	if got.Status != transaction.StatusSucceeded {
		t.Errorf("expected the recovered SUCCEEDED state, got %s", got.Status)
	}
}

func TestProcessPayment_FinalizeFailureStillProcessingIsAnError(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}
	svc := NewService(repo, &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{failAt: 3}, &fakeLease{acquired: true}, &fakeRegistry{adapter: adapter}, noopLogger{}, noopMetrics{})

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err == nil {
		t.Fatal("a finalize failure with nobody else settling the payment must surface as an error")
	}
}

func TestRecoverExpiredLease_UsesTheGatewayLastCalled(t *testing.T) {
	txn := processingExpiredTxn()
	txn.ActualGateway = "razorpay"
	stripe := &fakeAdapter{}
	razorpay := &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_stuck", Status: ports.GatewayPaymentStatusSucceeded}}
	reg := &multiRegistry{adapters: map[string]ports.GatewayAdapter{"stripe": stripe, "razorpay": razorpay}}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeRouter{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	if _, err := svc.RecoverExpiredLease(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if razorpay.statusChecks != 1 || stripe.statusChecks != 0 {
		t.Errorf("recovery must query the gateway recorded as last called, stripe=%d razorpay=%d", stripe.statusChecks, razorpay.statusChecks)
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
