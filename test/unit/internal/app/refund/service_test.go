package refund_test

import . "samarth/payment-service/internal/app/refund"

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/app/authz"
	domainrefund "samarth/payment-service/internal/domain/refund"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type fakeTxns struct {
	txn *transaction.Transaction
	err error
}

func (f *fakeTxns) GetByID(context.Context, uuid.UUID) (*transaction.Transaction, error) {
	return f.txn, f.err
}

type fakeRefunds struct {
	sum      int64
	inserted []*domainrefund.Refund
	byID     map[uuid.UUID]*domainrefund.Refund
	updates  int
	exists   bool
	reviews  map[uuid.UUID]string
}

func (f *fakeRefunds) LockParentTransaction(context.Context, uuid.UUID) error { return nil }
func (f *fakeRefunds) SumActiveRefunds(context.Context, uuid.UUID) (int64, error) {
	return f.sum, nil
}
func (f *fakeRefunds) Insert(_ context.Context, rf *domainrefund.Refund) error {
	f.inserted = append(f.inserted, rf)
	if f.byID == nil {
		f.byID = map[uuid.UUID]*domainrefund.Refund{}
	}
	f.byID[rf.ID] = rf
	return nil
}
func (f *fakeRefunds) GetByID(_ context.Context, id uuid.UUID) (*domainrefund.Refund, error) {
	if rf, ok := f.byID[id]; ok {
		return rf, nil
	}
	return nil, errors.New("not found")
}
func (f *fakeRefunds) UpdateStatus(_ context.Context, rf *domainrefund.Refund) error {
	f.updates++
	rf.Version++
	return nil
}
func (f *fakeRefunds) ExistsByReason(_ context.Context, _ uuid.UUID, _ string) (bool, error) {
	return f.exists, nil
}
func (f *fakeRefunds) MarkReviewRequired(_ context.Context, id uuid.UUID, reason string) (bool, error) {
	if f.reviews == nil {
		f.reviews = map[uuid.UUID]string{}
	}
	if _, done := f.reviews[id]; done {
		return false, nil
	}
	f.reviews[id] = reason
	return true, nil
}

type fakeRegistry struct {
	adapter ports.GatewayAdapter
	err     error
}

func (f *fakeRegistry) Get(string) (ports.GatewayAdapter, error) { return f.adapter, f.err }

type fakeOutbox struct{ events []ports.OutboxEvent }

func (f *fakeOutbox) Write(_ context.Context, e ports.OutboxEvent) error {
	f.events = append(f.events, e)
	return nil
}

type fakeTransactor struct{}

func (fakeTransactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
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

func succeededTxn(amount int64) *transaction.Transaction {
	t, _ := transaction.New(uuid.New(), amount, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@e.com", "o", nil, 30)
	t.Status = transaction.StatusSucceeded
	return t
}

func svc(txns *fakeTxns, refunds *fakeRefunds, outbox *fakeOutbox) *Service {
	return NewService(txns, refunds, outbox, fakeTransactor{}, &fakeRegistry{}, noopLogger{}, noopMetrics{})
}

type fakeAdapter struct {
	resp *ports.GatewayRefundResponse
	err  error
}

func (a *fakeAdapter) InitiatePayment(context.Context, ports.GatewayPaymentRequest) (*ports.GatewayPaymentResponse, error) {
	return nil, nil
}
func (a *fakeAdapter) CheckStatus(context.Context, ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	return nil, nil
}
func (a *fakeAdapter) Refund(context.Context, ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	return a.resp, a.err
}
func (a *fakeAdapter) Cancel(context.Context, ports.GatewayCancelRequest) (*ports.GatewayCancelResponse, error) {
	return nil, nil
}
func (a *fakeAdapter) Capabilities() ports.GatewayCapabilities { return ports.GatewayCapabilities{} }

type checkingAdapter struct {
	fakeAdapter
	status    *ports.GatewayRefundResponse
	statusErr error
	refunds   int
	asked     []ports.GatewayRefundStatusRequest
}

func (a *checkingAdapter) Refund(context.Context, ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	a.refunds++
	return a.resp, a.err
}

func (a *checkingAdapter) RefundStatus(_ context.Context, req ports.GatewayRefundStatusRequest) (*ports.GatewayRefundResponse, error) {
	a.asked = append(a.asked, req)
	return a.status, a.statusErr
}

func processingRefund(t *testing.T, adapter ports.GatewayAdapter) (*Service, *fakeRefunds, *fakeOutbox, *domainrefund.Refund) {
	t.Helper()
	parent := succeededTxn(100000)
	parent.GatewayReferenceID = "order_1"
	rf, _ := domainrefund.New(parent.ID, 40000, 100000, 0, "r", "merchant")
	rf.AttemptedGateway = "razorpay"
	rf.Status = domainrefund.StatusProcessing
	refunds := &fakeRefunds{byID: map[uuid.UUID]*domainrefund.Refund{rf.ID: rf}}
	outbox := &fakeOutbox{}
	s := NewService(&fakeTxns{txn: parent}, refunds, outbox, fakeTransactor{}, &fakeRegistry{adapter: adapter}, noopLogger{}, noopMetrics{})
	return s, refunds, outbox, rf
}

func TestRecoverRefund_CompletedAtTheGatewayIsFinalized(t *testing.T) {
	adapter := &checkingAdapter{status: &ports.GatewayRefundResponse{GatewayRefundID: "rfnd_9", Status: ports.GatewayRefundStatusCompleted}}
	s, _, outbox, rf := processingRefund(t, adapter)

	got, err := s.RecoverRefund(context.Background(), rf.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domainrefund.StatusRefunded || got.GatewayRefundID != "rfnd_9" {
		t.Fatalf("expected REFUNDED with the gateway refund id, got %s %q", got.Status, got.GatewayRefundID)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeRefundSucceeded {
		t.Fatalf("expected a REFUND_SUCCEEDED event, got %+v", outbox.events)
	}
	if adapter.refunds != 0 {
		t.Fatal("recovery must check the status, never re-post the refund")
	}
	if adapter.asked[0].RefundID != rf.ID || adapter.asked[0].GatewayReferenceID != "order_1" {
		t.Fatalf("the status check must carry our refund id and the payment reference, got %+v", adapter.asked[0])
	}
}

func TestRecoverRefund_FailedAtTheGatewayIsFinalized(t *testing.T) {
	adapter := &checkingAdapter{status: &ports.GatewayRefundResponse{Status: ports.GatewayRefundStatusFailed}}
	s, _, outbox, rf := processingRefund(t, adapter)

	got, err := s.RecoverRefund(context.Background(), rf.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domainrefund.StatusFailed || len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeRefundFailed {
		t.Fatalf("expected REFUND_FAILED with an event, got %s %+v", got.Status, outbox.events)
	}
}

func TestRecoverRefund_StillProcessingIsLeftAlone(t *testing.T) {
	adapter := &checkingAdapter{status: &ports.GatewayRefundResponse{GatewayRefundID: "rfnd_learned", Status: ports.GatewayRefundStatusProcessing}}
	s, refunds, outbox, rf := processingRefund(t, adapter)

	got, err := s.RecoverRefund(context.Background(), rf.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != domainrefund.StatusProcessing || len(outbox.events) != 0 || len(refunds.reviews) != 0 {
		t.Fatalf("a pending refund must stay as it is, got %s events=%d reviews=%v", got.Status, len(outbox.events), refunds.reviews)
	}
	if got.GatewayRefundID != "rfnd_learned" {
		t.Fatalf("a gateway refund id learned from the lookup must be kept for the next check, got %q", got.GatewayRefundID)
	}
}

func TestRecoverRefund_UndeterminableRefundsGoToReviewOnce(t *testing.T) {
	for name, adapter := range map[string]ports.GatewayAdapter{
		"not_found_at_gateway":     &checkingAdapter{status: &ports.GatewayRefundResponse{Status: ports.GatewayRefundStatusNotFound}},
		"status_unknown":           &checkingAdapter{status: &ports.GatewayRefundResponse{Status: ports.GatewayRefundStatusUnknown}},
		"status_check_unsupported": &fakeAdapter{},
	} {
		s, refunds, outbox, rf := processingRefund(t, adapter)
		got, err := s.RecoverRefund(context.Background(), rf.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != domainrefund.StatusProcessing || len(outbox.events) != 0 {
			t.Errorf("%s: must not be guessed at, got %s", name, got.Status)
		}
		if refunds.reviews[rf.ID] != name {
			t.Errorf("%s: expected a review mark with that reason, got %v", name, refunds.reviews)
		}
	}
}

func TestRecoverRefund_StatusCheckErrorChangesNothing(t *testing.T) {
	adapter := &checkingAdapter{statusErr: &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout, Code: "network_error"}}
	s, refunds, outbox, rf := processingRefund(t, adapter)

	if _, err := s.RecoverRefund(context.Background(), rf.ID); err == nil {
		t.Fatal("the caller must see the error so it is logged and retried next run")
	}
	if rf.Status != domainrefund.StatusProcessing || len(outbox.events) != 0 || len(refunds.reviews) != 0 || refunds.updates != 0 {
		t.Fatal("a failed status check must leave the refund untouched")
	}
}

func TestRecoverRefund_StaleInitiatedRefundIsSent(t *testing.T) {
	adapter := &checkingAdapter{fakeAdapter: fakeAdapter{resp: &ports.GatewayRefundResponse{GatewayRefundID: "rfnd_1", Status: ports.GatewayRefundStatusCompleted}}}
	s, _, _, rf := processingRefund(t, adapter)
	rf.Status = domainrefund.StatusInitiated

	got, err := s.RecoverRefund(context.Background(), rf.ID)
	if err != nil {
		t.Fatal(err)
	}
	if adapter.refunds != 1 || got.Status != domainrefund.StatusRefunded {
		t.Fatalf("an INITIATED refund was never sent, so recovery sends it once; got %d posts, %s", adapter.refunds, got.Status)
	}
}

func TestRecoverRefund_PastTheCutOffItGoesToReview(t *testing.T) {
	for name, tc := range map[string]struct {
		adapter   *checkingAdapter
		age       time.Duration
		reason    string
		wantErr   bool
		wantFinal domainrefund.Status
	}{
		"still pending after the cut-off": {
			&checkingAdapter{status: &ports.GatewayRefundResponse{Status: ports.GatewayRefundStatusProcessing}},
			8 * 24 * time.Hour, "pending_too_long", false, domainrefund.StatusProcessing,
		},
		"still failing after the cut-off": {
			&checkingAdapter{statusErr: &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "gateway_error"}},
			8 * 24 * time.Hour, "recovery_stalled", false, domainrefund.StatusProcessing,
		},
		"failing but young": {
			&checkingAdapter{statusErr: &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "gateway_error"}},
			time.Hour, "", true, domainrefund.StatusProcessing,
		},
		"old but completed": {
			&checkingAdapter{status: &ports.GatewayRefundResponse{Status: ports.GatewayRefundStatusCompleted}},
			30 * 24 * time.Hour, "", false, domainrefund.StatusRefunded,
		},
	} {
		s, refunds, _, rf := processingRefund(t, tc.adapter)
		s.SetRecoveryMaxAge(7 * 24 * time.Hour)
		rf.InitiatedAt = time.Now().UTC().Add(-tc.age)

		got, err := s.RecoverRefund(context.Background(), rf.ID)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err=%v, wantErr=%v", name, err, tc.wantErr)
		}
		if refunds.reviews[rf.ID] != tc.reason {
			t.Errorf("%s: review reason %q, want %q", name, refunds.reviews[rf.ID], tc.reason)
		}
		if got != nil && got.Status != tc.wantFinal {
			t.Errorf("%s: status %s, want %s", name, got.Status, tc.wantFinal)
		}
	}
}

func TestRecoverRefund_TerminalRefundIsANoOp(t *testing.T) {
	adapter := &checkingAdapter{}
	s, _, _, rf := processingRefund(t, adapter)
	rf.Status = domainrefund.StatusRefunded

	if _, err := s.RecoverRefund(context.Background(), rf.ID); err != nil {
		t.Fatal(err)
	}
	if adapter.refunds != 0 || len(adapter.asked) != 0 {
		t.Fatal("a finished refund must not touch the gateway")
	}
}

func TestProcessRefund_GatewayCompleted(t *testing.T) {
	parent := succeededTxn(100000)
	parent.GatewayReferenceID = "pi_1"
	rf, _ := domainrefund.New(parent.ID, 40000, 100000, 0, "r", "by")
	rf.AttemptedGateway = "stripe"
	refunds := &fakeRefunds{byID: map[uuid.UUID]*domainrefund.Refund{rf.ID: rf}}
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayRefundResponse{GatewayRefundID: "re_1", Status: ports.GatewayRefundStatusCompleted}}}

	s := NewService(&fakeTxns{txn: parent}, refunds, outbox, fakeTransactor{}, reg, noopLogger{}, noopMetrics{})
	got, err := s.ProcessRefund(context.Background(), rf.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != domainrefund.StatusRefunded {
		t.Errorf("expected REFUNDED, got %s", got.Status)
	}
	if got.GatewayRefundID != "re_1" {
		t.Errorf("expected gateway refund id recorded, got %q", got.GatewayRefundID)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeRefundSucceeded {
		t.Errorf("expected REFUND_SUCCEEDED event, got %+v", outbox.events)
	}
}

func TestProcessRefund_GatewayErrorStaysNonTerminal(t *testing.T) {
	parent := succeededTxn(100000)
	parent.GatewayReferenceID = "pi_1"
	rf, _ := domainrefund.New(parent.ID, 40000, 100000, 0, "r", "by")
	rf.AttemptedGateway = "stripe"
	refunds := &fakeRefunds{byID: map[uuid.UUID]*domainrefund.Refund{rf.ID: rf}}
	outbox := &fakeOutbox{}
	// A 5xx / gateway error is "maybe refunded, can't tell" — declaring it FAILED
	// and retrying would double-refund. It must stay non-terminal for reconciliation.
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "gateway_error"}}}

	s := NewService(&fakeTxns{txn: parent}, refunds, outbox, fakeTransactor{}, reg, noopLogger{}, noopMetrics{})
	got, err := s.ProcessRefund(context.Background(), rf.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != domainrefund.StatusProcessing {
		t.Errorf("a gateway error must leave the refund PROCESSING, got %s", got.Status)
	}
	if len(outbox.events) != 0 {
		t.Errorf("no terminal refund event should be written on an ambiguous gateway error, got %+v", outbox.events)
	}
}

type countingMetrics struct{ counts map[string]int }

func (m *countingMetrics) Increment(name string, tags map[string]string) {
	m.counts[name+"/"+tags["code"]]++
}
func (m *countingMetrics) Histogram(string, float64, map[string]string) {}
func (m *countingMetrics) Gauge(string, float64, map[string]string)     {}

type errorLog struct {
	noopLogger
	events []string
}

func (l *errorLog) Error(event string, _ map[string]any, _ error) { l.events = append(l.events, event) }

func processWith(t *testing.T, gwErr *ports.GatewayError) (*domainrefund.Refund, *countingMetrics, *errorLog) {
	t.Helper()
	parent := succeededTxn(100000)
	parent.GatewayReferenceID = "order_1"
	rf, _ := domainrefund.New(parent.ID, 40000, 100000, 0, "r", "merchant")
	rf.AttemptedGateway = "razorpay"
	refunds := &fakeRefunds{byID: map[uuid.UUID]*domainrefund.Refund{rf.ID: rf}}
	metrics := &countingMetrics{counts: map[string]int{}}
	log := &errorLog{}
	s := NewService(&fakeTxns{txn: parent}, refunds, &fakeOutbox{}, fakeTransactor{}, &fakeRegistry{adapter: &fakeAdapter{err: gwErr}}, log, metrics)

	got, err := s.ProcessRefund(context.Background(), rf.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got, metrics, log
}

func TestProcessRefund_AmbiguousRefundIsFlaggedForReview(t *testing.T) {
	got, metrics, log := processWith(t, &ports.GatewayError{Category: ports.ErrorCategoryAmbiguous, Code: "multiple_captured_payments"})
	if got.Status != domainrefund.StatusProcessing {
		t.Fatalf("an ambiguous refund must not be guessed at, got %s", got.Status)
	}
	if metrics.counts[ports.MetricRefundReviewRequired+"/multiple_captured_payments"] != 1 {
		t.Fatalf("a refund parked for a person must raise a metric with a stable code, got %v", metrics.counts)
	}
	if len(log.events) != 1 || log.events[0] != ports.LogEventRefundReviewRequired {
		t.Fatalf("and an error log, got %v", log.events)
	}
}

func TestProcessRefund_PlainNonTerminalErrorIsNotFlagged(t *testing.T) {
	_, metrics, log := processWith(t, &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout, Code: "network_error"})
	if len(log.events) != 0 || metrics.counts[ports.MetricRefundReviewRequired+"/network_error"] != 0 {
		t.Fatalf("only ambiguity needs a person, got logs=%v metrics=%v", log.events, metrics.counts)
	}
}

func TestProcessRefund_LookupFailureEndsTheRefund(t *testing.T) {
	got, _, _ := processWith(t, ports.RefundLookupFailed(&ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout, Code: "network_error"}))
	if got.Status != domainrefund.StatusFailed {
		t.Fatalf("a failed lookup means nothing was sent, so the refund must end REFUND_FAILED rather than stick, got %s", got.Status)
	}
}

func TestInitiateRefund_Success(t *testing.T) {
	parent := succeededTxn(100000)
	outbox := &fakeOutbox{}
	refunds := &fakeRefunds{sum: 0}
	s := svc(&fakeTxns{txn: parent}, refunds, outbox)

	res, err := s.InitiateRefund(context.Background(), InitiateInput{
		TransactionID: parent.ID, Scope: authz.AllMerchants(), Amount: 40000, Reason: "customer_request", InitiatedBy: "ops:1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rf := res.Refund
	if rf.Status != domainrefund.StatusInitiated {
		t.Errorf("expected REFUND_INITIATED, got %s", rf.Status)
	}
	if len(refunds.inserted) != 1 {
		t.Errorf("expected 1 refund inserted, got %d", len(refunds.inserted))
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeRefundInitiated {
		t.Errorf("expected REFUND_INITIATED event, got %+v", outbox.events)
	}
}

func TestInitiateRefund_TargetsTheGatewayThatProcessedThePayment(t *testing.T) {
	parent := succeededTxn(100000)
	parent.GatewayID = "stripe"
	parent.ActualGateway = "razorpay"
	refunds := &fakeRefunds{}
	s := svc(&fakeTxns{txn: parent}, refunds, &fakeOutbox{})

	res, err := s.InitiateRefund(context.Background(), InitiateInput{TransactionID: parent.ID, Scope: authz.AllMerchants(), Amount: 100, Reason: "r", InitiatedBy: "merchant"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Refund.AttemptedGateway != "razorpay" {
		t.Fatalf("the refund must go to the gateway that holds the reference, got %q", res.Refund.AttemptedGateway)
	}
}

func TestInitiateRefund_AnotherMerchantsTransactionIsNotFound(t *testing.T) {
	parent := succeededTxn(100000)
	refunds := &fakeRefunds{}
	outbox := &fakeOutbox{}
	s := svc(&fakeTxns{txn: parent}, refunds, outbox)

	for name, scope := range map[string]authz.Scope{"another merchant": authz.Merchant(uuid.New()), "zero scope": {}} {
		_, err := s.InitiateRefund(context.Background(), InitiateInput{TransactionID: parent.ID, Scope: scope, Amount: 100, Reason: "r", InitiatedBy: "merchant"})
		if !errors.Is(err, ports.ErrNotFound) || !errors.Is(err, authz.ErrDenied) {
			t.Fatalf("%s: expected a denial that reads as not found, got %v", name, err)
		}
	}
	if len(refunds.inserted) != 0 || len(outbox.events) != 0 {
		t.Error("nothing may be written outside the caller's scope")
	}

	if _, err := s.InitiateRefund(context.Background(), InitiateInput{TransactionID: parent.ID, Scope: authz.Merchant(parent.MerchantID), Amount: 100, Reason: "r", InitiatedBy: "merchant"}); err != nil {
		t.Fatalf("the owner must be able to refund, got %v", err)
	}
}

func TestInitiateRefund_NotRefundable(t *testing.T) {
	parent := succeededTxn(100000)
	parent.Status = transaction.StatusPending
	s := svc(&fakeTxns{txn: parent}, &fakeRefunds{}, &fakeOutbox{})

	_, err := s.InitiateRefund(context.Background(), InitiateInput{TransactionID: parent.ID, Scope: authz.AllMerchants(), Amount: 100, Reason: "r", InitiatedBy: "by"})
	if !errors.Is(err, ErrNotRefundable) {
		t.Fatalf("expected ErrNotRefundable, got %v", err)
	}
}

func TestInitiateRefund_OverRefundBlocked(t *testing.T) {
	parent := succeededTxn(100000)
	outbox := &fakeOutbox{}
	s := svc(&fakeTxns{txn: parent}, &fakeRefunds{sum: 60000}, outbox)

	_, err := s.InitiateRefund(context.Background(), InitiateInput{TransactionID: parent.ID, Scope: authz.AllMerchants(), Amount: 50000, Reason: "r", InitiatedBy: "by"})
	var over domainrefund.ErrOverRefund
	if !errors.As(err, &over) {
		t.Fatalf("expected ErrOverRefund, got %v", err)
	}
	if len(outbox.events) != 0 {
		t.Error("no event should be written when over-refund is blocked")
	}
}
