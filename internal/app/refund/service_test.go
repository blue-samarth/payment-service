package refund

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

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

func TestInitiateRefund_Success(t *testing.T) {
	parent := succeededTxn(100000)
	outbox := &fakeOutbox{}
	refunds := &fakeRefunds{sum: 0}
	s := svc(&fakeTxns{txn: parent}, refunds, outbox)

	res, err := s.InitiateRefund(context.Background(), InitiateInput{
		TransactionID: parent.ID, Amount: 40000, Reason: "customer_request", InitiatedBy: "ops:1",
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

func TestInitiateRefund_NotRefundable(t *testing.T) {
	parent := succeededTxn(100000)
	parent.Status = transaction.StatusPending
	s := svc(&fakeTxns{txn: parent}, &fakeRefunds{}, &fakeOutbox{})

	_, err := s.InitiateRefund(context.Background(), InitiateInput{TransactionID: parent.ID, Amount: 100, Reason: "r", InitiatedBy: "by"})
	if !errors.Is(err, ErrNotRefundable) {
		t.Fatalf("expected ErrNotRefundable, got %v", err)
	}
}

func TestInitiateRefund_OverRefundBlocked(t *testing.T) {
	parent := succeededTxn(100000)
	outbox := &fakeOutbox{}
	s := svc(&fakeTxns{txn: parent}, &fakeRefunds{sum: 60000}, outbox)

	_, err := s.InitiateRefund(context.Background(), InitiateInput{TransactionID: parent.ID, Amount: 50000, Reason: "r", InitiatedBy: "by"})
	var over domainrefund.ErrOverRefund
	if !errors.As(err, &over) {
		t.Fatalf("expected ErrOverRefund, got %v", err)
	}
	if len(outbox.events) != 0 {
		t.Error("no event should be written when over-refund is blocked")
	}
}
