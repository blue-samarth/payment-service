package webhook_test

import . "samarth/payment-service/internal/app/webhook"

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type fakeTxns struct {
	txn     *transaction.Transaction
	byID    *transaction.Transaction
	updates int
	saved   []transaction.Transaction
	reviews []string
}

func (f *fakeTxns) GetByGatewayReference(context.Context, string, string) (*transaction.Transaction, error) {
	return f.txn, nil
}
func (f *fakeTxns) FindByID(_ context.Context, id uuid.UUID) (*transaction.Transaction, error) {
	if f.byID == nil || f.byID.ID != id {
		return nil, nil
	}
	return f.byID, nil
}
func (f *fakeTxns) UpdateStatus(_ context.Context, t *transaction.Transaction) error {
	f.updates++
	f.saved = append(f.saved, *t)
	return nil
}
func (f *fakeTxns) MarkReviewRequired(_ context.Context, _ uuid.UUID, reason string) (bool, error) {
	f.reviews = append(f.reviews, reason)
	return true, nil
}

type fakeWebhooks struct {
	recorded bool
	rawCount int
}

func (f *fakeWebhooks) RecordEvent(context.Context, string, string) (bool, error) {
	return f.recorded, nil
}
func (f *fakeWebhooks) InsertRawMetadata(context.Context, uuid.UUID, string, []byte) error {
	f.rawCount++
	return nil
}

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

type countingMetrics struct{ counts map[string]int }

func (m *countingMetrics) Increment(name string, tags map[string]string) {
	m.counts[name+"/"+tags["reason"]]++
}
func (m *countingMetrics) Histogram(string, float64, map[string]string) {}
func (m *countingMetrics) Gauge(string, float64, map[string]string)     {}

func TestProcess_CaptureThatDoesNotMatchThePaymentGoesToReview(t *testing.T) {
	for name, tc := range map[string]struct {
		amount   int64
		currency string
		reason   string
	}{
		"amount differs":   {149999, "INR", "webhook_amount_mismatch"},
		"currency differs": {150000, "USD", "webhook_currency_mismatch"},
		"amount missing":   {0, "INR", "webhook_amount_missing"},
		"currency missing": {150000, "", "webhook_currency_missing"},
	} {
		txn := processingTxn()
		txns := &fakeTxns{txn: txn}
		outbox := &fakeOutbox{}
		metrics := &countingMetrics{counts: map[string]int{}}
		s := NewService(txns, &fakeWebhooks{recorded: true}, outbox, fakeTransactor{}, noopLogger{}, metrics)

		out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_1", Status: "success", Amount: tc.amount, Currency: tc.currency}, []byte(`{}`))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !out.Mismatch || out.Resolved || txn.Status != transaction.StatusProcessing || len(outbox.events) != 0 {
			t.Errorf("%s: a signed capture that doesn't match must never mark the payment SUCCEEDED, got %+v status=%s events=%d", name, out, txn.Status, len(outbox.events))
		}
		if len(txns.reviews) != 1 || txns.reviews[0] != tc.reason {
			t.Errorf("%s: expected a review mark %q, got %v", name, tc.reason, txns.reviews)
		}
		if metrics.counts[ports.MetricWebhookCaptureMismatch+"/"+tc.reason] != 1 {
			t.Errorf("%s: expected the mismatch to be counted, got %v", name, metrics.counts)
		}
	}
}

func failedTxn() *transaction.Transaction {
	t := processingTxn()
	t.Status = transaction.StatusFailed
	t.FailureReason = &transaction.FailureReason{Category: "gateway_declined", Source: transaction.FailureReasonSourceGateway}
	return t
}

func TestProcess_LateCaptureAfterFailedSettlesTheCharge(t *testing.T) {
	txn := failedTxn()
	txns := &fakeTxns{txn: txn}
	outbox := &fakeOutbox{}
	metrics := &countingMetrics{counts: map[string]int{}}
	s := NewService(txns, &fakeWebhooks{recorded: true}, outbox, fakeTransactor{}, noopLogger{}, metrics)

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_1", Status: "success", Amount: 150000, Currency: "INR"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Resolved || txn.Status != transaction.StatusSucceeded || txn.FailureReason != nil {
		t.Fatalf("the customer was charged, so the payment must become SUCCEEDED, got %+v status=%s reason=%+v", out, txn.Status, txn.FailureReason)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypePaymentSucceeded {
		t.Fatalf("expected a PAYMENT_SUCCEEDED event, got %+v", outbox.events)
	}
	if metrics.counts[ports.MetricTransactionLateCapture+"/"] != 1 {
		t.Fatalf("a late capture must raise an alert, got %v", metrics.counts)
	}
}

func TestProcess_LateCaptureThatDoesNotMatchStaysFailed(t *testing.T) {
	txn := failedTxn()
	txns := &fakeTxns{txn: txn}
	outbox := &fakeOutbox{}
	metrics := &countingMetrics{counts: map[string]int{}}
	s := NewService(txns, &fakeWebhooks{recorded: true}, outbox, fakeTransactor{}, noopLogger{}, metrics)

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_1", Status: "success", Amount: 1, Currency: "INR"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Mismatch || txn.Status != transaction.StatusFailed || len(outbox.events) != 0 {
		t.Fatalf("a mismatched late capture must stay FAILED for a person, got %+v status=%s", out, txn.Status)
	}
	if len(txns.reviews) != 0 {
		t.Fatal("the review mark is only for PROCESSING payments, which the resolve endpoint can settle")
	}
	if metrics.counts[ports.MetricWebhookCaptureMismatch+"/webhook_amount_mismatch"] != 1 {
		t.Fatalf("it must still raise the mismatch alert, got %v", metrics.counts)
	}
}

func TestProcess_LateFailureAfterFailedIsANoOp(t *testing.T) {
	txn := failedTxn()
	txns := &fakeTxns{txn: txn}
	outbox := &fakeOutbox{}
	s := NewService(txns, &fakeWebhooks{recorded: true}, outbox, fakeTransactor{}, noopLogger{}, noopMetrics{})

	if _, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_1", Status: "failed"}, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if txns.updates != 0 || len(outbox.events) != 0 {
		t.Fatal("only a capture reopens a FAILED payment")
	}
}

func processingTxn() *transaction.Transaction {
	t, _ := transaction.New(uuid.New(), 150000, "INR", transaction.PaymentMethodCard, "razorpay", uuid.New(), "b@e.com", "o", nil, 30)
	t.Status = transaction.StatusProcessing
	t.GatewayReferenceID = "order_1"
	return t
}

func TestProcess_ResolvesProcessingToSucceeded(t *testing.T) {
	txn := processingTxn()
	txns := &fakeTxns{txn: txn}
	webhooks := &fakeWebhooks{recorded: true}
	outbox := &fakeOutbox{}
	s := NewService(txns, webhooks, outbox, fakeTransactor{}, noopLogger{}, noopMetrics{})

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_1", Status: "success", Amount: 150000, Currency: "INR"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Resolved || out.Status != transaction.StatusSucceeded {
		t.Errorf("expected resolved to SUCCEEDED, got %+v", out)
	}
	if txns.updates != 1 || len(outbox.events) != 1 || webhooks.rawCount != 1 {
		t.Errorf("expected update+event+raw metadata, got updates=%d events=%d raw=%d", txns.updates, len(outbox.events), webhooks.rawCount)
	}
	if outbox.events[0].EventType != ports.EventTypePaymentSucceeded {
		t.Errorf("expected PAYMENT_SUCCEEDED, got %s", outbox.events[0].EventType)
	}
}

func TestProcess_DuplicateIsNoop(t *testing.T) {
	txns := &fakeTxns{txn: processingTxn()}
	webhooks := &fakeWebhooks{recorded: false} // already recorded -> duplicate
	outbox := &fakeOutbox{}
	s := NewService(txns, webhooks, outbox, fakeTransactor{}, noopLogger{}, noopMetrics{})

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_1", Status: "success", Amount: 150000, Currency: "INR"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Duplicate || txns.updates != 0 || len(outbox.events) != 0 {
		t.Errorf("duplicate webhook must be a no-op, got %+v updates=%d events=%d", out, txns.updates, len(outbox.events))
	}
}

func TestProcess_UnknownTransaction(t *testing.T) {
	txns := &fakeTxns{txn: nil} // not found
	s := NewService(txns, &fakeWebhooks{recorded: true}, &fakeOutbox{}, fakeTransactor{}, noopLogger{}, noopMetrics{})

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "nope", Status: "success"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !out.UnknownTxn {
		t.Errorf("expected UnknownTxn, got %+v", out)
	}
}

func unreferencedTxn(gateway string) *transaction.Transaction {
	t := processingTxn()
	t.GatewayReferenceID = ""
	t.ActualGateway = gateway
	return t
}

func TestProcess_MatchesByTransactionIDWhenTheReferenceWasNeverSaved(t *testing.T) {
	txn := unreferencedTxn("razorpay")
	txns := &fakeTxns{byID: txn}
	outbox := &fakeOutbox{}
	s := NewService(txns, &fakeWebhooks{recorded: true}, outbox, fakeTransactor{}, noopLogger{}, noopMetrics{})

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_9", TransactionID: txn.ID.String(), Status: "success", Amount: 150000, Currency: "INR"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Resolved || out.Status != transaction.StatusSucceeded {
		t.Fatalf("a late webhook must settle a transaction whose reference was lost, got %+v", out)
	}
	if len(txns.saved) != 1 || txns.saved[0].GatewayReferenceID != "order_9" {
		t.Errorf("the recovered reference must be persisted, got %+v", txns.saved)
	}
}

func TestProcess_NonTerminalWebhookStillRecordsTheRecoveredReference(t *testing.T) {
	txn := unreferencedTxn("razorpay")
	txns := &fakeTxns{byID: txn}
	s := NewService(txns, &fakeWebhooks{recorded: true}, &fakeOutbox{}, fakeTransactor{}, noopLogger{}, noopMetrics{})

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_9", TransactionID: txn.ID.String(), Status: "pending"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if out.Resolved || len(txns.saved) != 1 || txns.saved[0].GatewayReferenceID != "order_9" {
		t.Errorf("the reference must be saved so the reaper can check status, got %+v saved=%+v", out, txns.saved)
	}
}

func TestProcess_TransactionIDFallbackRefusesMismatches(t *testing.T) {
	cases := map[string]struct {
		txn     *transaction.Transaction
		gateway string
		txID    func(*transaction.Transaction) string
	}{
		"other gateway's transaction": {unreferencedTxn("stripe"), "razorpay", func(t *transaction.Transaction) string { return t.ID.String() }},
		"reference already set": {func() *transaction.Transaction {
			t := unreferencedTxn("razorpay")
			t.GatewayReferenceID = "order_1"
			return t
		}(), "razorpay", func(t *transaction.Transaction) string { return t.ID.String() }},
		"not a UUID": {unreferencedTxn("razorpay"), "razorpay", func(*transaction.Transaction) string { return "not-a-uuid" }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			txns := &fakeTxns{byID: tc.txn}
			s := NewService(txns, &fakeWebhooks{recorded: true}, &fakeOutbox{}, fakeTransactor{}, noopLogger{}, noopMetrics{})

			out, err := s.Process(context.Background(), tc.gateway, Event{EventID: "evt", GatewayReferenceID: "order_9", TransactionID: tc.txID(tc.txn), Status: "success", Amount: 150000, Currency: "INR"}, []byte(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			if !out.UnknownTxn || txns.updates != 0 {
				t.Errorf("the fallback must not attach a webhook to a transaction it does not belong to, got %+v updates=%d", out, txns.updates)
			}
		})
	}
}

func TestProcess_NonTerminalStatusNoTransition(t *testing.T) {
	txn := processingTxn()
	txns := &fakeTxns{txn: txn}
	s := NewService(txns, &fakeWebhooks{recorded: true}, &fakeOutbox{}, fakeTransactor{}, noopLogger{}, noopMetrics{})

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_1", Status: "pending"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if out.Resolved || txns.updates != 0 {
		t.Errorf("a non-terminal webhook should not transition, got %+v updates=%d", out, txns.updates)
	}
}
