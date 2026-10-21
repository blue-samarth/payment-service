package cancel_test

import . "samarth/payment-service/internal/app/cancel"

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type fakeStore struct {
	txn       *transaction.Transaction
	setResult bool
	setCalls  int
}

func (f *fakeStore) GetByID(context.Context, uuid.UUID) (*transaction.Transaction, error) {
	return f.txn, nil
}
func (f *fakeStore) SetCancelIntent(context.Context, uuid.UUID, transaction.Actor, transaction.CancelVia) (bool, error) {
	f.setCalls++
	return f.setResult, nil
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

func txnWith(status transaction.Status, intent bool) *transaction.Transaction {
	t, _ := transaction.New(uuid.New(), 1000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "", "", nil, 30)
	t.Status = status
	t.CancelIntent = intent
	return t
}

func TestCancel_AnotherMerchantsTransactionIsNotFound(t *testing.T) {
	store := &fakeStore{txn: txnWith(transaction.StatusProcessing, false), setResult: true}
	s := NewService(store, noopLogger{}, noopMetrics{})

	_, err := s.Cancel(context.Background(), CancelInput{TransactionID: store.txn.ID, MerchantID: uuid.New(), By: transaction.ActorMerchant, Via: transaction.CancelViaAPI})
	if !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("expected ports.ErrNotFound, got %v", err)
	}
	if store.setCalls != 0 {
		t.Fatal("no intent may be set on another merchant's transaction")
	}

	if _, err := s.Cancel(context.Background(), CancelInput{TransactionID: store.txn.ID, MerchantID: store.txn.MerchantID, By: transaction.ActorMerchant, Via: transaction.CancelViaAPI}); err != nil {
		t.Fatalf("the owner must be able to cancel, got %v", err)
	}
	if store.setCalls != 1 {
		t.Fatalf("expected the owner's cancel to set intent once, got %d", store.setCalls)
	}
}

func TestCancel_AlreadyTerminal(t *testing.T) {
	store := &fakeStore{txn: txnWith(transaction.StatusSucceeded, false)}
	s := NewService(store, noopLogger{}, noopMetrics{})

	res, err := s.Cancel(context.Background(), CancelInput{TransactionID: store.txn.ID})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != OutcomeAlreadyTerminal {
		t.Errorf("expected ALREADY_TERMINAL, got %s", res.Outcome)
	}
	if store.setCalls != 0 {
		t.Error("should not attempt to set intent on a terminal transaction")
	}
}

func TestCancel_AlreadyRequested(t *testing.T) {
	store := &fakeStore{txn: txnWith(transaction.StatusProcessing, true)}
	s := NewService(store, noopLogger{}, noopMetrics{})

	res, err := s.Cancel(context.Background(), CancelInput{TransactionID: store.txn.ID})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != OutcomeRequested {
		t.Errorf("expected CANCEL_REQUESTED, got %s", res.Outcome)
	}
	if store.setCalls != 0 {
		t.Error("should not re-set intent when already requested")
	}
}

type completedKey struct{ response []byte }

func (c completedKey) Reserve(context.Context, string, string) (bool, error) { return false, nil }
func (c completedKey) Lookup(_ context.Context, _ string) (bool, string, string, []byte, error) {
	return true, idempotency.RequestHash(cancelTxnID.String(), string(transaction.ActorMerchant), string(transaction.CancelViaAPI)), "COMPLETED", c.response, nil
}
func (c completedKey) Complete(context.Context, string, []byte) error { return nil }

type passThroughTx struct{}

func (passThroughTx) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

var cancelTxnID = uuid.New()

func TestCancel_ReplayReportsCurrentStatusNotTheSnapshot(t *testing.T) {
	settled := txnWith(transaction.StatusSucceeded, true)
	settled.ID = cancelTxnID
	store := &fakeStore{txn: settled}
	s := NewService(store, noopLogger{}, noopMetrics{})
	s.SetIdempotency(idempotency.NewGuard(completedKey{response: []byte(`{"outcome":"CANCEL_REQUESTED","status":"PROCESSING"}`)}, passThroughTx{}))

	res, err := s.Cancel(context.Background(), CancelInput{
		TransactionID: cancelTxnID, By: transaction.ActorMerchant, Via: transaction.CancelViaAPI, IdempotencyKey: "k1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != idempotency.Replayed || res.Outcome != OutcomeRequested {
		t.Fatalf("expected a replay of the original outcome, got %+v", res)
	}
	if res.Status != transaction.StatusSucceeded {
		t.Errorf("a replay after the payment settled must report the current status, not the stored PROCESSING, got %s", res.Status)
	}
	if store.setCalls != 0 {
		t.Error("a replay must not set the cancel intent again")
	}
}

func TestCancel_SetsIntent(t *testing.T) {
	store := &fakeStore{txn: txnWith(transaction.StatusProcessing, false), setResult: true}
	s := NewService(store, noopLogger{}, noopMetrics{})

	res, err := s.Cancel(context.Background(), CancelInput{TransactionID: store.txn.ID, By: transaction.ActorMerchant, Via: transaction.CancelViaAPI})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != OutcomeRequested {
		t.Errorf("expected CANCEL_REQUESTED, got %s", res.Outcome)
	}
	if store.setCalls != 1 {
		t.Errorf("expected one SetCancelIntent call, got %d", store.setCalls)
	}
}
