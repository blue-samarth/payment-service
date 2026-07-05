package cancel

import (
	"context"
	"testing"

	"github.com/google/uuid"

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
