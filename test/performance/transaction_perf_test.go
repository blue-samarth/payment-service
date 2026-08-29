//go:build performance

package performance

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/transaction"
)

func newTransaction() *transaction.Transaction {
	now := time.Now()
	return &transaction.Transaction{
		ID:                      uuid.New(),
		MerchantID:              uuid.New(),
		Amount:                  250_000,
		Currency:                "INR",
		PaymentMethod:           transaction.PaymentMethodCard,
		Status:                  transaction.StatusPending,
		Version:                 1,
		GatewayID:               "stripe",
		EstimatedTimeoutSeconds: 30,
		CustomerID:              uuid.New(),
		CustomerEmail:           "buyer@example.com",
		CreatedAt:               now,
		UpdatedAt:               now,
	}
}

func TestPerf_TransitionState(t *testing.T) {
	tx := newTransaction()

	enforce(t, "transaction.TransitionState", Baseline{NsPerOp: 31, AllocsPerOp: 0, BytesPerOp: 0}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx.Status = transaction.StatusPending
			if err := transaction.TransitionState(tx, transaction.StatusProcessing, transaction.ActorSystem); err != nil {
				b.Fatalf("unexpected transition error: %v", err)
			}
		}
	})
}

func TestPerf_TransitionState_Rejects(t *testing.T) {
	tx := newTransaction()
	tx.Status = transaction.StatusSucceeded

	enforce(t, "transaction.TransitionState (invalid)", Baseline{NsPerOp: 20, AllocsPerOp: 1, BytesPerOp: 48}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tx.Status = transaction.StatusSucceeded
			if err := transaction.TransitionState(tx, transaction.StatusPending, transaction.ActorSystem); err == nil {
				b.Fatal("expected invalid transition to be rejected")
			}
		}
	})
}

func TestPerf_TransactionValidate(t *testing.T) {
	tx := newTransaction()

	enforce(t, "transaction.Validate", Baseline{NsPerOp: 10, AllocsPerOp: 0, BytesPerOp: 0}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := tx.Validate(); err != nil {
				b.Fatalf("unexpected validation error: %v", err)
			}
		}
	})
}

func TestPerf_IsLeaseExpired(t *testing.T) {
	tx := newTransaction()
	tx.Status = transaction.StatusProcessing
	started := time.Now()
	timeout := 30 * time.Second
	tx.ProcessingStartedAt = &started
	tx.ProcessingTimeout = &timeout

	if tx.IsLeaseExpired() {
		t.Fatal("lease must still be live for the benchmark to exercise the time path")
	}

	enforce(t, "transaction.IsLeaseExpired", Baseline{NsPerOp: 30, AllocsPerOp: 0, BytesPerOp: 0}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sinkBool = tx.IsLeaseExpired()
		}
	})
}
