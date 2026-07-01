//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/adapters/postgres"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
	"samarth/payment-service/internal/testsupport"
)

func newTxn(t *testing.T) *transaction.Transaction {
	t.Helper()
	txn, err := transaction.New(uuid.New(), 150000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@e.com", "order #42", map[string]any{"source": "test"}, 30)
	if err != nil {
		t.Fatalf("build transaction: %v", err)
	}
	txn.AttemptedGateway = "stripe"
	return txn
}

func TestTransactionRoundTrip_JSONB(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions")
	ctx := context.Background()

	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)

	txn := newTxn(t)
	txn.MethodDetails = &transaction.MethodDetails{Card: &transaction.CardDetails{
		CardBrand: "visa", Last4: "4242", Network: "visa",
	}}

	if err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.Insert(ctx, txn) }); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := repo.GetByID(ctx, txn.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Amount != txn.Amount || got.Status != transaction.StatusPending || got.Version != 1 {
		t.Errorf("scalar mismatch: amount=%d status=%s version=%d", got.Amount, got.Status, got.Version)
	}
	if got.Metadata["source"] != "test" {
		t.Errorf("metadata JSONB did not round-trip: %v", got.Metadata)
	}
	if got.MethodDetails == nil || got.MethodDetails.Card == nil || got.MethodDetails.Card.Last4 != "4242" {
		t.Errorf("method_details JSONB did not round-trip: %+v", got.MethodDetails)
	}
}

func TestOptimisticLock_Conflict(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions")
	ctx := context.Background()

	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)

	txn := newTxn(t)
	if err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.Insert(ctx, txn) }); err != nil {
		t.Fatalf("insert: %v", err)
	}

	first, _ := repo.GetByID(ctx, txn.ID)
	stale := *first

	_ = transaction.TransitionState(first, transaction.StatusProcessing, transaction.ActorSystem)
	if err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.UpdateStatus(ctx, first) }); err != nil {
		t.Fatalf("first update: %v", err)
	}
	if first.Version != 2 {
		t.Errorf("expected version bumped to 2, got %d", first.Version)
	}

	_ = transaction.TransitionState(&stale, transaction.StatusProcessing, transaction.ActorSystem)
	err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.UpdateStatus(ctx, &stale) })
	if !errors.Is(err, postgres.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict on stale update, got %v", err)
	}
}

func TestOptimisticLock_ConcurrentSingleWinner(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions")
	ctx := context.Background()

	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)

	txn := newTxn(t)
	if err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.Insert(ctx, txn) }); err != nil {
		t.Fatalf("insert: %v", err)
	}
	base, _ := repo.GetByID(ctx, txn.ID)

	const n = 12
	var wins, conflicts int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := *base
			_ = transaction.TransitionState(&c, transaction.StatusProcessing, transaction.ActorSystem)
			err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.UpdateStatus(ctx, &c) })
			switch {
			case err == nil:
				atomic.AddInt64(&wins, 1)
			case errors.Is(err, postgres.ErrVersionConflict):
				atomic.AddInt64(&conflicts, 1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	if wins != 1 {
		t.Errorf("expected exactly 1 winner, got %d (conflicts=%d)", wins, conflicts)
	}
}

func TestTransactor_RollsBackOnError(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions")
	ctx := context.Background()

	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)
	txn := newTxn(t)

	boom := errors.New("boom")
	err := tr.WithinTx(ctx, func(ctx context.Context) error {
		if err := repo.Insert(ctx, txn); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}

	if _, err := repo.GetByID(ctx, txn.ID); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("expected transaction rolled back (ErrNotFound), got %v", err)
	}
}

func TestTransactor_CommitsTransactionAndOutboxAtomically(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "outbox_events")
	ctx := context.Background()

	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	outbox := postgres.NewOutboxWriter(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)
	txn := newTxn(t)

	event := ports.OutboxEvent{
		AggregateID:   txn.ID,
		AggregateType: "transaction",
		EventType:     ports.EventTypePaymentCreated,
		Payload:       []byte(`{"transaction_id":"` + txn.ID.String() + `"}`),
		EventVersion:  1,
	}

	err := tr.WithinTx(ctx, func(ctx context.Context) error {
		if err := repo.Insert(ctx, txn); err != nil {
			return err
		}
		return outbox.Write(ctx, event)
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	if _, err := repo.GetByID(ctx, txn.ID); err != nil {
		t.Errorf("transaction not persisted: %v", err)
	}
	events, err := outbox.PollPending(ctx, 0, 63, 10)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(events) != 1 || events[0].AggregateID != txn.ID {
		t.Errorf("expected 1 outbox event for the transaction, got %d", len(events))
	}
}

func TestLease_SingleFlight(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease")
	ctx := context.Background()

	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	lease := postgres.NewLeaseRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)

	txn := newTxn(t)
	if err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.Insert(ctx, txn) }); err != nil {
		t.Fatalf("insert: %v", err)
	}

	const n = 16
	var acquired int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := tr.WithinTx(ctx, func(ctx context.Context) error {
				ok, _, err := lease.Acquire(ctx, txn.ID, txn.ID, 30)
				if err != nil {
					return err
				}
				if ok {
					atomic.AddInt64(&acquired, 1)
				}
				return nil
			})
			if err != nil {
				t.Errorf("acquire: %v", err)
			}
		}()
	}
	wg.Wait()

	if acquired != 1 {
		t.Errorf("expected exactly 1 lease acquisition, got %d", acquired)
	}
}

func TestOutbox_WritePollMarkPublished(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "outbox_events")
	ctx := context.Background()

	outbox := postgres.NewOutboxWriter(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)
	aggID := uuid.New()

	event := ports.OutboxEvent{
		AggregateID:   aggID,
		AggregateType: "transaction",
		EventType:     ports.EventTypePaymentSucceeded,
		Payload:       []byte(`{"ok":true}`),
		EventVersion:  1,
	}
	if err := tr.WithinTx(ctx, func(ctx context.Context) error { return outbox.Write(ctx, event) }); err != nil {
		t.Fatalf("write: %v", err)
	}

	events, err := outbox.PollPending(ctx, 0, 63, 10)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 pending event, got %d", len(events))
	}

	if err := outbox.MarkPublished(ctx, events[0].ID, events[0].CreatedAt); err != nil {
		t.Fatalf("mark published: %v", err)
	}

	remaining, err := outbox.PollPending(ctx, 0, 63, 10)
	if err != nil {
		t.Fatalf("poll after publish: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("expected no pending events after publish, got %d", len(remaining))
	}
}
