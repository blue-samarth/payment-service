//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestReads_JoinAmbientTransaction(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions")
	ctx := context.Background()

	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)
	txn := newTxn(t)

	err := tr.WithinTx(ctx, func(ctx context.Context) error {
		if err := repo.Insert(ctx, txn); err != nil {
			return err
		}
		got, err := repo.GetByID(ctx, txn.ID)
		if err != nil {
			return err
		}
		if got.ID != txn.ID {
			t.Errorf("read inside the tx returned the wrong row: %s", got.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("a read inside WithinTx must see its own uncommitted insert, got %v", err)
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

func writeOutboxEvent(t *testing.T, pg *testsupport.PG, outbox *postgres.OutboxWriter) uuid.UUID {
	t.Helper()
	tr := postgres.NewTransactor(pg.DB)
	aggID := uuid.New()
	event := ports.OutboxEvent{
		AggregateID:   aggID,
		AggregateType: "transaction",
		EventType:     ports.EventTypePaymentCreated,
		Payload:       []byte(`{"ok":true}`),
		EventVersion:  1,
	}
	if err := tr.WithinTx(context.Background(), func(ctx context.Context) error { return outbox.Write(ctx, event) }); err != nil {
		t.Fatalf("write: %v", err)
	}
	return aggID
}

func TestOutbox_SeedPartitionsAbsorbCurrentWrites(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "outbox_events")
	ctx := context.Background()

	// No partition_manager run here — the migration's deploy-relative seed alone
	// must place a NOW()-dated write in a dated weekly partition, not outbox_default.
	aggID := writeOutboxEvent(t, pg, postgres.NewOutboxWriter(pg.DB, pg.Q))

	var partition string
	if err := pg.DB.Pool().QueryRow(ctx,
		"SELECT tableoid::regclass::text FROM outbox_events WHERE aggregate_id = $1", aggID,
	).Scan(&partition); err != nil {
		t.Fatalf("locate partition: %v", err)
	}
	if partition == "outbox_default" {
		t.Error("a current-dated write landed in outbox_default; the migration seed should cover the current week")
	}
}

func TestOutbox_ClaimHidesEventFromConcurrentPoller(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "outbox_events")
	ctx := context.Background()

	outbox := postgres.NewOutboxWriter(pg.DB, pg.Q)
	writeOutboxEvent(t, pg, outbox)

	// First poller claims the event (PENDING -> PUBLISHING).
	claimed, err := outbox.PollPending(ctx, 0, 63, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("first poll should claim the event, got %d", len(claimed))
	}

	// A second poller (another relay worker) must not see the in-flight claim,
	// so it cannot publish the same event a second time.
	second, err := outbox.PollPending(ctx, 0, 63, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Errorf("a claimed event must be invisible to a concurrent poller until published or stale, got %d", len(second))
	}
}

func TestOutbox_StaleClaimIsReclaimed(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "outbox_events")
	ctx := context.Background()

	outbox := postgres.NewOutboxWriter(pg.DB, pg.Q)
	outbox.SetClaimTTL(200 * time.Millisecond)
	writeOutboxEvent(t, pg, outbox)

	claimed, err := outbox.PollPending(ctx, 0, 63, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("first poll should claim, got %d err=%v", len(claimed), err)
	}

	// Simulate a crashed worker: the claim is never published. After the TTL,
	// another poll must reclaim it rather than strand it in PUBLISHING forever.
	time.Sleep(300 * time.Millisecond)

	reclaimed, err := outbox.PollPending(ctx, 0, 63, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 {
		t.Errorf("a stale claim past its TTL should be reclaimed, got %d", len(reclaimed))
	}
}

func TestOutbox_MarkFailedReleasesClaimForRetry(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "outbox_events")
	ctx := context.Background()

	outbox := postgres.NewOutboxWriter(pg.DB, pg.Q)
	writeOutboxEvent(t, pg, outbox)

	claimed, err := outbox.PollPending(ctx, 0, 63, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("first poll should claim, got %d err=%v", len(claimed), err)
	}

	// A publish failure returns the event to PENDING with a past next_attempt_at,
	// so the next poll re-claims it with a bumped attempt count.
	if err := outbox.MarkFailed(ctx, claimed[0].ID, claimed[0].CreatedAt, "sns down", time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("mark failed: %v", err)
	}

	retried, err := outbox.PollPending(ctx, 0, 63, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(retried) != 1 {
		t.Fatalf("a failed publish should make the event claimable again, got %d", len(retried))
	}
	if retried[0].Attempts != 1 {
		t.Errorf("expected attempts bumped to 1 after a failed publish, got %d", retried[0].Attempts)
	}
}

func TestTransaction_ProcessingTimeoutRoundTrips(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions")
	ctx := context.Background()

	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)

	// Includes durations >= 1 day, which Postgres renders as "1 day ..." — the
	// old hand-rolled interval text parser failed on the singular "day" form and
	// on whole-day values, breaking GetByID entirely.
	for _, d := range []time.Duration{30 * time.Second, 24 * time.Hour, 25*time.Hour + 90*time.Second} {
		txn := newTxn(t)
		started := time.Now().UTC()
		timeout := d
		txn.ProcessingStartedAt = &started
		txn.ProcessingTimeout = &timeout

		if err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.Insert(ctx, txn) }); err != nil {
			t.Fatalf("insert (d=%s): %v", d, err)
		}
		got, err := repo.GetByID(ctx, txn.ID)
		if err != nil {
			t.Fatalf("get (d=%s): %v", d, err)
		}
		if got.ProcessingTimeout == nil || *got.ProcessingTimeout != d {
			t.Errorf("processing_timeout %s did not round-trip, got %v", d, got.ProcessingTimeout)
		}
	}
}

func TestUpdateStatus_NotFoundIsDistinctFromVersionConflict(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions")
	ctx := context.Background()

	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)

	// (a) A transaction that was never inserted must surface as ErrNotFound,
	// not a misleading ErrVersionConflict.
	ghost := newTxn(t)
	err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.UpdateStatus(ctx, ghost) })
	if !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("expected ErrNotFound updating a nonexistent transaction, got %v", err)
	}

	// (b) A genuine stale-version write on an existing row is ErrVersionConflict.
	txn := newTxn(t)
	if err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.Insert(ctx, txn) }); err != nil {
		t.Fatal(err)
	}
	fresh, _ := repo.GetByID(ctx, txn.ID)
	stale := *fresh

	_ = transaction.TransitionState(fresh, transaction.StatusProcessing, transaction.ActorSystem)
	if err := tr.WithinTx(ctx, func(ctx context.Context) error { return repo.UpdateStatus(ctx, fresh) }); err != nil {
		t.Fatal(err)
	}

	_ = transaction.TransitionState(&stale, transaction.StatusProcessing, transaction.ActorSystem)
	err = tr.WithinTx(ctx, func(ctx context.Context) error { return repo.UpdateStatus(ctx, &stale) })
	if !errors.Is(err, postgres.ErrVersionConflict) {
		t.Fatalf("expected ErrVersionConflict on a stale update, got %v", err)
	}
}

func TestTransactor_PanicInsideRollsBack(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions")
	ctx := context.Background()

	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)
	txn := newTxn(t)

	func() {
		defer func() { _ = recover() }() // the panic re-raises after rollback
		_ = tr.WithinTx(ctx, func(ctx context.Context) error {
			if err := repo.Insert(ctx, txn); err != nil {
				return err
			}
			panic("boom")
		})
	}()

	// The deferred rollback must have undone the insert despite the panic.
	if _, err := repo.GetByID(ctx, txn.ID); !errors.Is(err, postgres.ErrNotFound) {
		t.Fatalf("expected the insert to be rolled back on panic (ErrNotFound), got %v", err)
	}
}
