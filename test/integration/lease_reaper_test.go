//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/adapters/postgres"
	"samarth/payment-service/internal/domain/transaction"
	leaseexpiry "samarth/payment-service/internal/jobs/lease_expiry"
	"samarth/payment-service/internal/testsupport"
)

func seedStuckTxn(t *testing.T, pg *testsupport.PG, gatewayID, reference string) *transaction.Transaction {
	t.Helper()
	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)

	txn, err := transaction.New(uuid.New(), 150000, "INR", transaction.PaymentMethodCard, gatewayID, uuid.New(), "b@e.com", "o", nil, 30)
	if err != nil {
		t.Fatal(err)
	}
	txn.Status = transaction.StatusProcessing
	txn.AttemptedGateway = gatewayID
	txn.ActualGateway = gatewayID
	txn.GatewayReferenceID = reference
	past := time.Now().UTC().Add(-time.Hour)
	d := time.Second
	txn.ProcessingStartedAt = &past
	txn.ProcessingTimeout = &d
	if err := tr.WithinTx(context.Background(), func(ctx context.Context) error { return repo.Insert(ctx, txn) }); err != nil {
		t.Fatalf("seed stuck txn: %v", err)
	}
	return txn
}

func TestLeaseReaper_RecoversStuckTransactionViaStatusCheck(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)
	ctx := context.Background()

	// The gateway reports the payment actually SUCCEEDED while we were stuck.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"pi_stuck","status":"succeeded","amount":150000,"currency":"inr"}`))
	}))
	defer srv.Close()

	stuck := seedStuckTxn(t, pg, "stripe", "pi_stuck")

	svc := buildService(pg, srv.URL)
	reaper := leaseexpiry.New(
		postgres.NewTransactionRepository(pg.DB, pg.Q),
		svc,
		postgres.NewIdempotencyRepository(pg.DB, pg.Q),
		discardLogger(),
		leaseexpiry.Config{IdempotencyProcessingTimeout: time.Minute},
	)

	if err := reaper.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	persisted, err := postgres.NewTransactionRepository(pg.DB, pg.Q).GetByID(ctx, stuck.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != transaction.StatusSucceeded {
		t.Errorf("expected the stuck PROCESSING txn reconciled to SUCCEEDED, got %s", persisted.Status)
	}

	var events int
	_ = pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_id=$1", stuck.ID).Scan(&events)
	if events != 1 {
		t.Errorf("expected one terminal outbox event from recovery, got %d", events)
	}
}

func TestLeaseReaper_SweepsStaleIdempotencyKeysOnly(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events", "idempotency_keys")
	ctx := context.Background()

	insert := func(hash, status string, createdAgo, expiresIn time.Duration) {
		_, err := pg.DB.Pool().Exec(ctx,
			`INSERT INTO idempotency_keys (composite_key_hash, request_hash, response, status, created_at, expires_at)
			 VALUES ($1, 'rh', '{}'::jsonb, $2, NOW() - ($3 * INTERVAL '1 second'), NOW() + ($4 * INTERVAL '1 second'))`,
			hash, status, int64(createdAgo.Seconds()), int64(expiresIn.Seconds()),
		)
		if err != nil {
			t.Fatalf("seed idempotency key %s: %v", hash, err)
		}
	}

	insert("stale-processing", "PROCESSING", time.Hour, 23*time.Hour)      // crashed mid-request -> swept
	insert("fresh-processing", "PROCESSING", 10*time.Second, 24*time.Hour) // in-flight -> kept
	insert("expired-completed", "COMPLETED", 25*time.Hour, -time.Hour)     // past TTL -> purged
	insert("live-completed", "COMPLETED", time.Hour, 23*time.Hour)         // valid replay cache -> kept

	reaper := leaseexpiry.New(
		postgres.NewTransactionRepository(pg.DB, pg.Q),
		buildService(pg, "http://unused"),
		postgres.NewIdempotencyRepository(pg.DB, pg.Q),
		discardLogger(),
		leaseexpiry.Config{IdempotencyProcessingTimeout: 5 * time.Minute},
	)
	if err := reaper.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	exists := func(hash string) bool {
		var n int
		_ = pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM idempotency_keys WHERE composite_key_hash=$1", hash).Scan(&n)
		return n == 1
	}

	if exists("stale-processing") {
		t.Error("a PROCESSING key older than the timeout should be swept so retries can proceed")
	}
	if !exists("fresh-processing") {
		t.Error("a recently-started PROCESSING key must not be swept")
	}
	if exists("expired-completed") {
		t.Error("a COMPLETED key past expires_at should be purged")
	}
	if !exists("live-completed") {
		t.Error("a COMPLETED key within its TTL must be kept for replay")
	}
}
