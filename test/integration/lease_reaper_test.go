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
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/domain/transaction"
	leaseexpiry "samarth/payment-service/internal/jobs/lease_expiry"
	"samarth/payment-service/internal/testsupport"
)

func seedStuckTxn(t *testing.T, pg *testsupport.PG, gatewayID, reference string) *transaction.Transaction {
	t.Helper()
	return seedStuckTxnStartedAgo(t, pg, gatewayID, reference, time.Hour)
}

func seedStuckTxnStartedAgo(t *testing.T, pg *testsupport.PG, gatewayID, reference string, startedAgo time.Duration) *transaction.Transaction {
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
	past := time.Now().UTC().Add(-startedAgo)
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

func newReaper(pg *testsupport.PG, stripeURL string) *leaseexpiry.Reaper {
	return leaseexpiry.New(
		postgres.NewTransactionRepository(pg.DB, pg.Q),
		buildService(pg, stripeURL),
		postgres.NewIdempotencyRepository(pg.DB, pg.Q),
		discardLogger(),
		leaseexpiry.Config{IdempotencyProcessingTimeout: time.Minute},
	)
}

func TestLeaseReaper_ReplaysInterruptedChargeWithTheSameIdempotencyKey(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)
	ctx := context.Background()

	var gotMethod, gotPath, gotKey string
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		gotMethod, gotPath, gotKey = r.Method, r.URL.Path, r.Header.Get("Idempotency-Key")
		_, _ = w.Write([]byte(`{"id":"pi_replayed","status":"succeeded","amount":150000,"currency":"inr"}`))
	}))
	defer srv.Close()

	stuck := seedStuckTxn(t, pg, "stripe", "")
	if err := newReaper(pg, srv.URL).RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if calls != 1 || gotMethod != http.MethodPost || gotPath != "/v1/payment_intents" || gotKey == "" {
		t.Fatalf("expected one idempotent create replay, got calls=%d %s %s key=%q", calls, gotMethod, gotPath, gotKey)
	}
	persisted, err := postgres.NewTransactionRepository(pg.DB, pg.Q).GetByID(ctx, stuck.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != transaction.StatusSucceeded || persisted.GatewayReferenceID != "pi_replayed" {
		t.Errorf("expected the replayed result recorded, got %s %q", persisted.Status, persisted.GatewayReferenceID)
	}
}

func TestLeaseReaper_MarksUnrecoverableChargeForReviewAndStopsListingIt(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)
	ctx := context.Background()

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
	defer srv.Close()

	stuck := seedStuckTxnStartedAgo(t, pg, "stripe", "", 48*time.Hour)
	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	if err := newReaper(pg, srv.URL).RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if calls != 0 {
		t.Fatal("past the idempotency window nothing may be sent to the gateway")
	}
	var reason *string
	var markedAt *time.Time
	if err := pg.DB.Pool().QueryRow(ctx, "SELECT review_reason, review_required_at FROM transactions WHERE id=$1", stuck.ID).Scan(&reason, &markedAt); err != nil {
		t.Fatal(err)
	}
	if reason == nil || *reason != "replay_window_passed" || markedAt == nil {
		t.Fatalf("expected the row marked for review, got reason=%v at=%v", reason, markedAt)
	}
	persisted, err := repo.GetByID(ctx, stuck.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != transaction.StatusProcessing {
		t.Errorf("the outcome is unknown, so the status must stay PROCESSING, got %s", persisted.Status)
	}

	ids, err := repo.ListExpiredLeaseIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if id == stuck.ID {
			t.Fatal("a transaction marked for review must not be re-listed every tick")
		}
	}
	if marked, err := repo.MarkReviewRequired(ctx, stuck.ID, "again"); err != nil || marked {
		t.Fatalf("marking is once only, got marked=%v err=%v", marked, err)
	}
	if n, err := repo.CountPendingReview(ctx); err != nil || n != 1 {
		t.Fatalf("expected one pending review, got %d err=%v", n, err)
	}
	if persisted.ReviewRequiredAt == nil || persisted.ReviewReason != "replay_window_passed" {
		t.Fatalf("GetByID must surface the review fields, got %v %q", persisted.ReviewRequiredAt, persisted.ReviewReason)
	}

	resolved, err := buildService(pg, srv.URL).ResolveReview(ctx, stuck.ID, payment.ResolveInput{
		Status: transaction.StatusSucceeded, GatewayReferenceID: "pi_found_in_dashboard",
	})
	if err != nil {
		t.Fatalf("ResolveReview: %v", err)
	}
	if resolved.Status != transaction.StatusSucceeded {
		t.Fatalf("got %s", resolved.Status)
	}
	reloaded, err := repo.GetByID(ctx, stuck.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != transaction.StatusSucceeded || reloaded.GatewayReferenceID != "pi_found_in_dashboard" {
		t.Errorf("resolution must persist, got %s %q", reloaded.Status, reloaded.GatewayReferenceID)
	}
	var events int
	_ = pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_id=$1", stuck.ID).Scan(&events)
	if events != 1 {
		t.Errorf("a resolution must publish one terminal event, got %d", events)
	}
	if n, err := repo.CountPendingReview(ctx); err != nil || n != 0 {
		t.Errorf("a resolved row is no longer pending, got %d err=%v", n, err)
	}
}

func TestTransactionRepository_GatewayReferenceMatchesTheGatewayActuallyCalled(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	ctx := context.Background()

	txn := seedStuckTxn(t, pg, "stripe", "order_1")
	if _, err := pg.DB.Pool().Exec(ctx, "UPDATE transactions SET actual_gateway='razorpay' WHERE id=$1", txn.ID); err != nil {
		t.Fatal(err)
	}
	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)

	got, err := repo.GetByGatewayReference(ctx, "razorpay", "order_1")
	if err != nil || got == nil || got.ID != txn.ID {
		t.Fatalf("after a fallback to razorpay, a razorpay webhook must find the payment, got %v err=%v", got, err)
	}
	if other, err := repo.GetByGatewayReference(ctx, "stripe", "order_1"); err != nil || other != nil {
		t.Fatalf("the routed-but-abandoned gateway must no longer match, got %v err=%v", other, err)
	}
	if missing, err := repo.FindByID(ctx, uuid.New()); err != nil || missing != nil {
		t.Fatalf("FindByID must return nil for a missing row, got %v err=%v", missing, err)
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
