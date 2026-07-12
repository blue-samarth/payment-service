//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/adapters/postgres"
	appwebhook "samarth/payment-service/internal/app/webhook"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/testsupport"
)

func seedProcessingTxn(t *testing.T, pg *testsupport.PG, gatewayID, reference string) *transaction.Transaction {
	t.Helper()
	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)

	txn, err := transaction.New(uuid.New(), 150000, "INR", transaction.PaymentMethodCard, gatewayID, uuid.New(), "b@e.com", "o", nil, 30)
	if err != nil {
		t.Fatal(err)
	}
	txn.Status = transaction.StatusProcessing
	txn.GatewayReferenceID = reference
	if err := tr.WithinTx(context.Background(), func(ctx context.Context) error { return repo.Insert(ctx, txn) }); err != nil {
		t.Fatalf("seed processing txn: %v", err)
	}
	return txn
}

func TestWebhook_ResolvesProcessingTransaction(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "webhook_events", "transaction_raw_metadata", "outbox_events")
	ctx := context.Background()

	txn := seedProcessingTxn(t, pg, "razorpay", "order_int")

	svc := appwebhook.NewService(
		postgres.NewTransactionRepository(pg.DB, pg.Q),
		postgres.NewWebhookRepository(pg.DB, pg.Q),
		postgres.NewOutboxWriter(pg.DB, pg.Q),
		postgres.NewTransactor(pg.DB),
		discardLogger(),
		observability.NewNoopMetrics(),
	)

	out, err := svc.Process(ctx, "razorpay", appwebhook.Event{
		EventID: "evt_int_1", GatewayReferenceID: "order_int", Status: "success",
	}, []byte(`{"raw":"payload"}`))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if !out.Resolved || out.Status != transaction.StatusSucceeded {
		t.Fatalf("expected resolved to SUCCEEDED, got %+v", out)
	}

	persisted, _ := postgres.NewTransactionRepository(pg.DB, pg.Q).GetByID(ctx, txn.ID)
	if persisted.Status != transaction.StatusSucceeded {
		t.Errorf("transaction should be SUCCEEDED in DB, got %s", persisted.Status)
	}

	var rawCount, eventCount int
	_ = pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM transaction_raw_metadata WHERE transaction_id=$1", txn.ID).Scan(&rawCount)
	_ = pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_id=$1", txn.ID).Scan(&eventCount)
	if rawCount != 1 {
		t.Errorf("expected 1 raw metadata row, got %d", rawCount)
	}
	if eventCount != 1 {
		t.Errorf("expected 1 outbox event, got %d", eventCount)
	}

	// Replaying the same event must be an idempotent no-op.
	again, err := svc.Process(ctx, "razorpay", appwebhook.Event{
		EventID: "evt_int_1", GatewayReferenceID: "order_int", Status: "success",
	}, []byte(`{"raw":"payload"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate {
		t.Errorf("replayed webhook should be a duplicate no-op, got %+v", again)
	}
	_ = pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM transaction_raw_metadata WHERE transaction_id=$1", txn.ID).Scan(&rawCount)
	if rawCount != 1 {
		t.Errorf("duplicate webhook must not insert a second raw metadata row, got %d", rawCount)
	}
}
