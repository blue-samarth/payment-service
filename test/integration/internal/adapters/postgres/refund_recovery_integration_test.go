//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"samarth/payment-service/internal/adapters/postgres"
	"samarth/payment-service/internal/domain/refund"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/testsupport"
)

func TestRefundRecovery_ListsOnlyStaleUnreviewedOpenRefunds(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "refunds", "transactions")
	ctx := context.Background()

	txns := postgres.NewTransactionRepository(pg.DB, pg.Q)
	refunds := postgres.NewRefundRepository(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)

	parent := newTxn(t)
	parent.Status = transaction.StatusSucceeded
	if err := tr.WithinTx(ctx, func(ctx context.Context) error { return txns.Insert(ctx, parent) }); err != nil {
		t.Fatal(err)
	}

	insert := func(status refund.Status, age time.Duration) *refund.Refund {
		t.Helper()
		rf, err := refund.New(parent.ID, 100, parent.Amount, 0, "r", "merchant")
		if err != nil {
			t.Fatal(err)
		}
		rf.Status = status
		rf.InitiatedAt = time.Now().UTC().Add(-age)
		if err := tr.WithinTx(ctx, func(ctx context.Context) error { return refunds.Insert(ctx, rf) }); err != nil {
			t.Fatal(err)
		}
		return rf
	}

	staleInitiated := insert(refund.StatusInitiated, time.Hour)
	staleProcessing := insert(refund.StatusProcessing, 2*time.Hour)
	fresh := insert(refund.StatusProcessing, time.Minute)
	done := insert(refund.StatusRefunded, 3*time.Hour)
	reviewed := insert(refund.StatusProcessing, 4*time.Hour)

	marked, err := refunds.MarkReviewRequired(ctx, reviewed.ID, "not_found_at_gateway")
	if err != nil || !marked {
		t.Fatalf("expected the first mark to apply, got %v %v", marked, err)
	}
	if again, _ := refunds.MarkReviewRequired(ctx, reviewed.ID, "status_unknown"); again {
		t.Fatal("a refund already under review must not be marked twice")
	}
	if onDone, _ := refunds.MarkReviewRequired(ctx, done.ID, "x"); onDone {
		t.Fatal("a finished refund must not be marked for review")
	}

	ids, err := refunds.ListRecoverableRefundIDs(ctx, 5*time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != staleProcessing.ID || ids[1] != staleInitiated.ID {
		t.Fatalf("expected the two stale open refunds, oldest first, got %v (fresh=%s done=%s reviewed=%s)", ids, fresh.ID, done.ID, reviewed.ID)
	}

	if limited, _ := refunds.ListRecoverableRefundIDs(ctx, 5*time.Minute, 1); len(limited) != 1 {
		t.Fatalf("the batch size must cap the listing, got %d", len(limited))
	}

	pending, err := refunds.CountPendingReview(ctx)
	if err != nil || pending != 1 {
		t.Fatalf("expected one refund pending review, got %d %v", pending, err)
	}
}
