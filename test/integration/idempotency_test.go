//go:build integration

package integration

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"samarth/payment-service/internal/adapters/postgres"
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/testsupport"
)

// idempotentService wires the service-owned idempotency guard and NO HTTP cache
// middleware — this is the "cache disabled" configuration, proving the service
// is the sole authority for every idempotency outcome.
func idempotentService(pg *testsupport.PG) *payment.Service {
	svc := buildService(pg, "http://unused")
	svc.SetIdempotency(idempotency.NewGuard(
		postgres.NewIdempotencyRepository(pg.DB, pg.Q),
		postgres.NewTransactor(pg.DB),
	))
	return svc
}

func TestServiceIdempotency_ConcurrentSingleExecution(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events", "idempotency_keys")
	testsupport.SeedStripeCardGateway(t, pg)
	ctx := context.Background()

	svc := idempotentService(pg)
	in := cardInput()
	in.IdempotencyKey = "concurrent-key"

	const goroutines = 16
	var created, replayed, inProgress int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := svc.CreatePayment(ctx, in)
			if err != nil {
				t.Errorf("CreatePayment: %v", err)
				return
			}
			switch res.Verdict {
			case idempotency.Created:
				atomic.AddInt64(&created, 1)
			case idempotency.Replayed:
				atomic.AddInt64(&replayed, 1)
			case idempotency.InProgress:
				atomic.AddInt64(&inProgress, 1)
			default:
				t.Errorf("unexpected verdict %v", res.Verdict)
			}
		}()
	}
	wg.Wait()

	if created != 1 {
		t.Errorf("exactly one request must create the payment, got %d", created)
	}
	if created+replayed+inProgress != goroutines {
		t.Errorf("every request must resolve to created/replayed/in-progress, got %d/%d/%d", created, replayed, inProgress)
	}

	var count int
	if err := pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM transactions").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("a shared idempotency key must create exactly one transaction, got %d", count)
	}
}

func TestServiceIdempotency_SequentialReplayReturnsSameTransaction(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events", "idempotency_keys")
	testsupport.SeedStripeCardGateway(t, pg)
	ctx := context.Background()

	svc := idempotentService(pg)
	in := cardInput()
	in.IdempotencyKey = "seq-key"

	first, err := svc.CreatePayment(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if first.Verdict != idempotency.Created {
		t.Fatalf("first call should be Created, got %v", first.Verdict)
	}

	second, err := svc.CreatePayment(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if second.Verdict != idempotency.Replayed {
		t.Fatalf("second call should replay, got %v", second.Verdict)
	}
	if second.Transaction.ID != first.Transaction.ID {
		t.Errorf("replay must return the same transaction, got %s vs %s", second.Transaction.ID, first.Transaction.ID)
	}

	var count int
	if err := pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM transactions").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("replay must not insert a second transaction, got %d", count)
	}
}

func TestServiceIdempotency_KeyReusedWithDifferentBody(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events", "idempotency_keys")
	testsupport.SeedStripeCardGateway(t, pg)
	ctx := context.Background()

	svc := idempotentService(pg)
	in := cardInput()
	in.IdempotencyKey = "reuse-key"

	if _, err := svc.CreatePayment(ctx, in); err != nil {
		t.Fatal(err)
	}

	reused := in
	reused.Amount = in.Amount + 1 // same key, different canonical body
	res, err := svc.CreatePayment(ctx, reused)
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != idempotency.KeyReused {
		t.Errorf("reusing a key with a different request must be KeyReused, got %v", res.Verdict)
	}
}
