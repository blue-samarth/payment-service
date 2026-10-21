//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/adapters/gateways"
	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/adapters/postgres"
	"samarth/payment-service/internal/api"
	"samarth/payment-service/internal/api/handlers"
	"samarth/payment-service/internal/api/middleware"
	appcancel "samarth/payment-service/internal/app/cancel"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/testsupport"
)

const b12 = "B12 (Docs/BACKLOG.md): handlers do not yet compare the caller's merchant with the request or the transaction; remove this skip with the fix, or set RUN_B12=1 to run it now"

func skipUntilB12(t *testing.T) {
	t.Helper()
	if os.Getenv("RUN_B12") == "" {
		t.Skip(b12)
	}
}

type isolation struct {
	pg        *testsupport.PG
	h         http.Handler
	merchantA uuid.UUID
	merchantB uuid.UUID
	ownTxn    *transaction.Transaction
	otherTxn  *transaction.Transaction
}

var (
	tokenMerchantA = "svc-a-" + strings.Repeat("a", 40)
	tokenOps       = "ops-" + strings.Repeat("o", 40)
)

func seedFor(t *testing.T, pg *testsupport.PG, merchant uuid.UUID, status transaction.Status) *transaction.Transaction {
	t.Helper()
	txn, err := transaction.New(merchant, 150000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@e.com", "o", nil, 30)
	if err != nil {
		t.Fatal(err)
	}
	txn.Status = status
	repo := postgres.NewTransactionRepository(pg.DB, pg.Q)
	if err := postgres.NewTransactor(pg.DB).WithinTx(context.Background(), func(ctx context.Context) error { return repo.Insert(ctx, txn) }); err != nil {
		t.Fatalf("seed transaction: %v", err)
	}
	return txn
}

func newIsolation(t *testing.T) *isolation {
	t.Helper()
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "refunds", "processing_lease", "outbox_events", "idempotency_keys")
	testsupport.SeedStripeCardGateway(t, pg)

	stripeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"pi_1","status":"succeeded","amount":150000,"currency":"inr"}`))
	}))
	t.Cleanup(stripeSrv.Close)

	s := &isolation{pg: pg, merchantA: uuid.New(), merchantB: uuid.New()}
	s.ownTxn = seedFor(t, pg, s.merchantA, transaction.StatusProcessing)
	s.otherTxn = seedFor(t, pg, s.merchantB, transaction.StatusSucceeded)

	provider := middleware.NewStaticTokenProvider(map[string]string{tokenMerchantA: s.merchantA.String()}, []string{tokenOps})
	s.h = api.NewMerchantRouter(api.Deps{
		Payment: handlers.NewPaymentHandler(buildService(pg, stripeSrv.URL)),
		Refund:  handlers.NewRefundHandler(refundService(pg, gateways.NewRegistry())),
		Cancel:  handlers.NewCancelHandler(appcancel.NewService(postgres.NewTransactionRepository(pg.DB, pg.Q), discardLogger(), observability.NewNoopMetrics())),
		Logger:  discardLogger(),
		Auth:    provider,
	})
	return s
}

func (s *isolation) call(method, path, tokenHeader, token, body string) int {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set(tokenHeader, token)
	req.Header.Set("Idempotency-Key", uuid.NewString())
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.h.ServeHTTP(rec, req)
	return rec.Code
}

func (s *isolation) asMerchantA(method, path, body string) int {
	return s.call(method, path, "X-Service-Token", tokenMerchantA, body)
}

func (s *isolation) reload(t *testing.T, id uuid.UUID) *transaction.Transaction {
	t.Helper()
	txn, err := postgres.NewTransactionRepository(s.pg.DB, s.pg.Q).GetByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return txn
}

func TestMerchantIsolation_MerchantCanReadItsOwnTransaction(t *testing.T) {
	s := newIsolation(t)
	if code := s.asMerchantA(http.MethodGet, "/payments/"+s.ownTxn.ID.String(), ""); code != http.StatusOK {
		t.Fatalf("got %d", code)
	}
}

func TestMerchantIsolation_OpsTokenActsAcrossMerchants(t *testing.T) {
	s := newIsolation(t)
	if code := s.call(http.MethodGet, "/payments/"+s.otherTxn.ID.String(), "X-Ops-Token", tokenOps, ""); code != http.StatusOK {
		t.Fatalf("ops tokens act across merchants by design, got %d", code)
	}
}

func TestMerchantIsolation_CannotReadAnotherMerchantsTransaction(t *testing.T) {
	skipUntilB12(t)
	s := newIsolation(t)
	if code := s.asMerchantA(http.MethodGet, "/payments/"+s.otherTxn.ID.String(), ""); code != http.StatusNotFound {
		t.Fatalf("merchant A must get 404 on merchant B's transaction, got %d", code)
	}
}

func TestMerchantIsolation_CannotRefundAnotherMerchantsTransaction(t *testing.T) {
	skipUntilB12(t)
	s := newIsolation(t)
	if code := s.asMerchantA(http.MethodPost, "/payments/"+s.otherTxn.ID.String()+"/refunds", `{"amount":1000,"reason":"x"}`); code != http.StatusNotFound {
		t.Fatalf("merchant A must get 404 refunding merchant B's transaction, got %d", code)
	}
	var refunds int
	_ = s.pg.DB.Pool().QueryRow(context.Background(), "SELECT count(*) FROM refunds WHERE transaction_id=$1", s.otherTxn.ID).Scan(&refunds)
	if refunds != 0 {
		t.Fatalf("no refund may be created, got %d", refunds)
	}
}

func TestMerchantIsolation_CannotCancelAnotherMerchantsTransaction(t *testing.T) {
	skipUntilB12(t)
	s := newIsolation(t)
	other := seedFor(t, s.pg, s.merchantB, transaction.StatusProcessing)
	if code := s.asMerchantA(http.MethodPost, "/payments/"+other.ID.String()+"/cancel", `{}`); code != http.StatusNotFound {
		t.Fatalf("merchant A must get 404 cancelling merchant B's transaction, got %d", code)
	}
	if s.reload(t, other.ID).CancelIntent {
		t.Fatal("no cancel intent may be recorded on another merchant's transaction")
	}
}

func TestMerchantIsolation_CannotCreateAPaymentForAnotherMerchant(t *testing.T) {
	skipUntilB12(t)
	s := newIsolation(t)
	body := `{"merchant_id":"` + s.merchantB.String() + `","amount":1000,"currency":"INR","payment_method":"card"}`
	if code := s.asMerchantA(http.MethodPost, "/payments", body); code < 400 {
		t.Fatalf("merchant A must not create a payment as merchant B, got %d", code)
	}
	var created int
	_ = s.pg.DB.Pool().QueryRow(context.Background(), "SELECT count(*) FROM transactions WHERE merchant_id=$1", s.merchantB).Scan(&created)
	if created != 1 {
		t.Fatalf("merchant B must still have only its seeded transaction, got %d", created)
	}
}

func TestMerchantIsolation_CancelActorComesFromThePrincipal(t *testing.T) {
	skipUntilB12(t)
	s := newIsolation(t)
	if code := s.asMerchantA(http.MethodPost, "/payments/"+s.ownTxn.ID.String()+"/cancel", `{"actor":"ops","via":"ops-tool"}`); code >= 500 {
		t.Fatalf("got %d", code)
	}
	if got := s.reload(t, s.ownTxn.ID).CancelRequestedBy; got == transaction.ActorOps {
		t.Fatal("a merchant token must not be able to record a cancel as ops; the actor comes from the principal")
	}

	fresh := seedFor(t, s.pg, s.merchantA, transaction.StatusProcessing)
	if code := s.asMerchantA(http.MethodPost, "/payments/"+fresh.ID.String()+"/cancel", `{"actor":"anything-at-all"}`); code != http.StatusBadRequest {
		t.Fatalf("an unknown actor value must be rejected, not written to the audit trail, got %d", code)
	}
}
