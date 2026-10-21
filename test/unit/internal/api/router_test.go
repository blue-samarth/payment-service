package api_test

import . "samarth/payment-service/internal/api"

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/api/handlers"
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/app/payment"
	appwebhook "samarth/payment-service/internal/app/webhook"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type stubService struct{ txn *transaction.Transaction }

func (s stubService) CreatePayment(ctx context.Context, in payment.CreatePaymentInput) (payment.CreateResult, error) {
	return payment.CreateResult{Verdict: idempotency.Created, Transaction: s.txn}, nil
}
func (s stubService) ProcessPayment(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error) {
	return s.txn, nil
}
func (s stubService) GetPayment(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error) {
	return s.txn, nil
}

type okPinger struct{}

func (okPinger) Ping(ctx context.Context) error { return nil }

type noProcessor struct{}

func (noProcessor) Process(context.Context, string, appwebhook.Event, []byte) (appwebhook.Outcome, error) {
	return appwebhook.Outcome{}, nil
}

type unknownGateways struct{}

func (unknownGateways) WebhookParser(string) (ports.GatewayWebhookParser, bool) { return nil, false }

type noSecrets struct{}

func (noSecrets) WebhookSecret(context.Context, string) (string, error) { return "", nil }

type response struct {
	code      int
	body      string
	requestID string
}

func do(t *testing.T, srv *httptest.Server, method, path string) response {
	t.Helper()
	req, err := http.NewRequest(method, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return response{code: resp.StatusCode, body: string(body), requestID: resp.Header.Get("X-Request-ID")}
}

func testDeps(txn *transaction.Transaction) Deps {
	logger := observability.NewSlogLoggerFromHandler(slog.NewJSONHandler(io.Discard, nil))
	return Deps{
		Payment: handlers.NewPaymentHandler(stubService{txn: txn}),
		Webhook: handlers.NewWebhookHandler(noProcessor{}, unknownGateways{}, noSecrets{}, nil, logger),
		Health:  handlers.NewHealthHandler(okPinger{}, okPinger{}, nil, logger),
		Review:  handlers.NewReviewHandler(nil, logger),
		Logger:  logger,
	}
}

func TestMerchantRouter_ServesPaymentsOnly(t *testing.T) {
	txn, _ := transaction.New(uuid.New(), 1000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "", "", nil, 30)
	srv := httptest.NewServer(NewMerchantRouter(testDeps(txn)))
	defer srv.Close()

	got := do(t, srv, http.MethodGet, "/payments/"+txn.ID.String())
	if got.code != http.StatusOK {
		t.Errorf("GET /payments/{id}: expected 200, got %d", got.code)
	}
	if got.requestID == "" {
		t.Error("expected RequestID middleware to set X-Request-ID")
	}

	for _, path := range []string{"/health", "/health-overall", "/health-gateways"} {
		if got := do(t, srv, http.MethodGet, path); got.code != http.StatusNotFound {
			t.Errorf("GET %s must not be served on the merchant listener, got %d", path, got.code)
		}
	}
	if got := do(t, srv, http.MethodPost, "/webhooks/gateway/stripe"); got.code != http.StatusNotFound {
		t.Errorf("webhooks must not be served on the merchant listener, got %d", got.code)
	}
	if got := do(t, srv, http.MethodPost, "/payments/"+txn.ID.String()+"/resolve"); got.code != http.StatusForbidden {
		t.Errorf("the resolve route must exist on the merchant listener and fail closed without an ops principal, got %d", got.code)
	}
}

func TestResolveRouteOnlyOnMerchantListener(t *testing.T) {
	txn, _ := transaction.New(uuid.New(), 1000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "", "", nil, 30)
	for name, h := range map[string]http.Handler{
		"webhook": NewWebhookRouter(testDeps(txn)),
		"health":  NewHealthRouter(testDeps(txn)),
	} {
		srv := httptest.NewServer(h)
		if got := do(t, srv, http.MethodPost, "/payments/"+txn.ID.String()+"/resolve"); got.code != http.StatusNotFound && got.code != http.StatusMethodNotAllowed {
			t.Errorf("%s listener must not serve resolve, got %d", name, got.code)
		}
		srv.Close()
	}
}

func TestWebhookRouter_ServesWebhooksOnly(t *testing.T) {
	txn, _ := transaction.New(uuid.New(), 1000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "", "", nil, 30)
	srv := httptest.NewServer(NewWebhookRouter(testDeps(txn)))
	defer srv.Close()

	got := do(t, srv, http.MethodPost, "/webhooks/gateway/stripe")
	if got.code != http.StatusNotFound || !strings.Contains(got.body, "unknown_gateway") {
		t.Errorf("expected the webhook handler to answer (unknown_gateway), got %d %q", got.code, got.body)
	}
	if got.requestID == "" {
		t.Error("expected RequestID middleware to set X-Request-ID")
	}

	for _, path := range []string{"/payments/" + txn.ID.String(), "/health"} {
		if got := do(t, srv, http.MethodGet, path); got.code != http.StatusNotFound || strings.Contains(got.body, "unknown_gateway") {
			t.Errorf("GET %s must not be served on the webhook listener, got %d", path, got.code)
		}
	}
}

func TestHealthRouter_ServesHealthOnly(t *testing.T) {
	txn, _ := transaction.New(uuid.New(), 1000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "", "", nil, 30)
	deps := testDeps(txn)
	deps.Health.SetReady()
	srv := httptest.NewServer(NewHealthRouter(deps))
	defer srv.Close()

	for _, path := range []string{"/health", "/health-db", "/health-cache", "/health-overall", "/health-gateways"} {
		if got := do(t, srv, http.MethodGet, path); got.code != http.StatusOK {
			t.Errorf("GET %s: expected 200, got %d", path, got.code)
		}
	}
	if got := do(t, srv, http.MethodGet, "/payments/"+txn.ID.String()); got.code != http.StatusNotFound {
		t.Errorf("payments must not be served on the health listener, got %d", got.code)
	}
	if got := do(t, srv, http.MethodPost, "/webhooks/gateway/stripe"); got.code != http.StatusNotFound {
		t.Errorf("webhooks must not be served on the health listener, got %d", got.code)
	}
}
