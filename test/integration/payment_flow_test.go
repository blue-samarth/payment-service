//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/adapters/gateways"
	"samarth/payment-service/internal/adapters/gateways/stripe"
	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/adapters/postgres"
	"samarth/payment-service/internal/app/payment"
	approuting "samarth/payment-service/internal/app/routing"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
	"samarth/payment-service/internal/testsupport"
)

func buildService(pg *testsupport.PG, stripeURL string) *payment.Service {
	cfg := postgres.NewConfigStore(pg.DB, pg.Q)
	registry := gateways.NewRegistry()
	registry.Register("stripe", stripe.New(stripe.Config{APIKey: "sk_test", BaseURL: stripeURL}))
	logger := observability.NewSlogLoggerFromHandler(slog.NewJSONHandler(io.Discard, nil))

	return payment.NewService(
		postgres.NewTransactionRepository(pg.DB, pg.Q),
		postgres.NewOutboxWriter(pg.DB, pg.Q),
		approuting.NewRouter(cfg),
		cfg,
		postgres.NewTransactor(pg.DB),
		postgres.NewLeaseRepository(pg.DB, pg.Q),
		registry,
		logger,
		observability.NewNoopMetrics(),
	)
}

func cardInput() payment.CreatePaymentInput {
	return payment.CreatePaymentInput{
		MerchantID:    uuid.New(),
		Amount:        150000,
		Currency:      "INR",
		PaymentMethod: transaction.PaymentMethodCard,
		CustomerEmail: "buyer@example.com",
		Description:   "integration order",
		MerchantTier:  "standard",
		IsDomestic:    true,
	}
}

func outboxEventTypes(t *testing.T, pg *testsupport.PG) []string {
	t.Helper()
	events, err := postgres.NewOutboxWriter(pg.DB, pg.Q).PollPending(context.Background(), 0, 63, 10)
	if err != nil {
		t.Fatalf("poll outbox: %v", err)
	}
	types := make([]string, 0, len(events))
	for _, e := range events {
		types = append(types, e.EventType)
	}
	return types
}

func contains(types []string, want string) bool {
	for _, ty := range types {
		if ty == want {
			return true
		}
	}
	return false
}

func TestPaymentFlow_SuccessEndToEnd(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"pi_flow_ok","status":"succeeded","amount":150000,"currency":"inr"}`))
	}))
	defer srv.Close()

	svc := buildService(pg, srv.URL)
	ctx := context.Background()

	createdRes, err := svc.CreatePayment(ctx, cardInput())
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	created := createdRes.Transaction
	if created.Status != transaction.StatusPending {
		t.Errorf("expected PENDING after create, got %s", created.Status)
	}
	if created.GatewayID != "stripe" {
		t.Errorf("expected routing to select stripe, got %q", created.GatewayID)
	}
	if created.EstimatedTimeoutSeconds != 30 {
		t.Errorf("expected processing timeout 30s from config, got %d", created.EstimatedTimeoutSeconds)
	}

	processed, err := svc.ProcessPayment(ctx, created.ID)
	if err != nil {
		t.Fatalf("ProcessPayment: %v", err)
	}
	if processed.Status != transaction.StatusSucceeded {
		t.Errorf("expected SUCCEEDED, got %s", processed.Status)
	}
	if processed.GatewayReferenceID != "pi_flow_ok" || processed.ActualGateway != "stripe" {
		t.Errorf("expected gateway ref pi_flow_ok / actual stripe, got %q / %q", processed.GatewayReferenceID, processed.ActualGateway)
	}

	persisted, err := postgres.NewTransactionRepository(pg.DB, pg.Q).GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if persisted.Status != transaction.StatusSucceeded {
		t.Errorf("persisted status should be SUCCEEDED, got %s", persisted.Status)
	}

	types := outboxEventTypes(t, pg)
	if len(types) != 2 || !contains(types, ports.EventTypePaymentCreated) || !contains(types, ports.EventTypePaymentSucceeded) {
		t.Errorf("expected PAYMENT_CREATED + PAYMENT_SUCCEEDED in outbox, got %v", types)
	}
}

func TestPaymentFlow_GatewayDecline(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"error":{"type":"card_error","code":"card_declined","decline_code":"generic_decline","message":"declined"}}`))
	}))
	defer srv.Close()

	svc := buildService(pg, srv.URL)
	ctx := context.Background()

	createdRes, err := svc.CreatePayment(ctx, cardInput())
	if err != nil {
		t.Fatalf("CreatePayment: %v", err)
	}
	created := createdRes.Transaction

	processed, err := svc.ProcessPayment(ctx, created.ID)
	if err != nil {
		t.Fatalf("ProcessPayment: %v", err)
	}
	if processed.Status != transaction.StatusFailed {
		t.Errorf("expected FAILED on decline, got %s", processed.Status)
	}
	if processed.FailureReason == nil {
		t.Error("expected failure reason recorded")
	}

	types := outboxEventTypes(t, pg)
	if !contains(types, ports.EventTypePaymentFailed) {
		t.Errorf("expected PAYMENT_FAILED in outbox, got %v", types)
	}
}

func TestPaymentFlow_NoEligibleGateway(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)

	svc := buildService(pg, "http://unused")
	in := cardInput()
	in.PaymentMethod = transaction.PaymentMethodUPI // no gateway supports UPI

	_, err := svc.CreatePayment(context.Background(), in)
	if err != payment.ErrNoGateway {
		t.Fatalf("expected ErrNoGateway for unsupported method, got %v", err)
	}
}
