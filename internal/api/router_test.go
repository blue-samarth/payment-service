package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"io"
	"log/slog"
	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/api/handlers"
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/domain/transaction"
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

func TestRouter_RoutesAndSetsRequestID(t *testing.T) {
	txn, _ := transaction.New(uuid.New(), 1000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "", "", nil, 30)
	logger := observability.NewSlogLoggerFromHandler(slog.NewJSONHandler(io.Discard, nil))

	router := NewRouter(Deps{
		Payment: handlers.NewPaymentHandler(stubService{txn: txn}),
		Health:  handlers.NewHealthHandler(okPinger{}),
		Logger:  logger,
	})

	srv := httptest.NewServer(router)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected /health 200, got %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Request-ID") == "" {
		t.Error("expected RequestID middleware to set X-Request-ID header")
	}

	resp2, err := http.Get(srv.URL + "/payments/" + txn.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected GET /payments/{id} 200, got %d", resp2.StatusCode)
	}
}
