package stripe

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/ports"
)

func newTestAdapter(handler http.HandlerFunc) (*Adapter, *httptest.Server) {
	srv := httptest.NewServer(handler)
	a := New(Config{APIKey: "sk_test_x", BaseURL: srv.URL, HTTPClient: srv.Client()})
	return a, srv
}

func TestInitiatePayment_Success(t *testing.T) {
	var gotPath, gotMethod, gotIdem, gotAuth string
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		gotIdem = r.Header.Get("Idempotency-Key")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"pi_123","status":"succeeded","amount":150000,"currency":"usd","metadata":{"transaction_id":"abc"}}`))
	})
	defer srv.Close()

	resp, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
		TransactionID: uuid.New(),
		Amount:        150000,
		Currency:      "USD",
		AttemptNumber: 1,
		Description:   "order",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GatewayReferenceID != "pi_123" {
		t.Errorf("expected reference pi_123, got %s", resp.GatewayReferenceID)
	}
	if resp.Status != ports.GatewayPaymentStatusSucceeded {
		t.Errorf("expected SUCCEEDED, got %s", resp.Status)
	}
	if resp.Currency != "USD" {
		t.Errorf("expected currency uppercased to USD, got %s", resp.Currency)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/payment_intents" {
		t.Errorf("expected POST /v1/payment_intents, got %s %s", gotMethod, gotPath)
	}
	if gotIdem == "" {
		t.Error("expected Idempotency-Key header to be set")
	}
	if gotAuth == "" {
		t.Error("expected Authorization header to be set")
	}
}

func TestInitiatePayment_UsesProvidedIdempotencyKey(t *testing.T) {
	var gotIdem string
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		gotIdem = r.Header.Get("Idempotency-Key")
		_, _ = w.Write([]byte(`{"id":"pi_1","status":"processing","amount":1,"currency":"usd"}`))
	})
	defer srv.Close()

	_, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
		TransactionID:  uuid.New(),
		Amount:         1,
		Currency:       "USD",
		IdempotencyKey: "stored-key-123",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotIdem != "stored-key-123" {
		t.Errorf("expected stored idempotency key to be used, got %q", gotIdem)
	}
}

func TestInitiatePayment_CardDeclined(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"error":{"type":"card_error","code":"card_declined","decline_code":"generic_decline","message":"Your card was declined."}}`))
	})
	defer srv.Close()

	_, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{TransactionID: uuid.New(), Amount: 1, Currency: "USD"})
	var gwErr *ports.GatewayError
	if !errors.As(err, &gwErr) {
		t.Fatalf("expected *ports.GatewayError, got %v", err)
	}
	if gwErr.Category != ports.ErrorCategoryHardDecline {
		t.Errorf("expected HardDecline, got %s", gwErr.Category)
	}
	if gwErr.Retryable {
		t.Error("hard decline should not be retryable")
	}
}

func TestInitiatePayment_InsufficientFundsIsSoftDecline(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"error":{"type":"card_error","code":"card_declined","decline_code":"insufficient_funds","message":"Insufficient funds."}}`))
	})
	defer srv.Close()

	_, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{TransactionID: uuid.New(), Amount: 1, Currency: "USD"})
	var gwErr *ports.GatewayError
	if !errors.As(err, &gwErr) {
		t.Fatalf("expected *ports.GatewayError, got %v", err)
	}
	if gwErr.Category != ports.ErrorCategorySoftDecline {
		t.Errorf("expected SoftDecline, got %s", gwErr.Category)
	}
	if !gwErr.Retryable {
		t.Error("soft decline should be retryable")
	}
}

func TestCheckStatus(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/payment_intents/pi_check" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"id":"pi_check","status":"processing","amount":5000,"currency":"inr"}`))
	})
	defer srv.Close()

	resp, err := a.CheckStatus(context.Background(), ports.GatewayStatusRequest{GatewayReferenceID: "pi_check"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayPaymentStatusProcessing {
		t.Errorf("expected PROCESSING, got %s", resp.Status)
	}
}

func TestRefund(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/refunds" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"id":"re_1","status":"succeeded","amount":5000,"currency":"usd"}`))
	})
	defer srv.Close()

	resp, err := a.Refund(context.Background(), ports.GatewayRefundRequest{
		RefundID:           uuid.New(),
		GatewayReferenceID: "pi_1",
		Amount:             5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayRefundStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", resp.Status)
	}
}

func TestCancel(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/payment_intents/pi_c/cancel" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"id":"pi_c","status":"canceled","amount":1,"currency":"usd"}`))
	})
	defer srv.Close()

	resp, err := a.Cancel(context.Background(), ports.GatewayCancelRequest{GatewayReferenceID: "pi_c"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayCancelStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", resp.Status)
	}
}

func TestClassifyError(t *testing.T) {
	cases := []struct {
		errType, code, decline string
		want                   ports.ErrorCategory
	}{
		{"card_error", "card_declined", "generic_decline", ports.ErrorCategoryHardDecline},
		{"card_error", "card_declined", "insufficient_funds", ports.ErrorCategorySoftDecline},
		{"rate_limit_error", "", "", ports.ErrorCategoryNetworkTimeout},
		{"api_connection_error", "", "", ports.ErrorCategoryNetworkTimeout},
		{"idempotency_error", "", "", ports.ErrorCategoryIdempotencyConflict},
		{"api_error", "", "", ports.ErrorCategoryGatewayError},
		{"something_new", "", "", ports.ErrorCategoryGatewayError},
	}
	for _, c := range cases {
		if got := classifyError(c.errType, c.code, c.decline); got != c.want {
			t.Errorf("classifyError(%q,%q,%q) = %s, want %s", c.errType, c.code, c.decline, got, c.want)
		}
	}
}

func TestDeriveIdempotencyKey_Deterministic(t *testing.T) {
	id := uuid.New()
	k1 := deriveIdempotencyKey(id, 1)
	k2 := deriveIdempotencyKey(id, 1)
	k3 := deriveIdempotencyKey(id, 2)
	if k1 != k2 {
		t.Error("expected deterministic key for same inputs")
	}
	if k1 == k3 {
		t.Error("expected different key for different attempt number")
	}
	if len(k1) > 255 {
		t.Errorf("key exceeds Stripe 255-char limit: %d", len(k1))
	}
}

func TestCapabilities(t *testing.T) {
	caps := New(Config{}).Capabilities()
	if !caps.SupportsCancel || !caps.SupportsPartialRefund || !caps.IdempotencyCapable {
		t.Error("expected Stripe to support cancel, partial refund, and idempotency")
	}
}
