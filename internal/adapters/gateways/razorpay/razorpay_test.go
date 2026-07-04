package razorpay

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
	return New(Config{KeyID: "rzp_test", KeySecret: "secret", BaseURL: srv.URL, HTTPClient: srv.Client()}), srv
}

func TestInitiatePayment_CreatesOrder(t *testing.T) {
	var gotPath, gotAuth string
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"id":"order_1","status":"created","amount":150000,"currency":"INR"}`))
	})
	defer srv.Close()

	resp, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
		TransactionID: uuid.New(), Amount: 150000, Currency: "INR", AttemptNumber: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GatewayReferenceID != "order_1" || resp.Status != ports.GatewayPaymentStatusPending {
		t.Errorf("unexpected response: %+v", resp)
	}
	if gotPath != "/v1/orders" {
		t.Errorf("expected POST /v1/orders, got %s", gotPath)
	}
	if gotAuth == "" {
		t.Error("expected basic auth header")
	}
}

func TestCheckStatus_PaidIsSucceeded(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"order_2","status":"paid","amount":150000,"currency":"INR"}`))
	})
	defer srv.Close()

	resp, err := a.CheckStatus(context.Background(), ports.GatewayStatusRequest{GatewayReferenceID: "order_2"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayPaymentStatusSucceeded {
		t.Errorf("expected SUCCEEDED, got %s", resp.Status)
	}
}

func TestRefund_Processed(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"rfnd_1","status":"processed","amount":150000,"currency":"INR"}`))
	})
	defer srv.Close()

	resp, err := a.Refund(context.Background(), ports.GatewayRefundRequest{GatewayReferenceID: "pay_1", Amount: 150000})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayRefundStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", resp.Status)
	}
}

func TestCancel_NotSupported(t *testing.T) {
	a := New(Config{})
	resp, _ := a.Cancel(context.Background(), ports.GatewayCancelRequest{})
	if resp.Status != ports.GatewayCancelStatusNotSupported {
		t.Errorf("razorpay should report cancel NOT_SUPPORTED, got %s", resp.Status)
	}
}

func TestError_Classification(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":"BAD_REQUEST_ERROR","reason":"payment_failed","description":"declined"}}`))
	})
	defer srv.Close()

	_, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{TransactionID: uuid.New(), Amount: 1, Currency: "INR"})
	var gwErr *ports.GatewayError
	if !errors.As(err, &gwErr) {
		t.Fatalf("expected *ports.GatewayError, got %v", err)
	}
	if gwErr.Category != ports.ErrorCategoryHardDecline {
		t.Errorf("expected HardDecline for payment_failed, got %s", gwErr.Category)
	}
}

func TestClassifyError_Table(t *testing.T) {
	cases := []struct {
		code, reason string
		want         ports.ErrorCategory
	}{
		{"GATEWAY_ERROR", "", ports.ErrorCategoryGatewayError},
		{"SERVER_ERROR", "", ports.ErrorCategoryNetworkTimeout},
		{"BAD_REQUEST_ERROR", "payment_failed", ports.ErrorCategoryHardDecline},
		{"BAD_REQUEST_ERROR", "insufficient_funds", ports.ErrorCategorySoftDecline},
		{"BAD_REQUEST_ERROR", "validation", ports.ErrorCategoryGatewayError},
		{"WHATEVER", "", ports.ErrorCategoryGatewayError},
	}
	for _, c := range cases {
		if got := classifyError(c.code, c.reason); got != c.want {
			t.Errorf("classifyError(%q,%q)=%s want %s", c.code, c.reason, got, c.want)
		}
	}
}

func TestDeriveReceipt_StableAnd40Chars(t *testing.T) {
	id := uuid.New()
	r1 := deriveReceipt(id, 1)
	r2 := deriveReceipt(id, 1)
	r3 := deriveReceipt(id, 2)
	if r1 != r2 {
		t.Error("receipt must be stable for same inputs")
	}
	if r1 == r3 {
		t.Error("receipt must differ by attempt")
	}
	if len(r1) != 40 {
		t.Errorf("receipt must be exactly 40 chars (Razorpay limit), got %d", len(r1))
	}
}
