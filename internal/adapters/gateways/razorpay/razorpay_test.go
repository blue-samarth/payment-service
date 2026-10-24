package razorpay

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/transaction"
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

func TestInitiatePayment_ReturnsCheckoutParameters(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"order_5","status":"created","amount":150000,"currency":"INR"}`))
	})
	defer srv.Close()

	resp, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
		TransactionID: uuid.New(), Amount: 150000, Currency: "INR", AttemptNumber: 1, CustomerEmail: "b@e.com", Description: "order 42",
	})
	if err != nil {
		t.Fatal(err)
	}
	na := resp.NextAction
	if na == nil || na.Type != transaction.NextActionRazorpayCheckout {
		t.Fatalf("expected Checkout parameters, got %+v", na)
	}
	want := map[string]string{"key_id": "rzp_test", "order_id": "order_5", "amount": "150000", "currency": "INR", "prefill_email": "b@e.com", "description": "order 42"}
	for k, v := range want {
		if na.Params[k] != v {
			t.Errorf("param %s = %q, want %q", k, na.Params[k], v)
		}
	}
	if _, leaked := na.Params["key_secret"]; leaked {
		t.Fatal("the key secret must never reach the browser")
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

func TestRefund_OrderReferenceRefundsItsCapturedPayment(t *testing.T) {
	var refundPath string
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/orders/order_7/payments":
			_, _ = w.Write([]byte(`{"items":[{"id":"pay_declined","status":"failed"},{"id":"pay_ok","status":"captured","amount":150000}]}`))
		case r.Method == http.MethodPost:
			refundPath = r.URL.Path
			_, _ = w.Write([]byte(`{"id":"rfnd_7","status":"processed","amount":150000,"currency":"INR"}`))
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
	})
	defer srv.Close()

	resp, err := a.Refund(context.Background(), ports.GatewayRefundRequest{GatewayReferenceID: "order_7", Amount: 150000})
	if err != nil {
		t.Fatal(err)
	}
	if refundPath != "/v1/payments/pay_ok/refund" {
		t.Fatalf("the refund must go to the captured pay_ ID, not the order, got %s", refundPath)
	}
	if resp.GatewayRefundID != "rfnd_7" {
		t.Errorf("got %+v", resp)
	}
}

func TestRefund_OrderWithoutExactlyOneCapturedPaymentIsNotRefunded(t *testing.T) {
	for name, tc := range map[string]struct {
		items string
		cat   ports.ErrorCategory
		code  string
	}{
		"none captured":    {`[{"id":"pay_1","status":"failed"}]`, ports.ErrorCategoryHardDecline, "no_captured_payment"},
		"several captured": {`[{"id":"pay_1","status":"captured"},{"id":"pay_2","status":"captured"}]`, ports.ErrorCategoryAmbiguous, "multiple_captured_payments"},
	} {
		refunded := false
		a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				refunded = true
			}
			_, _ = w.Write([]byte(`{"items":` + tc.items + `}`))
		})

		_, err := a.Refund(context.Background(), ports.GatewayRefundRequest{GatewayReferenceID: "order_8", Amount: 100})
		srv.Close()
		var gwErr *ports.GatewayError
		if !errors.As(err, &gwErr) || gwErr.Category != tc.cat || gwErr.Code != tc.code {
			t.Errorf("%s: expected %s/%s, got %v", name, tc.cat, tc.code, err)
		}
		if refunded {
			t.Errorf("%s: no refund may be sent when the payment to refund is not unambiguous", name)
		}
	}
}

func TestRefund_LookupFailureIsTerminalAndPostsNothing(t *testing.T) {
	for name, status := range map[string]int{"4xx": http.StatusBadRequest, "5xx": http.StatusInternalServerError, "no connection": 0} {
		posted := false
		a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				posted = true
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"code":"BAD_REQUEST_ERROR","description":"The id provided does not exist"}}`))
		})
		if status == 0 {
			srv.Close()
		}

		_, err := a.Refund(context.Background(), ports.GatewayRefundRequest{GatewayReferenceID: "order_9", Amount: 100})
		srv.Close()
		var gwErr *ports.GatewayError
		if !errors.As(err, &gwErr) || gwErr.Category != ports.ErrorCategoryHardDecline || gwErr.Code != "refund_lookup_failed" {
			t.Errorf("%s: nothing was sent, so the refund must end terminally, got %v", name, err)
		}
		if posted {
			t.Errorf("%s: no refund may be posted when the lookup fails", name)
		}
	}
}

func TestRefund_AmountAboveCapturedIsRefused(t *testing.T) {
	posted := false
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posted = true
		}
		_, _ = w.Write([]byte(`{"items":[{"id":"pay_1","status":"captured","amount":1000}]}`))
	})
	defer srv.Close()

	_, err := a.Refund(context.Background(), ports.GatewayRefundRequest{GatewayReferenceID: "order_10", Amount: 1001})
	var gwErr *ports.GatewayError
	if !errors.As(err, &gwErr) || gwErr.Code != "refund_exceeds_captured" || posted {
		t.Fatalf("expected refund_exceeds_captured with nothing posted, got %v posted=%v", err, posted)
	}
}

func TestRefund_CarriesOurRefundIDInNotes(t *testing.T) {
	id := uuid.New()
	var body map[string]any
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&body)
		_, _ = w.Write([]byte(`{"id":"rfnd_1","status":"pending"}`))
	})
	defer srv.Close()

	if _, err := a.Refund(context.Background(), ports.GatewayRefundRequest{RefundID: id, GatewayReferenceID: "pay_1", Amount: 100, Reason: "r"}); err != nil {
		t.Fatal(err)
	}
	notes, _ := body["notes"].(map[string]any)
	if notes["refund_id"] != id.String() || notes["reason"] != "r" {
		t.Fatalf("recovery matches refunds by notes.refund_id, got notes %v", notes)
	}
}

func TestRefundStatus(t *testing.T) {
	ours := uuid.New()
	for name, tc := range map[string]struct {
		req    ports.GatewayRefundStatusRequest
		routes map[string]string
		want   ports.GatewayRefundStatus
		wantID string
	}{
		"by gateway refund id": {
			ports.GatewayRefundStatusRequest{RefundID: ours, GatewayRefundID: "rfnd_1"},
			map[string]string{"/v1/refunds/rfnd_1": `{"id":"rfnd_1","status":"processed"}`},
			ports.GatewayRefundStatusCompleted, "rfnd_1",
		},
		"matched by notes on the captured payment": {
			ports.GatewayRefundStatusRequest{RefundID: ours, GatewayReferenceID: "order_1"},
			map[string]string{
				"/v1/orders/order_1/payments": `{"items":[{"id":"pay_1","status":"captured","amount":1000}]}`,
				"/v1/payments/pay_1/refunds":  `{"items":[{"id":"rfnd_x","status":"processed","notes":{"refund_id":"x"}},{"id":"rfnd_ours","status":"pending","notes":{"refund_id":"` + ours.String() + `"}}]}`,
			},
			ports.GatewayRefundStatusProcessing, "rfnd_ours",
		},
		"not there": {
			ports.GatewayRefundStatusRequest{RefundID: ours, GatewayReferenceID: "pay_1"},
			map[string]string{"/v1/payments/pay_1/refunds": `{"items":[]}`},
			ports.GatewayRefundStatusNotFound, "",
		},
		"full refund: the only payment is now refunded": {
			ports.GatewayRefundStatusRequest{RefundID: ours, GatewayReferenceID: "order_2"},
			map[string]string{
				"/v1/orders/order_2/payments": `{"items":[{"id":"pay_declined","status":"failed"},{"id":"pay_2","status":"refunded","amount":1000,"amount_refunded":1000}]}`,
				"/v1/payments/pay_2/refunds":  `{"items":[{"id":"rfnd_full","status":"processed","notes":{"refund_id":"` + ours.String() + `"}}]}`,
			},
			ports.GatewayRefundStatusCompleted, "rfnd_full",
		},
		"no single payment to look under": {
			ports.GatewayRefundStatusRequest{RefundID: ours, GatewayReferenceID: "order_3"},
			map[string]string{"/v1/orders/order_3/payments": `{"items":[{"id":"pay_x","status":"failed"}]}`},
			ports.GatewayRefundStatusUnknown, "",
		},
	} {
		a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
			body, ok := tc.routes[r.URL.Path]
			if r.Method != http.MethodGet || !ok {
				t.Errorf("%s: a status check must only read known paths, got %s %s", name, r.Method, r.URL.Path)
			}
			_, _ = w.Write([]byte(body))
		})
		resp, err := a.RefundStatus(context.Background(), tc.req)
		srv.Close()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if resp.Status != tc.want || resp.GatewayRefundID != tc.wantID {
			t.Errorf("%s: got %s %q, want %s %q", name, resp.Status, resp.GatewayRefundID, tc.want, tc.wantID)
		}
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
