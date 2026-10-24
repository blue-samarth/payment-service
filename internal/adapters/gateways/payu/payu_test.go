package payu

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/ports"
)

func newTestAdapter(handler http.HandlerFunc) (*Adapter, *httptest.Server) {
	srv := httptest.NewServer(handler)
	return New(Config{MerchantKey: "key", MerchantSalt: "salt", BaseURL: srv.URL, HTTPClient: srv.Client()}), srv
}

func TestInitiatePayment_ReturnsPendingTxnID(t *testing.T) {
	a := New(Config{MerchantKey: "key", MerchantSalt: "salt"})
	id := uuid.New()
	resp, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{TransactionID: id, Amount: 150000, Currency: "INR", AttemptNumber: 1})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayPaymentStatusPending {
		t.Errorf("PayU initiate should be PENDING (redirect flow), got %s", resp.Status)
	}
	if resp.GatewayReferenceID != deriveTxnID(id, 1) {
		t.Errorf("expected derived txnid as reference, got %q", resp.GatewayReferenceID)
	}
}

func TestCheckStatus_SuccessMapsToSucceeded(t *testing.T) {
	var gotCommand, gotVar1, gotHash string
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		gotCommand = r.FormValue("command")
		gotVar1 = r.FormValue("var1")
		gotHash = r.FormValue("hash")
		_, _ = w.Write([]byte(`{"status":1,"transaction_details":{"TXN-1":{"txnid":"TXN-1","status":"success","amt":"1500.00","mihpayid":"403993"}}}`))
	})
	defer srv.Close()

	resp, err := a.CheckStatus(context.Background(), ports.GatewayStatusRequest{GatewayReferenceID: "TXN-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayPaymentStatusSucceeded {
		t.Errorf("expected SUCCEEDED, got %s", resp.Status)
	}
	if resp.Amount != 150000 {
		t.Errorf("expected 150000 paise from '1500.00', got %d", resp.Amount)
	}
	if gotCommand != "verify_payment" || gotVar1 != "TXN-1" || gotHash == "" {
		t.Errorf("unexpected request fields: command=%q var1=%q hash=%q", gotCommand, gotVar1, gotHash)
	}
}

func TestCheckStatus_FailureMapsToFailed(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":1,"transaction_details":{"TXN-2":{"txnid":"TXN-2","status":"failure","amt":"1500.00"}}}`))
	})
	defer srv.Close()

	resp, err := a.CheckStatus(context.Background(), ports.GatewayStatusRequest{GatewayReferenceID: "TXN-2"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayPaymentStatusFailed {
		t.Errorf("expected FAILED, got %s", resp.Status)
	}
}

func TestRefund_AcceptedIsProcessing(t *testing.T) {
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.FormValue("command") != "cancel_refund_transaction" {
			t.Errorf("unexpected command %q", r.FormValue("command"))
		}
		_, _ = w.Write([]byte(`{"status":1,"request_id":"req_99","msg":"refund accepted"}`))
	})
	defer srv.Close()

	resp, err := a.Refund(context.Background(), ports.GatewayRefundRequest{RefundID: uuid.New(), GatewayReferenceID: "403993", Amount: 150000})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayRefundStatusProcessing {
		t.Errorf("PayU refund is async; expected PROCESSING, got %s", resp.Status)
	}
	if resp.GatewayRefundID != "req_99" {
		t.Errorf("expected request_id as refund id, got %q", resp.GatewayRefundID)
	}
}

func TestRefund_TxnIDIsResolvedToTheMihpayID(t *testing.T) {
	var refundVar1 string
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.FormValue("command") {
		case "verify_payment":
			if r.FormValue("var1") != "ABC-1" {
				t.Errorf("verify_payment must be asked about the stored txnid, got %q", r.FormValue("var1"))
			}
			_, _ = w.Write([]byte(`{"status":1,"transaction_details":{"ABC-1":{"txnid":"ABC-1","status":"success","amt":"1500.00","mihpayid":"403993"}}}`))
		case "cancel_refund_transaction":
			refundVar1 = r.FormValue("var1")
			_, _ = w.Write([]byte(`{"status":1,"request_id":"req_1"}`))
		}
	})
	defer srv.Close()

	if _, err := a.Refund(context.Background(), ports.GatewayRefundRequest{RefundID: uuid.New(), GatewayReferenceID: "ABC-1", Amount: 150000}); err != nil {
		t.Fatal(err)
	}
	if refundVar1 != "403993" {
		t.Fatalf("cancel_refund_transaction needs the mihpayid, got %q", refundVar1)
	}
}

func TestRefund_UncapturedOrUnknownPaymentIsNotRefunded(t *testing.T) {
	for name, details := range map[string]string{
		"unknown": `{}`,
		"failed":  `{"ABC-2":{"txnid":"ABC-2","status":"failure","mihpayid":"5"}}`,
	} {
		refunded := false
		a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			if r.FormValue("command") == "cancel_refund_transaction" {
				refunded = true
			}
			_, _ = w.Write([]byte(`{"status":1,"transaction_details":` + details + `}`))
		})

		_, err := a.Refund(context.Background(), ports.GatewayRefundRequest{RefundID: uuid.New(), GatewayReferenceID: "ABC-2", Amount: 100})
		srv.Close()
		var gwErr *ports.GatewayError
		if !errors.As(err, &gwErr) || gwErr.Category != ports.ErrorCategoryHardDecline {
			t.Errorf("%s: expected a terminal refusal, got %v", name, err)
		}
		if refunded {
			t.Errorf("%s: no refund may be sent for a payment that was never captured", name)
		}
	}
}

func TestRefund_LookupFailureIsTerminalAndPostsNothing(t *testing.T) {
	for name, status := range map[string]int{"4xx": http.StatusBadRequest, "5xx": http.StatusBadGateway, "no connection": 0} {
		posted := false
		a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			if r.FormValue("command") == "cancel_refund_transaction" {
				posted = true
			}
			w.WriteHeader(status)
		})
		if status == 0 {
			srv.Close()
		}

		_, err := a.Refund(context.Background(), ports.GatewayRefundRequest{RefundID: uuid.New(), GatewayReferenceID: "ABC-3", Amount: 100})
		srv.Close()
		var gwErr *ports.GatewayError
		if !errors.As(err, &gwErr) || gwErr.Category != ports.ErrorCategoryHardDecline || gwErr.Code != "refund_lookup_failed" {
			t.Errorf("%s: nothing was sent, so the refund must end terminally, got %v", name, err)
		}
		if posted {
			t.Errorf("%s: no refund may be sent when the lookup fails", name)
		}
	}
}

func TestRefund_AmountAboveCapturedIsRefused(t *testing.T) {
	posted := false
	a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.FormValue("command") == "cancel_refund_transaction" {
			posted = true
		}
		_, _ = w.Write([]byte(`{"status":1,"transaction_details":{"ABC-4":{"txnid":"ABC-4","status":"success","amt":"10.00","mihpayid":"77"}}}`))
	})
	defer srv.Close()

	_, err := a.Refund(context.Background(), ports.GatewayRefundRequest{RefundID: uuid.New(), GatewayReferenceID: "ABC-4", Amount: 1001})
	var gwErr *ports.GatewayError
	if !errors.As(err, &gwErr) || gwErr.Code != "refund_exceeds_captured" || posted {
		t.Fatalf("expected refund_exceeds_captured with nothing sent, got %v posted=%v", err, posted)
	}
}

func TestRefundStatus(t *testing.T) {
	for name, tc := range map[string]struct {
		details string
		want    ports.GatewayRefundStatus
	}{
		"success":    {`{"req_1":{"403993":{"action":"refund","status":"success","amount":"15.00"}}}`, ports.GatewayRefundStatusCompleted},
		"queued":     {`{"req_1":{"403993":{"action":"refund","status":"queued"}}}`, ports.GatewayRefundStatusProcessing},
		"failure":    {`{"req_1":{"403993":{"action":"refund","status":"failure"}}}`, ports.GatewayRefundStatusFailed},
		"odd status": {`{"req_1":{"403993":{"action":"refund","status":"something_new"}}}`, ports.GatewayRefundStatusUnknown},
		"missing":    {`{}`, ports.GatewayRefundStatusUnknown},
		"not a map":  {`"no records found"`, ports.GatewayRefundStatusUnknown},
	} {
		a, srv := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			if r.FormValue("command") != "check_action_status" || r.FormValue("var1") != "req_1" {
				t.Errorf("%s: a status check must only call check_action_status on the request id, got %q %q", name, r.FormValue("command"), r.FormValue("var1"))
			}
			_, _ = w.Write([]byte(`{"status":1,"transaction_details":` + tc.details + `}`))
		})
		resp, err := a.RefundStatus(context.Background(), ports.GatewayRefundStatusRequest{RefundID: uuid.New(), GatewayRefundID: "req_1"})
		srv.Close()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if resp.Status != tc.want {
			t.Errorf("%s: got %s, want %s", name, resp.Status, tc.want)
		}
	}
}

func TestRefundStatus_WithoutARequestIDIsUnknown(t *testing.T) {
	a := New(Config{})
	resp, err := a.RefundStatus(context.Background(), ports.GatewayRefundStatusRequest{RefundID: uuid.New()})
	if err != nil || resp.Status != ports.GatewayRefundStatusUnknown {
		t.Fatalf("PayU can't look a refund up without its request_id, so it must go to review, got %v %v", resp, err)
	}
}

func TestHash_IsDeterministicSHA512(t *testing.T) {
	a := New(Config{MerchantKey: "key", MerchantSalt: "salt"})
	h1 := a.hash("verify_payment", "TXN-1")
	h2 := a.hash("verify_payment", "TXN-1")
	if h1 != h2 || len(h1) != 128 {
		t.Errorf("expected stable 128-char sha512 hex, got len=%d stable=%v", len(h1), h1 == h2)
	}
}

func TestDeriveTxnID_Format(t *testing.T) {
	id := uuid.New()
	got := deriveTxnID(id, 3)
	if !strings.HasSuffix(got, "-3") {
		t.Errorf("expected attempt suffix, got %q", got)
	}
	if got != strings.ToUpper(got) {
		t.Errorf("PayU txnid must be uppercased, got %q", got)
	}
	if deriveTxnID(id, 3) != got {
		t.Error("txnid must be stable")
	}
	if deriveTxnID(id, 4) == got {
		t.Error("txnid must differ by attempt")
	}
}
