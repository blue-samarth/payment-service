package payu

import (
	"context"
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
