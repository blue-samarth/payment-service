package razorpay

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"samarth/payment-service/internal/ports"
)

func rzpSig(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func TestParseWebhook_Valid(t *testing.T) {
	a := New(Config{})
	secret := "whsec"
	body := []byte(`{"event":"payment.captured","payload":{"payment":{"entity":{"id":"pay_1","order_id":"order_1","status":"captured"}}}}`)
	headers := map[string]string{"X-Razorpay-Signature": rzpSig(secret, body), "X-Razorpay-Event-Id": "evt_rzp"}

	ev, err := a.ParseWebhook(body, headers, secret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventID != "evt_rzp" || ev.GatewayReferenceID != "order_1" || ev.Status != ports.GatewayPaymentStatusSucceeded {
		t.Errorf("unexpected event: %+v", ev)
	}
}

func TestParseWebhook_CarriesTransactionIDFromNotes(t *testing.T) {
	const id = "0b7c3a52-6a3e-4c1f-9d2e-6f1a2b3c4d5e"
	cases := map[string]string{
		"order.paid carries the order's notes": `{"event":"order.paid","payload":{"payment":{"entity":{"id":"pay_1","order_id":"order_1","status":"captured"}},"order":{"entity":{"notes":{"transaction_id":"` + id + `"}}}}}`,
		"payment notes as a fallback":          `{"event":"payment.captured","payload":{"payment":{"entity":{"id":"pay_1","order_id":"order_1","status":"captured","notes":{"transaction_id":"` + id + `"}}}}}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			a := New(Config{})
			body := []byte(raw)
			ev, err := a.ParseWebhook(body, map[string]string{"X-Razorpay-Signature": rzpSig("whsec", body)}, "whsec")
			if err != nil {
				t.Fatal(err)
			}
			if ev.TransactionID != id {
				t.Errorf("got %q", ev.TransactionID)
			}
		})
	}
}

func TestParseWebhook_BadSignature(t *testing.T) {
	a := New(Config{})
	body := []byte(`{"payload":{"payment":{"entity":{"order_id":"order_1","status":"captured"}}}}`)
	headers := map[string]string{"X-Razorpay-Signature": rzpSig("wrong", body)}

	_, err := a.ParseWebhook(body, headers, "real")
	if !errors.Is(err, ports.ErrWebhookSignature) {
		t.Fatalf("expected ErrWebhookSignature, got %v", err)
	}
}

func TestParseWebhook_FailedStatus(t *testing.T) {
	a := New(Config{})
	secret := "whsec"
	body := []byte(`{"payload":{"payment":{"entity":{"id":"pay_2","order_id":"order_2","status":"failed"}}}}`)
	headers := map[string]string{"X-Razorpay-Signature": rzpSig(secret, body)}

	ev, err := a.ParseWebhook(body, headers, secret)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Status != ports.GatewayPaymentStatusFailed {
		t.Errorf("expected FAILED, got %s", ev.Status)
	}
	if ev.EventID != "pay_2" {
		t.Errorf("expected fallback event id from payment id, got %q", ev.EventID)
	}
}
