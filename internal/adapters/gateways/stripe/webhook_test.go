package stripe

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"testing"
	"time"

	"samarth/payment-service/internal/ports"
)

func stripeSig(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return "t=" + strconv.FormatInt(ts, 10) + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func TestParseWebhook_Valid(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	body := []byte(`{"id":"evt_1","type":"payment_intent.succeeded","data":{"object":{"id":"pi_1","status":"succeeded"}}}`)
	headers := map[string]string{"Stripe-Signature": stripeSig(secret, time.Now().Unix(), body)}

	ev, err := a.ParseWebhook(body, headers, secret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventID != "evt_1" || ev.GatewayReferenceID != "pi_1" || ev.Status != ports.GatewayPaymentStatusSucceeded {
		t.Errorf("unexpected event: %+v", ev)
	}
}

func TestParseWebhook_CarriesTheCapturedAmount(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	for name, tc := range map[string]struct {
		body string
		want int64
	}{
		"amount_received wins": {`{"id":"evt_1","data":{"object":{"id":"pi_1","status":"succeeded","amount":150000,"amount_received":149000,"currency":"inr"}}}`, 149000},
		"amount as a fallback": {`{"id":"evt_1","data":{"object":{"id":"pi_1","status":"succeeded","amount":150000,"currency":"inr"}}}`, 150000},
	} {
		body := []byte(tc.body)
		ev, err := a.ParseWebhook(body, map[string]string{"Stripe-Signature": stripeSig(secret, time.Now().Unix(), body)}, secret)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if ev.Amount != tc.want || ev.Currency != "INR" {
			t.Errorf("%s: got %d %q, want %d INR", name, ev.Amount, ev.Currency, tc.want)
		}
	}
}

func TestParseWebhook_CarriesTransactionIDFromMetadata(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	body := []byte(`{"id":"evt_1","type":"payment_intent.succeeded","data":{"object":{"id":"pi_1","status":"succeeded","metadata":{"transaction_id":"0b7c3a52-6a3e-4c1f-9d2e-6f1a2b3c4d5e"}}}}`)
	headers := map[string]string{"Stripe-Signature": stripeSig(secret, time.Now().Unix(), body)}

	ev, err := a.ParseWebhook(body, headers, secret)
	if err != nil {
		t.Fatal(err)
	}
	if ev.TransactionID != "0b7c3a52-6a3e-4c1f-9d2e-6f1a2b3c4d5e" {
		t.Errorf("the transaction id set as metadata at creation must come back for fallback matching, got %q", ev.TransactionID)
	}
}

func TestParseWebhook_BadSignature(t *testing.T) {
	a := New(Config{})
	body := []byte(`{"id":"evt_1","data":{"object":{"id":"pi_1","status":"succeeded"}}}`)
	headers := map[string]string{"Stripe-Signature": stripeSig("the-wrong-secret", time.Now().Unix(), body)}

	_, err := a.ParseWebhook(body, headers, "real-secret")
	if !errors.Is(err, ports.ErrWebhookSignature) {
		t.Fatalf("expected ErrWebhookSignature, got %v", err)
	}
}

func TestParseWebhook_StaleTimestamp(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	body := []byte(`{"id":"evt_1","data":{"object":{"id":"pi_1","status":"succeeded"}}}`)
	old := time.Now().Add(-time.Hour).Unix()
	headers := map[string]string{"Stripe-Signature": stripeSig(secret, old, body)}

	_, err := a.ParseWebhook(body, headers, secret)
	if !errors.Is(err, ports.ErrWebhookSignature) {
		t.Fatalf("stale timestamp should fail signature verification, got %v", err)
	}
}

func TestParseWebhook_MissingHeader(t *testing.T) {
	a := New(Config{})
	_, err := a.ParseWebhook([]byte(`{}`), map[string]string{}, "secret")
	if !errors.Is(err, ports.ErrWebhookSignature) {
		t.Fatalf("missing signature header should fail, got %v", err)
	}
}
