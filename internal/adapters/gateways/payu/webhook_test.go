package payu

import (
	"bytes"
	"encoding/hex"
	"errors"
	"net/url"
	"testing"

	"samarth/payment-service/internal/ports"
)

func signedForm(salt string) url.Values {
	form := url.Values{}
	form.Set("status", "success")
	form.Set("txnid", "TXN-1")
	form.Set("mihpayid", "403993")
	form.Set("amount", "1500.00")
	form.Set("productinfo", "prod")
	form.Set("firstname", "john")
	form.Set("email", "j@e.com")
	form.Set("key", "mkey")
	form.Set("hash", hex.EncodeToString(reverseHash(salt, form)))
	return form
}

func TestParseWebhook_Valid(t *testing.T) {
	a := New(Config{})
	salt := "salt"
	body := []byte(signedForm(salt).Encode())

	ev, err := a.ParseWebhook(body, nil, salt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.GatewayReferenceID != "TXN-1" || ev.EventID != "403993" || ev.Status != ports.GatewayPaymentStatusSucceeded {
		t.Errorf("unexpected event: %+v", ev)
	}
}

func TestParseWebhook_BadHash(t *testing.T) {
	a := New(Config{})
	form := signedForm("salt")
	form.Set("hash", "deadbeef") // tamper
	body := []byte(form.Encode())

	_, err := a.ParseWebhook(body, nil, "salt")
	if !errors.Is(err, ports.ErrWebhookSignature) {
		t.Fatalf("expected ErrWebhookSignature for tampered hash, got %v", err)
	}
}

func TestParseWebhook_TamperedFieldFailsHash(t *testing.T) {
	a := New(Config{})
	salt := "salt"
	form := signedForm(salt)
	form.Set("amount", "9999.00") // change amount after signing
	body := []byte(form.Encode())

	_, err := a.ParseWebhook(body, nil, salt)
	if !errors.Is(err, ports.ErrWebhookSignature) {
		t.Fatalf("tampering a signed field must fail the reverse hash, got %v", err)
	}
}

func TestReverseHash_IncludesAdditionalCharges(t *testing.T) {
	base := signedForm("salt")

	withAC := url.Values{}
	for k, v := range base {
		withAC[k] = v
	}
	withAC.Set("additionalCharges", "5.00")

	// PayU prepends additionalCharges to the reverse hash; ignoring it (the old
	// behaviour) would produce the same digest and fail those callbacks.
	if bytes.Equal(reverseHash("salt", base), reverseHash("salt", withAC)) {
		t.Error("additionalCharges must change the reverse hash")
	}
}

func TestParseWebhook_WithAdditionalCharges(t *testing.T) {
	a := New(Config{})
	salt := "salt"

	form := signedForm(salt) // start from a valid form, then add + re-sign with the charge
	form.Set("additionalCharges", "5.00")
	form.Set("hash", hex.EncodeToString(reverseHash(salt, form)))

	ev, err := a.ParseWebhook([]byte(form.Encode()), nil, salt)
	if err != nil {
		t.Fatalf("a callback carrying additionalCharges must verify, got %v", err)
	}
	if ev.GatewayReferenceID != "TXN-1" {
		t.Errorf("unexpected reference: %s", ev.GatewayReferenceID)
	}
}
