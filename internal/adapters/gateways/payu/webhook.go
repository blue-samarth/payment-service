package payu

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"net/url"
	"strings"

	"samarth/payment-service/internal/ports"
)

// ParseWebhook verifies PayU's reverse hash and parses the form-encoded callback.
// PayU webhooks are application/x-www-form-urlencoded and carry a `hash` field
// that is sha512(salt|status|||||...|udf1|email|firstname|productinfo|amount|txnid|key).
// The webhook secret supplied by the caller is the merchant salt.
func (a *Adapter) ParseWebhook(body []byte, headers map[string]string, secret string) (*ports.GatewayWebhookEvent, error) {
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, ports.ErrWebhookParse
	}

	provided := form.Get("hash")
	if provided == "" {
		return nil, ports.ErrWebhookSignature
	}

	providedMAC, err := hex.DecodeString(strings.ToLower(provided))
	if err != nil || !hmac.Equal(reverseHash(secret, form), providedMAC) {
		return nil, ports.ErrWebhookSignature
	}

	txnid := form.Get("txnid")
	if txnid == "" {
		return nil, ports.ErrWebhookParse
	}
	eventID := form.Get("mihpayid")
	if eventID == "" {
		eventID = txnid
	}

	return &ports.GatewayWebhookEvent{
		EventID:            eventID,
		GatewayReferenceID: txnid,
		Status:             mapStatus(form.Get("status")),
	}, nil
}

func reverseHash(salt string, form url.Values) []byte {
	fields := []string{
		salt,
		form.Get("status"),
		"", "", "", "", "", // udf10..udf6
		form.Get("udf5"), form.Get("udf4"), form.Get("udf3"), form.Get("udf2"), form.Get("udf1"),
		form.Get("email"), form.Get("firstname"), form.Get("productinfo"),
		form.Get("amount"), form.Get("txnid"), form.Get("key"),
	}
	if ac := additionalCharges(form); ac != "" {
		fields = append([]string{ac}, fields...)
	}
	sum := sha512.Sum512([]byte(strings.Join(fields, "|")))
	return sum[:]
}

func additionalCharges(form url.Values) string {
	if ac := form.Get("additionalCharges"); ac != "" {
		return ac
	}
	return form.Get("additional_charges")
}
