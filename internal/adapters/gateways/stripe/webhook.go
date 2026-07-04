package stripe

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"samarth/payment-service/internal/ports"
)

const webhookToleranceSeconds = 300

type stripeWebhookEvent struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data struct {
		Object struct {
			ID               string       `json:"id"`
			Status           string       `json:"status"`
			LastPaymentError *stripeError `json:"last_payment_error"`
		} `json:"object"`
	} `json:"data"`
}

func (a *Adapter) ParseWebhook(body []byte, headers map[string]string, secret string) (*ports.GatewayWebhookEvent, error) {
	ts, v1, ok := parseStripeSignature(headers["Stripe-Signature"])
	if !ok {
		return nil, ports.ErrWebhookSignature
	}

	providedMAC, err := hex.DecodeString(v1)
	if err != nil {
		return nil, ports.ErrWebhookSignature
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	if !hmac.Equal(mac.Sum(nil), providedMAC) {
		return nil, ports.ErrWebhookSignature
	}

	if diff := time.Now().Unix() - ts; diff > webhookToleranceSeconds || diff < -webhookToleranceSeconds {
		return nil, ports.ErrWebhookSignature
	}

	var ev stripeWebhookEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return nil, ports.ErrWebhookParse
	}

	return &ports.GatewayWebhookEvent{
		EventID:            ev.ID,
		GatewayReferenceID: ev.Data.Object.ID,
		Status:             mapStatusString(ev.Data.Object.Status, ev.Data.Object.LastPaymentError != nil),
	}, nil
}

func parseStripeSignature(header string) (ts int64, v1 string, ok bool) {
	for _, part := range strings.Split(header, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "t":
			ts, _ = strconv.ParseInt(kv[1], 10, 64)
		case "v1":
			v1 = kv[1]
		}
	}
	if ts == 0 || v1 == "" {
		return 0, "", false
	}
	return ts, v1, true
}

func mapStatusString(s string, hasError bool) ports.GatewayPaymentStatus {
	if hasError {
		return ports.GatewayPaymentStatusFailed
	}
	switch s {
	case "succeeded":
		return ports.GatewayPaymentStatusSucceeded
	case "processing":
		return ports.GatewayPaymentStatusProcessing
	case "canceled":
		return ports.GatewayPaymentStatusCancelled
	default:
		return ports.GatewayPaymentStatusPending
	}
}
