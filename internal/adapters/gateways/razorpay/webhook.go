package razorpay

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"samarth/payment-service/internal/ports"
)

type rzpWebhook struct {
	Event   string `json:"event"`
	Payload struct {
		Payment struct {
			Entity struct {
				ID      string `json:"id"`
				OrderID string `json:"order_id"`
				Status  string `json:"status"`
			} `json:"entity"`
		} `json:"payment"`
	} `json:"payload"`
}

func (a *Adapter) ParseWebhook(body []byte, headers map[string]string, secret string) (*ports.GatewayWebhookEvent, error) {
	provided := headers["X-Razorpay-Signature"]
	if provided == "" {
		return nil, ports.ErrWebhookSignature
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(provided)) {
		return nil, ports.ErrWebhookSignature
	}

	var wh rzpWebhook
	if err := json.Unmarshal(body, &wh); err != nil {
		return nil, ports.ErrWebhookParse
	}
	payment := wh.Payload.Payment.Entity

	eventID := headers["X-Razorpay-Event-Id"]
	if eventID == "" {
		eventID = payment.ID
	}

	return &ports.GatewayWebhookEvent{
		EventID:            eventID,
		GatewayReferenceID: payment.OrderID,
		Status:             mapWebhookPaymentStatus(payment.Status),
	}, nil
}

func mapWebhookPaymentStatus(s string) ports.GatewayPaymentStatus {
	switch s {
	case "captured":
		return ports.GatewayPaymentStatusSucceeded
	case "failed":
		return ports.GatewayPaymentStatusFailed
	case "authorized":
		return ports.GatewayPaymentStatusProcessing
	default:
		return ports.GatewayPaymentStatusPending
	}
}
