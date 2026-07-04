package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	appwebhook "samarth/payment-service/internal/app/webhook"
	"samarth/payment-service/internal/ports"
)

type WebhookProcessor interface {
	Process(ctx context.Context, gatewayID string, ev appwebhook.Event, rawPayload []byte) (appwebhook.Outcome, error)
}

type WebhookParserResolver interface {
	WebhookParser(gatewayID string) (ports.GatewayWebhookParser, bool)
}

type SecretProvider interface {
	WebhookSecret(ctx context.Context, gatewayID string) (string, error)
}

type WebhookPolicyProvider interface {
	WebhookPolicy(ctx context.Context, gatewayID string) (replayWindowSec, clockSkewSec int, err error)
}

type WebhookHandler struct {
	processor WebhookProcessor
	parsers   WebhookParserResolver
	secrets   SecretProvider
	policy    WebhookPolicyProvider
	log       ports.Logger
}

func NewWebhookHandler(processor WebhookProcessor, parsers WebhookParserResolver, secrets SecretProvider, policy WebhookPolicyProvider, log ports.Logger) *WebhookHandler {
	return &WebhookHandler{processor: processor, parsers: parsers, secrets: secrets, policy: policy, log: log}
}

func (h *WebhookHandler) Handle(w http.ResponseWriter, r *http.Request) {
	gatewayID := r.PathValue("gateway_id")

	rawBody, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "could not read webhook body")
		return
	}

	parser, ok := h.parsers.WebhookParser(gatewayID)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown_gateway", "no webhook parser for gateway")
		return
	}

	secret, err := h.secrets.WebhookSecret(r.Context(), gatewayID)
	if err != nil || secret == "" {
		writeError(w, http.StatusUnauthorized, "unknown_gateway", "no webhook secret for gateway")
		return
	}

	ev, err := parser.ParseWebhook(rawBody, headerMap(r), secret)
	if errors.Is(err, ports.ErrWebhookSignature) {
		h.log.Warn(ports.LogEventWebhookInboundInvalid, map[string]any{ports.FieldGatewayID: gatewayID})
		writeError(w, http.StatusUnauthorized, "invalid_signature", "webhook signature verification failed")
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_webhook", "could not parse webhook payload")
		return
	}
	if ev.EventID == "" || ev.GatewayReferenceID == "" {
		writeError(w, http.StatusBadRequest, "invalid_webhook", "event id and reference are required")
		return
	}

	if ok, code, msg := h.checkTimestamp(r, gatewayID); !ok {
		h.log.Warn(ports.LogEventWebhookInboundInvalid, map[string]any{ports.FieldGatewayID: gatewayID, "reason": code})
		writeError(w, http.StatusBadRequest, code, msg)
		return
	}

	outcome, err := h.processor.Process(r.Context(), gatewayID, appwebhook.Event{
		EventID:            ev.EventID,
		GatewayReferenceID: ev.GatewayReferenceID,
		Status:             normalizeStatus(ev.Status),
	}, rawBody)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "webhook_processing_failed", "could not process webhook")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"received":    true,
		"duplicate":   outcome.Duplicate,
		"resolved":    outcome.Resolved,
		"unknown_txn": outcome.UnknownTxn,
	})
}

func (h *WebhookHandler) checkTimestamp(r *http.Request, gatewayID string) (ok bool, code, msg string) {
	if h.policy == nil {
		return true, "", ""
	}
	tsRaw := r.Header.Get("X-Webhook-Timestamp")
	if tsRaw == "" {
		return true, "", ""
	}

	ts, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return false, "invalid_timestamp", "X-Webhook-Timestamp must be a unix timestamp"
	}

	window, skew, err := h.policy.WebhookPolicy(r.Context(), gatewayID)
	if err != nil {
		return false, "policy_unavailable", "could not resolve webhook policy"
	}

	tolerance := int64(window + skew)
	diff := time.Now().Unix() - ts
	if diff < 0 {
		diff = -diff
	}
	if diff > tolerance {
		return false, "stale_webhook", "webhook timestamp outside the accepted window"
	}
	return true, "", ""
}

func headerMap(r *http.Request) map[string]string {
	m := make(map[string]string, len(r.Header))
	for k := range r.Header {
		m[k] = r.Header.Get(k)
	}
	return m
}

func normalizeStatus(s ports.GatewayPaymentStatus) string {
	switch s {
	case ports.GatewayPaymentStatusSucceeded:
		return "succeeded"
	case ports.GatewayPaymentStatusFailed:
		return "failed"
	case ports.GatewayPaymentStatusCancelled:
		return "cancelled"
	default:
		return "pending"
	}
}
