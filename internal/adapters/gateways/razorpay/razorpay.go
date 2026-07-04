package razorpay

import (
	"bytes"
	"context"
	"encoding/json"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type Config struct {
	KeyID      string
	KeySecret  string
	BaseURL    string
	HTTPClient *http.Client
}

type Adapter struct {
	keyID     string
	keySecret string
	baseURL   string
	client    *http.Client
}

func New(cfg Config) *Adapter {
	base := cfg.BaseURL
	if base == "" {
		base = "https://api.razorpay.com"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Adapter{
		keyID:     cfg.KeyID,
		keySecret: cfg.KeySecret,
		baseURL:   strings.TrimRight(base, "/"),
		client:    client,
	}
}

func (a *Adapter) Capabilities() ports.GatewayCapabilities {
	return ports.GatewayCapabilities{
		SupportsCancel:        false,
		SupportsPartialRefund: true,
		IdempotencyCapable:    true,
		SupportedPaymentMethods: []transaction.PaymentMethod{
			transaction.PaymentMethodCard, transaction.PaymentMethodUPI,
			transaction.PaymentMethodNetbanking, transaction.PaymentMethodWallet,
		},
		SupportedCurrencies: []string{"INR"},
	}
}

func (a *Adapter) InitiatePayment(ctx context.Context, req ports.GatewayPaymentRequest) (*ports.GatewayPaymentResponse, error) {
	receipt := req.IdempotencyKey
	if receipt == "" {
		receipt = deriveReceipt(req.TransactionID, req.AttemptNumber)
	}

	body := map[string]any{
		"amount":   req.Amount,
		"currency": strings.ToUpper(req.Currency),
		"receipt":  receipt,
		"notes":    map[string]string{"transaction_id": req.TransactionID.String()},
	}

	var order rzpOrder
	if err := a.do(ctx, http.MethodPost, "/v1/orders", body, &order); err != nil {
		return nil, err
	}
	return toPaymentResponse(&order), nil
}

func (a *Adapter) CheckStatus(ctx context.Context, req ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	var order rzpOrder
	if err := a.do(ctx, http.MethodGet, "/v1/orders/"+url.PathEscape(req.GatewayReferenceID), nil, &order); err != nil {
		return nil, err
	}
	return toPaymentResponse(&order), nil
}

func (a *Adapter) Refund(ctx context.Context, req ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	body := map[string]any{}
	if req.Amount > 0 {
		body["amount"] = req.Amount
	}
	if req.Reason != "" {
		body["notes"] = map[string]string{"reason": req.Reason}
	}

	var refund rzpRefund
	if err := a.do(ctx, http.MethodPost, "/v1/payments/"+url.PathEscape(req.GatewayReferenceID)+"/refund", body, &refund); err != nil {
		return nil, err
	}
	return &ports.GatewayRefundResponse{
		GatewayRefundID: refund.ID,
		Status:          mapRefundStatus(refund.Status),
		Amount:          refund.Amount,
		Currency:        strings.ToUpper(refund.Currency),
	}, nil
}

func (a *Adapter) Cancel(ctx context.Context, req ports.GatewayCancelRequest) (*ports.GatewayCancelResponse, error) {
	return &ports.GatewayCancelResponse{Status: ports.GatewayCancelStatusNotSupported}, nil
}

func (a *Adapter) do(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "marshal_error", GatewayMessage: err.Error()}
		}
		reader = bytes.NewReader(b)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, reader)
	if err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "request_build_error", GatewayMessage: err.Error()}
	}
	httpReq.SetBasicAuth(a.keyID, a.keySecret)
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout, Code: "network_error", GatewayMessage: err.Error(), Retryable: true, Underlying: err}
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		var env rzpErrorEnvelope
		_ = json.Unmarshal(data, &env)
		e := env.Error
		if e == nil {
			e = &rzpError{Code: "SERVER_ERROR", Description: "unknown gateway error"}
		}
		cat := classifyError(e.Code, e.Reason)
		return &ports.GatewayError{
			Category:       cat,
			Code:           string(cat),
			GatewayCode:    firstNonEmpty(e.Reason, e.Code),
			GatewayMessage: e.Description,
			Retryable:      cat == ports.ErrorCategoryNetworkTimeout || cat == ports.ErrorCategorySoftDecline,
		}
	}

	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "decode_error", GatewayMessage: err.Error()}
		}
	}
	return nil
}

func deriveReceipt(id uuid.UUID, attemptNumber int) string {
	sum := crc32.ChecksumIEEE([]byte(id.String() + strconv.Itoa(attemptNumber)))
	enc := base62(sum)
	if len(enc) >= 40 {
		return enc[:40]
	}
	return strings.Repeat("0", 40-len(enc)) + enc
}

const base62chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func base62(n uint32) string {
	if n == 0 {
		return "0"
	}
	var buf []byte
	for n > 0 {
		buf = append([]byte{base62chars[n%62]}, buf...)
		n /= 62
	}
	return string(buf)
}

func toPaymentResponse(o *rzpOrder) *ports.GatewayPaymentResponse {
	resp := &ports.GatewayPaymentResponse{
		GatewayReferenceID: o.ID,
		Status:             mapOrderStatus(o.Status),
		Amount:             o.Amount,
		Currency:           strings.ToUpper(o.Currency),
		RawMetadata:        map[string]any{},
	}
	for k, v := range o.Notes {
		resp.RawMetadata[k] = v
	}
	return resp
}

func mapOrderStatus(s string) ports.GatewayPaymentStatus {
	switch s {
	case "paid":
		return ports.GatewayPaymentStatusSucceeded
	case "attempted":
		return ports.GatewayPaymentStatusProcessing
	case "created":
		return ports.GatewayPaymentStatusPending
	default:
		return ports.GatewayPaymentStatusPending
	}
}

func mapRefundStatus(s string) ports.GatewayRefundStatus {
	switch s {
	case "processed":
		return ports.GatewayRefundStatusCompleted
	case "pending":
		return ports.GatewayRefundStatusProcessing
	case "failed":
		return ports.GatewayRefundStatusFailed
	default:
		return ports.GatewayRefundStatusProcessing
	}
}

func classifyError(code, reason string) ports.ErrorCategory {
	switch code {
	case "GATEWAY_ERROR":
		return ports.ErrorCategoryGatewayError
	case "SERVER_ERROR":
		return ports.ErrorCategoryNetworkTimeout
	case "BAD_REQUEST_ERROR":
		switch reason {
		case "payment_failed", "payment_declined", "card_declined":
			return ports.ErrorCategoryHardDecline
		case "insufficient_funds", "payment_pending":
			return ports.ErrorCategorySoftDecline
		default:
			return ports.ErrorCategoryGatewayError
		}
	default:
		return ports.ErrorCategoryGatewayError
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

var _ ports.GatewayAdapter = (*Adapter)(nil)
