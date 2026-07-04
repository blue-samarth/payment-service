package stripe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	APIKey     string
	BaseURL    string
	HTTPClient *http.Client
}

type Adapter struct {
	apiKey  string
	baseURL string
	client  *http.Client
}

func New(cfg Config) *Adapter {
	base := cfg.BaseURL
	if base == "" {
		base = "https://api.stripe.com"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Adapter{
		apiKey:  cfg.APIKey,
		baseURL: strings.TrimRight(base, "/"),
		client:  client,
	}
}

func (a *Adapter) Capabilities() ports.GatewayCapabilities {
	return ports.GatewayCapabilities{
		SupportsCancel:          true,
		SupportsPartialRefund:   true,
		IdempotencyCapable:      true,
		SupportedPaymentMethods: []transaction.PaymentMethod{transaction.PaymentMethodCard},
		SupportedCurrencies:     []string{"USD", "EUR", "GBP", "INR"},
	}
}

func (a *Adapter) InitiatePayment(ctx context.Context, req ports.GatewayPaymentRequest) (*ports.GatewayPaymentResponse, error) {
	idem := req.IdempotencyKey
	if idem == "" {
		idem = deriveIdempotencyKey(req.TransactionID, req.AttemptNumber)
	}

	form := url.Values{}
	form.Set("amount", strconv.FormatInt(req.Amount, 10))
	form.Set("currency", strings.ToLower(req.Currency))
	if req.Description != "" {
		form.Set("description", req.Description)
	}
	if req.CustomerEmail != "" {
		form.Set("receipt_email", req.CustomerEmail)
	}
	form.Set("metadata[transaction_id]", req.TransactionID.String())

	var pi stripePaymentIntent
	if err := a.do(ctx, http.MethodPost, "/v1/payment_intents", form, idem, &pi); err != nil {
		return nil, err
	}
	return toPaymentResponse(&pi), nil
}

func (a *Adapter) CheckStatus(ctx context.Context, req ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	var pi stripePaymentIntent
	if err := a.do(ctx, http.MethodGet, "/v1/payment_intents/"+url.PathEscape(req.GatewayReferenceID), nil, "", &pi); err != nil {
		return nil, err
	}
	return toPaymentResponse(&pi), nil
}

func (a *Adapter) Refund(ctx context.Context, req ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	idem := req.IdempotencyKey
	if idem == "" {
		idem = deriveIdempotencyKey(req.RefundID, 0)
	}

	form := url.Values{}
	form.Set("payment_intent", req.GatewayReferenceID)
	if req.Amount > 0 {
		form.Set("amount", strconv.FormatInt(req.Amount, 10))
	}
	if req.Reason != "" {
		form.Set("metadata[reason]", req.Reason)
	}

	var rf stripeRefund
	if err := a.do(ctx, http.MethodPost, "/v1/refunds", form, idem, &rf); err != nil {
		return nil, err
	}
	return &ports.GatewayRefundResponse{
		GatewayRefundID: rf.ID,
		Status:          mapRefundStatus(rf.Status),
		Amount:          rf.Amount,
		Currency:        strings.ToUpper(rf.Currency),
	}, nil
}

func (a *Adapter) Cancel(ctx context.Context, req ports.GatewayCancelRequest) (*ports.GatewayCancelResponse, error) {
	var pi stripePaymentIntent
	if err := a.do(ctx, http.MethodPost, "/v1/payment_intents/"+url.PathEscape(req.GatewayReferenceID)+"/cancel", url.Values{}, req.IdempotencyKey, &pi); err != nil {
		return nil, err
	}
	status := ports.GatewayCancelStatusFailed
	if pi.Status == "canceled" {
		status = ports.GatewayCancelStatusCancelled
	}
	return &ports.GatewayCancelResponse{Status: status}, nil
}

func (a *Adapter) do(ctx context.Context, method, path string, form url.Values, idemKey string, out any) error {
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, a.baseURL+path, body)
	if err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "request_build_error", GatewayMessage: err.Error(), Underlying: err}
	}
	httpReq.SetBasicAuth(a.apiKey, "")
	if form != nil {
		httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if idemKey != "" {
		httpReq.Header.Set("Idempotency-Key", idemKey)
	}

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout, Code: "network_error", GatewayMessage: err.Error(), Retryable: true, Underlying: err}
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout, Code: "read_error", GatewayMessage: err.Error(), Retryable: true, Underlying: err}
	}

	if resp.StatusCode >= 400 {
		var env stripeErrorEnvelope
		_ = json.Unmarshal(data, &env)
		se := env.Error
		if se == nil {
			se = &stripeError{Type: "api_error", Message: "unknown gateway error"}
		}
		cat := classifyError(se.Type, se.Code, se.DeclineCode)
		return &ports.GatewayError{
			Category:       cat,
			Code:           string(cat),
			GatewayCode:    firstNonEmpty(se.DeclineCode, se.Code, se.Type),
			GatewayMessage: se.Message,
			Retryable:      isRetryable(cat),
		}
	}

	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "decode_error", GatewayMessage: err.Error(), Underlying: err}
		}
	}
	return nil
}

func deriveIdempotencyKey(id uuid.UUID, attemptNumber int) string {
	sum := sha256.Sum256([]byte(id.String() + ":" + strconv.Itoa(attemptNumber)))
	key := hex.EncodeToString(sum[:])
	if len(key) > 255 {
		key = key[:255]
	}
	return key
}

func toPaymentResponse(pi *stripePaymentIntent) *ports.GatewayPaymentResponse {
	resp := &ports.GatewayPaymentResponse{
		GatewayReferenceID: pi.ID,
		Status:             mapPaymentStatus(pi),
		Amount:             pi.Amount,
		Currency:           strings.ToUpper(pi.Currency),
		RawMetadata:        map[string]any{},
	}
	for k, v := range pi.Metadata {
		resp.RawMetadata[k] = v
	}
	if pi.LastPaymentError != nil {
		resp.ErrorCode = firstNonEmpty(pi.LastPaymentError.DeclineCode, pi.LastPaymentError.Code)
		resp.ErrorMessage = pi.LastPaymentError.Message
	}
	if pi.Charges != nil && len(pi.Charges.Data) > 0 {
		c := pi.Charges.Data[0]
		resp.GatewayFees = c.balanceTransactionFee()
		if c.PaymentMethodDetails != nil && c.PaymentMethodDetails.Card != nil {
			resp.MethodResponse = &ports.GatewayCardResponse{
				CardBrand: c.PaymentMethodDetails.Card.Brand,
				Last4:     c.PaymentMethodDetails.Card.Last4,
				Network:   c.PaymentMethodDetails.Card.Network,
			}
		}
	}
	return resp
}

func mapPaymentStatus(pi *stripePaymentIntent) ports.GatewayPaymentStatus {
	if pi.LastPaymentError != nil {
		return ports.GatewayPaymentStatusFailed
	}
	switch pi.Status {
	case "succeeded":
		return ports.GatewayPaymentStatusSucceeded
	case "processing":
		return ports.GatewayPaymentStatusProcessing
	case "canceled":
		return ports.GatewayPaymentStatusCancelled
	case "requires_payment_method", "requires_confirmation", "requires_action", "requires_capture":
		return ports.GatewayPaymentStatusPending
	default:
		return ports.GatewayPaymentStatusPending
	}
}

func mapRefundStatus(s string) ports.GatewayRefundStatus {
	switch s {
	case "succeeded":
		return ports.GatewayRefundStatusCompleted
	case "pending":
		return ports.GatewayRefundStatusProcessing
	case "failed", "canceled":
		return ports.GatewayRefundStatusFailed
	default:
		return ports.GatewayRefundStatusProcessing
	}
}

func classifyError(errType, code, declineCode string) ports.ErrorCategory {
	switch errType {
	case "card_error":
		switch declineCode {
		case "insufficient_funds", "try_again_later", "processing_error", "issuer_not_available":
			return ports.ErrorCategorySoftDecline
		}
		switch code {
		case "card_declined", "expired_card", "incorrect_cvc", "incorrect_number", "invalid_cvc", "invalid_expiry_month", "invalid_expiry_year":
			return ports.ErrorCategoryHardDecline
		}
		return ports.ErrorCategoryHardDecline
	case "rate_limit_error", "api_connection_error":
		return ports.ErrorCategoryNetworkTimeout
	case "idempotency_error":
		return ports.ErrorCategoryIdempotencyConflict
	case "api_error", "invalid_request_error", "authentication_error":
		return ports.ErrorCategoryGatewayError
	default:
		return ports.ErrorCategoryGatewayError
	}
}

func isRetryable(cat ports.ErrorCategory) bool {
	return cat == ports.ErrorCategorySoftDecline || cat == ports.ErrorCategoryNetworkTimeout
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
