package payu

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
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
	MerchantKey  string
	MerchantSalt string
	BaseURL      string
	HTTPClient   *http.Client
}

type Adapter struct {
	key     string
	salt    string
	baseURL string
	client  *http.Client
}

func New(cfg Config) *Adapter {
	base := cfg.BaseURL
	if base == "" {
		base = "https://info.payu.in"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &Adapter{
		key:     cfg.MerchantKey,
		salt:    cfg.MerchantSalt,
		baseURL: strings.TrimRight(base, "/"),
		client:  client,
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

// InitiatePayment for PayU produces the merchant txnid only. PayU payments
// complete via a client redirect, so there is no synchronous gateway call here;
// the transaction stays PENDING until CheckStatus (verify_payment) or a webhook
// resolves it.
func (a *Adapter) InitiatePayment(ctx context.Context, req ports.GatewayPaymentRequest) (*ports.GatewayPaymentResponse, error) {
	txnid := req.IdempotencyKey
	if txnid == "" {
		txnid = deriveTxnID(req.TransactionID, req.AttemptNumber)
	}
	return &ports.GatewayPaymentResponse{
		GatewayReferenceID: txnid,
		Status:             ports.GatewayPaymentStatusPending,
		Amount:             req.Amount,
		Currency:           strings.ToUpper(req.Currency),
		RawMetadata:        map[string]any{"txnid": txnid},
	}, nil
}

func (a *Adapter) CheckStatus(ctx context.Context, req ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	var out payuVerifyResponse
	if err := a.postService(ctx, "verify_payment", req.GatewayReferenceID, nil, &out); err != nil {
		return nil, err
	}

	detail, ok := out.TransactionDetails[req.GatewayReferenceID]
	if !ok {
		return &ports.GatewayPaymentResponse{
			GatewayReferenceID: req.GatewayReferenceID,
			Status:             ports.GatewayPaymentStatusPending,
		}, nil
	}

	return &ports.GatewayPaymentResponse{
		GatewayReferenceID: req.GatewayReferenceID,
		Status:             mapStatus(detail.Status),
		Amount:             rupeesToPaise(detail.Amount),
		Currency:           "INR",
		RawMetadata:        map[string]any{"mihpayid": detail.MihpayID},
	}, nil
}

func (a *Adapter) Refund(ctx context.Context, req ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	token := req.IdempotencyKey
	if token == "" {
		token = req.RefundID.String()
	}
	amount := fmt.Sprintf("%.2f", float64(req.Amount)/100)

	var out payuRefundResponse
	if err := a.postService(ctx, "cancel_refund_transaction", req.GatewayReferenceID, []string{token, amount}, &out); err != nil {
		return nil, err
	}

	status := ports.GatewayRefundStatusFailed
	if out.Status == 1 {
		status = ports.GatewayRefundStatusProcessing
	}
	return &ports.GatewayRefundResponse{
		GatewayRefundID: out.RequestID,
		Status:          status,
		Amount:          req.Amount,
		Currency:        "INR",
	}, nil
}

func (a *Adapter) Cancel(ctx context.Context, req ports.GatewayCancelRequest) (*ports.GatewayCancelResponse, error) {
	return &ports.GatewayCancelResponse{Status: ports.GatewayCancelStatusNotSupported}, nil
}

func (a *Adapter) postService(ctx context.Context, command, var1 string, extraVars []string, out any) error {
	form := url.Values{}
	form.Set("key", a.key)
	form.Set("command", command)
	form.Set("var1", var1)
	form.Set("hash", a.hash(command, var1))
	for i, v := range extraVars {
		form.Set("var"+strconv.Itoa(i+2), v)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/merchant/postservice?form=2", strings.NewReader(form.Encode()))
	if err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "request_build_error", GatewayMessage: err.Error()}
	}
	httpReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout, Code: "network_error", GatewayMessage: err.Error(), Retryable: true, Underlying: err}
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "gateway_error", GatewayCode: strconv.Itoa(resp.StatusCode), GatewayMessage: string(data)}
	}

	if err := decodeJSON(data, out); err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "decode_error", GatewayMessage: err.Error()}
	}
	return nil
}

func (a *Adapter) hash(command, var1 string) string {
	sum := sha512.Sum512([]byte(a.key + "|" + command + "|" + var1 + "|" + a.salt))
	return hex.EncodeToString(sum[:])
}

func deriveTxnID(id uuid.UUID, attemptNumber int) string {
	raw := id.String()
	if len(raw) > 20 {
		raw = raw[:20]
	}
	return strings.ToUpper(raw + "-" + strconv.Itoa(attemptNumber))
}

func mapStatus(s string) ports.GatewayPaymentStatus {
	switch strings.ToLower(s) {
	case "success", "captured":
		return ports.GatewayPaymentStatusSucceeded
	case "failure", "failed":
		return ports.GatewayPaymentStatusFailed
	case "usercancelled", "cancelled":
		return ports.GatewayPaymentStatusCancelled
	case "pending", "in progress", "initiated":
		return ports.GatewayPaymentStatusProcessing
	default:
		return ports.GatewayPaymentStatusPending
	}
}

func rupeesToPaise(s string) int64 {
	if s == "" {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return int64(f*100 + 0.5)
}

var _ ports.GatewayAdapter = (*Adapter)(nil)
