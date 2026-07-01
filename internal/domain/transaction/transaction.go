package transaction

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Status string
type PaymentMethod string
type Actor string
type CancelVia string
type FailureReasonSource string

type FailureReason struct {
	Category       string              `json:"category"`
	Code           string              `json:"code"`
	GatewayCode    string              `json:"gateway_code"`
	GatewayMessage string              `json:"gateway_message"`
	Source         FailureReasonSource `json:"source"`
}

type CardDetails struct {
	CardBrand string  `json:"card_brand"`
	Last4     string  `json:"last4"`
	Network   string  `json:"network"`
	RiskScore float64 `json:"risk_score"`
	AuthCode  string  `json:"auth_code"`
}

type UPIDetails struct {
	VPA              string `json:"vpa"`
	UPITransactionID string `json:"upi_transaction_id"`
	PayerBank        string `json:"payer_bank"`
}

type NetbankingDetails struct {
	BankCode        string `json:"bank_code"`
	BankReferenceID string `json:"bank_reference_id"`
}

type WalletDetails struct {
	WalletProvider      string `json:"wallet_provider"`
	WalletTransactionID string `json:"wallet_transaction_id"`
}

type MethodDetails struct {
	Card       *CardDetails       `json:"card,omitempty"`
	UPI        *UPIDetails        `json:"upi,omitempty"`
	Netbanking *NetbankingDetails `json:"netbanking,omitempty"`
	Wallet     *WalletDetails     `json:"wallet,omitempty"`
}

type Transaction struct {
	ID                      uuid.UUID
	MerchantID              uuid.UUID
	Amount                  int64
	Currency                string
	PaymentMethod           PaymentMethod
	Status                  Status
	Version                 int // optimistic locking — repo rejects writes where stored version != expected
	GatewayID               string
	GatewayReferenceID      string
	GatewayIdempotencyKey   string
	AttemptedGateway        string
	ActualGateway           string
	OriginalGateway         string
	EstimatedTimeoutSeconds int
	FailureReason           *FailureReason
	MethodDetails           *MethodDetails
	Metadata                map[string]any
	Description             string
	CustomerID              uuid.UUID
	CustomerEmail           string
	CancelIntent            bool
	CancelRequestedBy       Actor
	CancelRequestedAt       *time.Time
	CancelRequestedVia      CancelVia
	ProcessingStartedAt     *time.Time
	ProcessingTimeout       *time.Duration
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

const (
	StatusPending      Status = "PENDING"
	StatusProcessing   Status = "PROCESSING"
	StatusSucceeded    Status = "SUCCEEDED"
	StatusFailed       Status = "FAILED"
	StatusCancelled    Status = "CANCELLED"
	StatusRefunded     Status = "REFUNDED"
	StatusRefundFailed Status = "REFUND_FAILED"
)

const (
	PaymentMethodCard       PaymentMethod = "card"
	PaymentMethodUPI        PaymentMethod = "upi"
	PaymentMethodNetbanking PaymentMethod = "netbanking"
	PaymentMethodWallet     PaymentMethod = "wallet"
)

const (
	ActorSystem   Actor = "system"
	ActorMerchant Actor = "merchant"
	ActorOps      Actor = "ops"
	ActorGateway  Actor = "gateway"
)

const (
	CancelViaAPI       CancelVia = "api"
	CancelViaDashboard CancelVia = "dashboard"
	CancelViaOpsTool   CancelVia = "ops-tool"
)

const (
	FailureReasonSourceGateway  FailureReasonSource = "gateway"
	FailureReasonSourceInternal FailureReasonSource = "internal"
	FailureReasonSourceTimeout  FailureReasonSource = "timeout"
)

var validPaymentMethods = map[PaymentMethod]struct{}{
	PaymentMethodCard:       {},
	PaymentMethodUPI:        {},
	PaymentMethodNetbanking: {},
	PaymentMethodWallet:     {},
}

func (s Status) IsTerminal() bool {
	switch s {
	case StatusSucceeded, StatusCancelled, StatusRefunded, StatusRefundFailed:
		return true
	}
	return false
}

func New(
	merchantID uuid.UUID,
	amount int64,
	currency string,
	method PaymentMethod,
	gatewayID string,
	customerID uuid.UUID,
	customerEmail string,
	description string,
	metadata map[string]any,
	estimatedTimeoutSec int,
) (*Transaction, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("amount must be positive, got %d", amount)
	}
	if len(currency) != 3 || currency != strings.ToUpper(currency) {
		return nil, fmt.Errorf("currency must be ISO 4217 uppercase 3-letter code, got %q", currency)
	}
	if _, ok := validPaymentMethods[method]; !ok {
		return nil, fmt.Errorf("invalid payment method %q", method)
	}
	if merchantID == uuid.Nil {
		return nil, fmt.Errorf("merchantID must not be nil")
	}
	if gatewayID == "" {
		return nil, fmt.Errorf("gatewayID must not be empty")
	}
	if estimatedTimeoutSec <= 0 {
		return nil, fmt.Errorf("estimatedTimeoutSec must be positive, got %d", estimatedTimeoutSec)
	}

	now := time.Now().UTC()
	return &Transaction{
		ID:                      uuid.New(),
		MerchantID:              merchantID,
		Amount:                  amount,
		Currency:                currency,
		PaymentMethod:           method,
		Status:                  StatusPending,
		Version:                 1,
		GatewayID:               gatewayID,
		EstimatedTimeoutSeconds: estimatedTimeoutSec,
		CustomerID:              customerID,
		CustomerEmail:           customerEmail,
		Description:             description,
		Metadata:                metadata,
		CreatedAt:               now,
		UpdatedAt:               now,
	}, nil
}

func (t *Transaction) SetCancelIntent(by Actor, via CancelVia) {
	now := time.Now().UTC()
	t.CancelIntent = true
	t.CancelRequestedBy = by
	t.CancelRequestedAt = &now
	t.CancelRequestedVia = via
	t.UpdatedAt = now
}

func (t *Transaction) IsLeaseExpired() bool {
	if t.Status != StatusProcessing {
		return false
	}
	if t.ProcessingStartedAt == nil || t.ProcessingTimeout == nil {
		return false
	}
	return time.Now().UTC().After(t.ProcessingStartedAt.Add(*t.ProcessingTimeout))
}

func (t *Transaction) HasGatewayDiscrepancy() bool {
	return t.ActualGateway != "" && t.AttemptedGateway != t.ActualGateway
}

func (t *Transaction) Validate() error {
	if t.ID == uuid.Nil {
		return fmt.Errorf("transaction ID must not be nil")
	}
	if t.MerchantID == uuid.Nil {
		return fmt.Errorf("merchantID must not be nil")
	}
	if t.Amount <= 0 {
		return fmt.Errorf("amount must be positive, got %d", t.Amount)
	}
	if len(t.Currency) != 3 || t.Currency != strings.ToUpper(t.Currency) {
		return fmt.Errorf("currency must be ISO 4217 uppercase 3-letter code, got %q", t.Currency)
	}
	if _, ok := validPaymentMethods[t.PaymentMethod]; !ok {
		return fmt.Errorf("invalid payment method %q", t.PaymentMethod)
	}
	if t.GatewayID == "" {
		return fmt.Errorf("gatewayID must not be empty")
	}
	if t.EstimatedTimeoutSeconds <= 0 {
		return fmt.Errorf("estimatedTimeoutSeconds must be positive, got %d", t.EstimatedTimeoutSeconds)
	}
	return nil
}
