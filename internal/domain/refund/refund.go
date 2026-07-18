package refund

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Status string
type FailureReasonSource string

const (
	FailureReasonSourceGateway  FailureReasonSource = "gateway"
	FailureReasonSourceInternal FailureReasonSource = "internal"
	FailureReasonSourceTimeout  FailureReasonSource = "timeout"
)

type FailureReason struct {
	Category       string              `json:"category"`
	Code           string              `json:"code"`
	GatewayCode    string              `json:"gateway_code"`
	GatewayMessage string              `json:"gateway_message"`
	Source         FailureReasonSource `json:"source"`
}

type ErrInvalidTransition struct {
	From Status
	To   Status
}

type ErrOverRefund struct {
	OriginalAmount  int64
	AlreadyRefunded int64
	Requested       int64
}

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("refund: invalid state transition %s → %s", e.From, e.To)
}

func (e ErrOverRefund) Error() string {
	return fmt.Sprintf(
		"refund: amount %d would exceed original %d (already refunded: %d)",
		e.Requested, e.OriginalAmount, e.AlreadyRefunded,
	)
}

type Refund struct {
	ID               uuid.UUID
	TransactionID    uuid.UUID
	Amount           int64
	Reason           string
	Status           Status
	InitiatedBy      string
	GatewayRefundID  string
	AttemptedGateway string
	ActualGateway    string
	Attempts         int
	FailureReason    *FailureReason
	InitiatedAt      time.Time
	ResolvedAt       *time.Time
	Version          int
}

const (
	StatusInitiated  Status = "REFUND_INITIATED"
	StatusProcessing Status = "REFUND_PROCESSING"
	StatusRefunded   Status = "REFUNDED"
	StatusFailed     Status = "REFUND_FAILED"
)

func (s Status) IsTerminal() bool {
	return s == StatusRefunded || s == StatusFailed
}

var transitionTable = map[Status][]Status{
	StatusInitiated:  {StatusProcessing, StatusFailed},
	StatusProcessing: {StatusRefunded, StatusFailed},
	StatusFailed:     {},
	StatusRefunded:   {},
}

func New(
	transactionID uuid.UUID,
	amount int64,
	originalAmount int64,
	alreadyRefunded int64,
	reason string,
	initiatedBy string,
) (*Refund, error) {
	if transactionID == uuid.Nil {
		return nil, fmt.Errorf("refund: transactionID must not be nil")
	}
	if amount <= 0 {
		return nil, fmt.Errorf("refund: amount must be > 0, got %d", amount)
	}
	if reason == "" {
		return nil, fmt.Errorf("refund: reason must not be empty")
	}
	if initiatedBy == "" {
		return nil, fmt.Errorf("refund: initiatedBy must not be empty")
	}
	if originalAmount <= 0 {
		return nil, fmt.Errorf("refund: originalAmount must be > 0, got %d", originalAmount)
	}
	if alreadyRefunded < 0 {
		return nil, fmt.Errorf("refund: alreadyRefunded must be >= 0, got %d", alreadyRefunded)
	}
	if alreadyRefunded > originalAmount {
		return nil, fmt.Errorf("refund: alreadyRefunded %d exceeds originalAmount %d", alreadyRefunded, originalAmount)
	}
	if alreadyRefunded+amount > originalAmount {
		return nil, ErrOverRefund{
			OriginalAmount:  originalAmount,
			AlreadyRefunded: alreadyRefunded,
			Requested:       amount,
		}
	}

	return &Refund{
		ID:            uuid.New(),
		TransactionID: transactionID,
		Amount:        amount,
		Reason:        reason,
		Status:        StatusInitiated,
		InitiatedBy:   initiatedBy,
		Attempts:      0,
		InitiatedAt:   time.Now().UTC(),
		Version:       1,
	}, nil
}

func (r *Refund) Transition(toState Status) error {
	allowed, ok := transitionTable[r.Status]
	if !ok {
		return ErrInvalidTransition{From: r.Status, To: toState}
	}
	for _, s := range allowed {
		if s == toState {
			r.Status = toState
			if toState.IsTerminal() {
				now := time.Now().UTC()
				r.ResolvedAt = &now
			}
			return nil
		}
	}
	return ErrInvalidTransition{From: r.Status, To: toState}
}

func (r *Refund) IsRetryable() bool {
	return r.Status == StatusFailed
}
