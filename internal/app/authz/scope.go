package authz

import (
	"fmt"

	"github.com/google/uuid"

	"samarth/payment-service/internal/ports"
)

var ErrDenied = fmt.Errorf("authz: outside the caller's scope: %w", ports.ErrNotFound)

type Scope struct {
	merchant uuid.UUID
	all      bool
}

func Merchant(id uuid.UUID) Scope { return Scope{merchant: id} }

func AllMerchants() Scope { return Scope{all: true} }

func (s Scope) Allows(owner uuid.UUID) bool {
	return s.all || (s.merchant != uuid.Nil && owner == s.merchant)
}

func (s Scope) String() string {
	switch {
	case s.all:
		return "all"
	case s.merchant != uuid.Nil:
		return s.merchant.String()
	default:
		return "none"
	}
}

func Check(log ports.Logger, metrics ports.MetricRecorder, operation string, s Scope, owner, transactionID uuid.UUID) error {
	if s.Allows(owner) {
		return nil
	}
	fields := map[string]any{
		"operation":         operation,
		"scope":             s.String(),
		"owner_merchant_id": owner.String(),
	}
	if transactionID != uuid.Nil {
		fields[ports.FieldTransactionID] = transactionID.String()
	}
	if log != nil {
		log.Warn(ports.LogEventAuthzDenied, fields)
	}
	if metrics != nil {
		metrics.Increment(ports.MetricAuthzDenied, map[string]string{"operation": operation})
	}
	return ErrDenied
}
