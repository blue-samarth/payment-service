package payment

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

var (
	ErrNotUnderReview    = errors.New("payment: transaction is not awaiting review")
	ErrInvalidResolution = errors.New("payment: invalid review resolution")
)

type ResolveInput struct {
	Status             transaction.Status
	GatewayReferenceID string
	Note               string
}

func (s *Service) ResolveReview(ctx context.Context, transactionID uuid.UUID, in ResolveInput) (*transaction.Transaction, error) {
	switch in.Status {
	case transaction.StatusSucceeded:
		if in.GatewayReferenceID == "" {
			return nil, fmt.Errorf("%w: a SUCCEEDED resolution needs the gateway reference so the payment can be refunded", ErrInvalidResolution)
		}
	case transaction.StatusFailed:
	default:
		return nil, fmt.Errorf("%w: status must be SUCCEEDED or FAILED", ErrInvalidResolution)
	}

	txn, err := s.repo.GetByID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("payment: load transaction %s: %w", transactionID, err)
	}
	if txn.Status != transaction.StatusProcessing || txn.ReviewRequiredAt == nil {
		return nil, ErrNotUnderReview
	}

	if txn.ActualGateway != "" {
		txn.GatewayID = txn.ActualGateway
	}
	reference := in.GatewayReferenceID
	if reference == "" {
		reference = txn.GatewayReferenceID
	}
	result := outcome{terminal: true, newStatus: in.Status}
	if in.Status == transaction.StatusFailed {
		result.failureReason = &transaction.FailureReason{
			Category:       "manual_review",
			Code:           "resolved_by_ops",
			GatewayMessage: in.Note,
			Source:         transaction.FailureReasonSourceInternal,
		}
	}

	resolved, err := s.finalize(ctx, txn, &ports.GatewayPaymentResponse{GatewayReferenceID: reference}, result)
	if err != nil {
		current, getErr := s.repo.GetByID(context.WithoutCancel(ctx), transactionID)
		if getErr == nil && current.Status != transaction.StatusProcessing {
			return nil, ErrNotUnderReview
		}
		return nil, err
	}
	s.log.Info(ports.LogEventTransactionReviewResolved, map[string]any{
		ports.FieldTransactionID: transactionID.String(),
		ports.FieldGatewayID:     txn.GatewayID,
		ports.FieldNewState:      string(in.Status),
		"review_reason":          txn.ReviewReason,
	})
	return resolved, nil
}
