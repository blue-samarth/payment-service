package refund

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/app/authz"
	"samarth/payment-service/internal/domain/refund"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type refundTerminalPayload struct {
	RefundID         string `json:"refund_id"`
	TransactionID    string `json:"transaction_id"`
	Status           string `json:"status"`
	GatewayRefundID  string `json:"gateway_refund_id"`
	Amount           int64  `json:"amount"`
	AggregateVersion int    `json:"aggregate_version"`
}

type refundOutcome struct {
	terminal      bool
	newStatus     refund.Status
	failureReason *refund.FailureReason
}

func (s *Service) ProcessRefund(ctx context.Context, refundID uuid.UUID) (*refund.Refund, error) {
	rf, err := s.refunds.GetByID(ctx, refundID)
	if err != nil {
		return nil, fmt.Errorf("refund: load refund %s: %w", refundID, err)
	}
	if rf.Status != refund.StatusInitiated {
		return rf, nil
	}

	parent, err := s.txns.GetByID(ctx, rf.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("refund: load parent %s: %w", rf.TransactionID, err)
	}

	adapter, err := s.gateways.Get(rf.AttemptedGateway)
	if err != nil {
		return nil, fmt.Errorf("refund: resolve adapter for %s: %w", rf.AttemptedGateway, err)
	}

	if err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := rf.Transition(refund.StatusProcessing); err != nil {
			return err
		}
		rf.Attempts++
		return s.refunds.UpdateStatus(ctx, rf)
	}); err != nil {
		return nil, fmt.Errorf("refund: begin processing %s: %w", refundID, err)
	}

	resp, gwErr := s.callGateway(ctx, adapter, rf, parent)
	outcome := resolveRefundOutcome(resp, gwErr)

	if resp != nil {
		rf.GatewayRefundID = resp.GatewayRefundID
	}
	rf.ActualGateway = rf.AttemptedGateway

	if !outcome.terminal {
		if err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
			return s.refunds.UpdateStatus(ctx, rf)
		}); err != nil {
			return nil, fmt.Errorf("refund: persist in-flight %s: %w", refundID, err)
		}
		if gwErr != nil && gwErr.Category == ports.ErrorCategoryAmbiguous {
			s.flagForReview(ctx, rf, gwErr.Code, gwErr)
		}
		return rf, nil
	}

	return s.finalize(ctx, rf, outcome)
}

func (s *Service) RecoverRefund(ctx context.Context, refundID uuid.UUID) (*refund.Refund, error) {
	rf, err := s.refunds.GetByID(ctx, refundID)
	if err != nil {
		return nil, fmt.Errorf("refund: load refund %s: %w", refundID, err)
	}

	pastCutOff := s.recoveryMaxAge > 0 && time.Since(rf.InitiatedAt) > s.recoveryMaxAge
	if pastCutOff && rf.Status == refund.StatusInitiated {
		s.flagForReview(ctx, rf, "initiated_too_long", nil)
		return rf, nil
	}

	got, err := s.recoverRefund(ctx, rf)
	if !pastCutOff || ctx.Err() != nil {
		return got, err
	}
	if err != nil {
		s.flagForReview(ctx, rf, "recovery_stalled", err)
		return rf, nil
	}
	if !got.Status.IsTerminal() {
		s.flagForReview(ctx, got, "pending_too_long", nil)
	}
	return got, nil
}

func (s *Service) recoverRefund(ctx context.Context, rf *refund.Refund) (*refund.Refund, error) {
	refundID := rf.ID
	switch rf.Status {
	case refund.StatusInitiated:
		return s.ProcessRefund(ctx, refundID)
	case refund.StatusProcessing:
	default:
		return rf, nil
	}

	adapter, err := s.gateways.Get(rf.AttemptedGateway)
	if err != nil {
		return nil, fmt.Errorf("refund: resolve adapter for %s: %w", rf.AttemptedGateway, err)
	}
	checker, ok := adapter.(ports.RefundStatusChecker)
	if !ok {
		s.flagForReview(ctx, rf, "status_check_unsupported", nil)
		return rf, nil
	}
	parent, err := s.txns.GetByID(ctx, rf.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("refund: load parent %s: %w", rf.TransactionID, err)
	}

	resp, err := checker.RefundStatus(ctx, ports.GatewayRefundStatusRequest{
		RefundID:           rf.ID,
		GatewayRefundID:    rf.GatewayRefundID,
		GatewayReferenceID: parent.GatewayReferenceID,
	})
	if err != nil {
		return nil, fmt.Errorf("refund: status check %s: %w", refundID, err)
	}

	switch resp.Status {
	case ports.GatewayRefundStatusCompleted, ports.GatewayRefundStatusFailed:
		if resp.GatewayRefundID != "" {
			rf.GatewayRefundID = resp.GatewayRefundID
		}
		rf.ActualGateway = rf.AttemptedGateway
		return s.finalize(ctx, rf, resolveRefundOutcome(resp, nil))
	case ports.GatewayRefundStatusNotFound:
		s.flagForReview(ctx, rf, "not_found_at_gateway", nil)
		return rf, nil
	case ports.GatewayRefundStatusUnknown:
		s.flagForReview(ctx, rf, "status_unknown", nil)
		return rf, nil
	}

	if resp.GatewayRefundID != "" && resp.GatewayRefundID != rf.GatewayRefundID {
		rf.GatewayRefundID = resp.GatewayRefundID
		if err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
			return s.refunds.UpdateStatus(ctx, rf)
		}); err != nil {
			return nil, fmt.Errorf("refund: persist gateway refund id %s: %w", refundID, err)
		}
	}
	return rf, nil
}

func (s *Service) finalize(ctx context.Context, rf *refund.Refund, outcome refundOutcome) (*refund.Refund, error) {
	if err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := rf.Transition(outcome.newStatus); err != nil {
			return err
		}
		rf.FailureReason = outcome.failureReason
		if err := s.refunds.UpdateStatus(ctx, rf); err != nil {
			return err
		}
		event, err := s.buildTerminalEvent(rf, outcome.newStatus)
		if err != nil {
			return err
		}
		return s.outbox.Write(ctx, event)
	}); err != nil {
		return nil, fmt.Errorf("refund: finalize %s: %w", rf.ID, err)
	}

	if outcome.newStatus == refund.StatusRefunded {
		s.log.Info(ports.LogEventRefundSucceeded, map[string]any{ports.FieldRefundID: rf.ID.String()})
		s.metrics.Increment(ports.MetricRefundSucceeded, map[string]string{"gateway_id": rf.ActualGateway})
	} else {
		s.log.Info(ports.LogEventRefundFailed, map[string]any{ports.FieldRefundID: rf.ID.String()})
		s.metrics.Increment(ports.MetricRefundFailed, map[string]string{"gateway_id": rf.ActualGateway})
	}
	return rf, nil
}

func (s *Service) flagForReview(ctx context.Context, rf *refund.Refund, code string, cause error) {
	marked, err := s.refunds.MarkReviewRequired(ctx, rf.ID, code)
	if err != nil {
		s.log.Error(ports.LogEventRefundReviewRequired, map[string]any{
			ports.FieldErrorCode:     "mark_review_failed",
			ports.FieldTraceID:       "",
			ports.FieldTransactionID: rf.TransactionID.String(),
			ports.FieldRefundID:      rf.ID.String(),
		}, err)
		return
	}
	if !marked {
		return
	}
	if cause == nil {
		cause = errors.New("refund outcome cannot be determined automatically")
	}
	s.metrics.Increment(ports.MetricRefundReviewRequired, map[string]string{
		"gateway_id": rf.AttemptedGateway,
		"code":       code,
	})
	s.log.Error(ports.LogEventRefundReviewRequired, map[string]any{
		ports.FieldErrorCode:     code,
		ports.FieldTraceID:       "",
		ports.FieldTransactionID: rf.TransactionID.String(),
		ports.FieldRefundID:      rf.ID.String(),
		ports.FieldGatewayID:     rf.AttemptedGateway,
	}, cause)
}

func (s *Service) ResolveCancelRefund(ctx context.Context, transactionID uuid.UUID, amount int64) error {
	exists, err := s.refunds.ExistsByReason(ctx, transactionID, ReasonCancelResolution)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	res, err := s.InitiateRefund(ctx, InitiateInput{
		TransactionID:  transactionID,
		Scope:          authz.AllMerchants(),
		Amount:         amount,
		Reason:         ReasonCancelResolution,
		InitiatedBy:    "system:cancel_resolution",
		IdempotencyKey: "cancel-resolution:" + transactionID.String(),
	})
	if err != nil {
		var over refund.ErrOverRefund
		if errors.As(err, &over) {
			return nil
		}
		return fmt.Errorf("refund: cancel resolution for %s: %w", transactionID, err)
	}
	if res.Refund == nil {
		return nil
	}

	if _, err := s.ProcessRefund(ctx, res.Refund.ID); err != nil {
		return fmt.Errorf("refund: process cancel resolution %s: %w", res.Refund.ID, err)
	}
	return nil
}

func (s *Service) callGateway(ctx context.Context, adapter ports.GatewayAdapter, rf *refund.Refund, parent *transaction.Transaction) (*ports.GatewayRefundResponse, *ports.GatewayError) {
	resp, err := adapter.Refund(ctx, ports.GatewayRefundRequest{
		RefundID:           rf.ID,
		TransactionID:      rf.TransactionID,
		GatewayReferenceID: parent.GatewayReferenceID,
		Amount:             rf.Amount,
		Currency:           parent.Currency,
		Reason:             rf.Reason,
	})
	if err != nil {
		var gwErr *ports.GatewayError
		if errors.As(err, &gwErr) {
			return nil, gwErr
		}
		return nil, &ports.GatewayError{
			Category:       ports.ErrorCategoryGatewayError,
			Code:           "refund_call_failed",
			GatewayMessage: err.Error(),
			Underlying:     err,
		}
	}
	return resp, nil
}

func resolveRefundOutcome(resp *ports.GatewayRefundResponse, gwErr *ports.GatewayError) refundOutcome {
	if gwErr != nil {
		switch gwErr.Category {
		case ports.ErrorCategoryAmbiguous, ports.ErrorCategoryNetworkTimeout, ports.ErrorCategoryGatewayError:
			return refundOutcome{terminal: false}
		}
		return refundOutcome{
			terminal:  true,
			newStatus: refund.StatusFailed,
			failureReason: &refund.FailureReason{
				Category:       string(gwErr.Category),
				Code:           gwErr.Code,
				GatewayCode:    gwErr.GatewayCode,
				GatewayMessage: gwErr.GatewayMessage,
				Source:         refund.FailureReasonSourceGateway,
			},
		}
	}

	switch resp.Status {
	case ports.GatewayRefundStatusCompleted:
		return refundOutcome{terminal: true, newStatus: refund.StatusRefunded}
	case ports.GatewayRefundStatusFailed:
		return refundOutcome{
			terminal:  true,
			newStatus: refund.StatusFailed,
			failureReason: &refund.FailureReason{
				Category: "gateway_refund_failed",
				Source:   refund.FailureReasonSourceGateway,
			},
		}
	default:
		return refundOutcome{terminal: false}
	}
}

func (s *Service) buildTerminalEvent(rf *refund.Refund, status refund.Status) (ports.OutboxEvent, error) {
	eventType := ports.EventTypeRefundFailed
	if status == refund.StatusRefunded {
		eventType = ports.EventTypeRefundSucceeded
	}

	payload, err := json.Marshal(refundTerminalPayload{
		RefundID:         rf.ID.String(),
		TransactionID:    rf.TransactionID.String(),
		Status:           string(status),
		GatewayRefundID:  rf.GatewayRefundID,
		Amount:           rf.Amount,
		AggregateVersion: rf.Version,
	})
	if err != nil {
		return ports.OutboxEvent{}, fmt.Errorf("refund: marshal terminal event: %w", err)
	}

	return ports.OutboxEvent{
		AggregateID:      rf.ID,
		AggregateType:    "refund",
		EventType:        eventType,
		Payload:          payload,
		EventVersion:     1,
		AggregateVersion: rf.Version,
	}, nil
}
