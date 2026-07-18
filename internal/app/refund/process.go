package refund

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

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
		return rf, nil
	}

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
		return nil, fmt.Errorf("refund: finalize %s: %w", refundID, err)
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
