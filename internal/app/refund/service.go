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

var ErrNotRefundable = errors.New("refund: transaction is not in a refundable state")

type TransactionReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error)
}

type RefundRepo interface {
	LockParentTransaction(ctx context.Context, transactionID uuid.UUID) error
	SumActiveRefunds(ctx context.Context, transactionID uuid.UUID) (int64, error)
	Insert(ctx context.Context, rf *refund.Refund) error
	GetByID(ctx context.Context, id uuid.UUID) (*refund.Refund, error)
	UpdateStatus(ctx context.Context, rf *refund.Refund) error
	ExistsByReason(ctx context.Context, transactionID uuid.UUID, reason string) (bool, error)
}

const ReasonCancelResolution = "cancel_resolution"

type EventWriter interface {
	Write(ctx context.Context, event ports.OutboxEvent) error
}

type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type GatewayRegistry interface {
	Get(gatewayID string) (ports.GatewayAdapter, error)
}

type Service struct {
	txns     TransactionReader
	refunds  RefundRepo
	outbox   EventWriter
	tx       Transactor
	gateways GatewayRegistry
	log      ports.Logger
	metrics  ports.MetricRecorder
}

func NewService(txns TransactionReader, refunds RefundRepo, outbox EventWriter, tx Transactor, gateways GatewayRegistry, log ports.Logger, metrics ports.MetricRecorder) *Service {
	return &Service{txns: txns, refunds: refunds, outbox: outbox, tx: tx, gateways: gateways, log: log, metrics: metrics}
}

type InitiateInput struct {
	TransactionID uuid.UUID
	Amount        int64
	Reason        string
	InitiatedBy   string
}

type refundInitiatedPayload struct {
	RefundID      string `json:"refund_id"`
	TransactionID string `json:"transaction_id"`
	Amount        int64  `json:"amount"`
	Reason        string `json:"reason"`
}

func (s *Service) InitiateRefund(ctx context.Context, in InitiateInput) (*refund.Refund, error) {
	parent, err := s.txns.GetByID(ctx, in.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("refund: load transaction %s: %w", in.TransactionID, err)
	}
	if parent.Status != transaction.StatusSucceeded {
		return nil, ErrNotRefundable
	}

	var rf *refund.Refund
	err = s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := s.refunds.LockParentTransaction(ctx, in.TransactionID); err != nil {
			return err
		}

		alreadyRefunded, err := s.refunds.SumActiveRefunds(ctx, in.TransactionID)
		if err != nil {
			return err
		}

		r, err := refund.New(in.TransactionID, in.Amount, parent.Amount, alreadyRefunded, in.Reason, in.InitiatedBy)
		if err != nil {
			return err
		}
		r.AttemptedGateway = parent.GatewayID

		if err := s.refunds.Insert(ctx, r); err != nil {
			return fmt.Errorf("insert refund: %w", err)
		}

		payload, err := json.Marshal(refundInitiatedPayload{
			RefundID:      r.ID.String(),
			TransactionID: in.TransactionID.String(),
			Amount:        r.Amount,
			Reason:        r.Reason,
		})
		if err != nil {
			return fmt.Errorf("marshal refund event: %w", err)
		}
		if err := s.outbox.Write(ctx, ports.OutboxEvent{
			AggregateID:   r.ID,
			AggregateType: "refund",
			EventType:     ports.EventTypeRefundInitiated,
			Payload:       payload,
			EventVersion:  1,
		}); err != nil {
			return fmt.Errorf("write refund event: %w", err)
		}

		rf = r
		return nil
	})
	if err != nil {
		var over refund.ErrOverRefund
		if errors.As(err, &over) {
			s.metrics.Increment(ports.MetricRefundDuplicationBlocked, map[string]string{"reason": "over_refund"})
			s.log.Warn(ports.LogEventRefundOverRefundBlocked, map[string]any{
				ports.FieldTransactionID: in.TransactionID.String(),
			})
			return nil, over
		}
		return nil, fmt.Errorf("refund: initiate for %s: %w", in.TransactionID, err)
	}

	s.log.Info(ports.LogEventRefundInitiated, map[string]any{
		ports.FieldRefundID:      rf.ID.String(),
		ports.FieldTransactionID: in.TransactionID.String(),
	})
	s.metrics.Increment(ports.MetricRefundInitiated, map[string]string{"gateway_id": rf.AttemptedGateway})
	return rf, nil
}
