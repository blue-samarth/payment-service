package refund

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"

	"samarth/payment-service/internal/app/idempotency"
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
	idem     *idempotency.Guard
	log      ports.Logger
	metrics  ports.MetricRecorder
}

func NewService(txns TransactionReader, refunds RefundRepo, outbox EventWriter, tx Transactor, gateways GatewayRegistry, log ports.Logger, metrics ports.MetricRecorder) *Service {
	return &Service{txns: txns, refunds: refunds, outbox: outbox, tx: tx, gateways: gateways, log: log, metrics: metrics}
}

func (s *Service) SetIdempotency(g *idempotency.Guard) { s.idem = g }

type InitiateInput struct {
	TransactionID  uuid.UUID
	Amount         int64
	Reason         string
	InitiatedBy    string
	IdempotencyKey string
}

type InitiateResult struct {
	Verdict idempotency.Verdict
	Refund  *refund.Refund
}

type idempotencyRefundResponse struct {
	RefundID string `json:"refund_id"`
}

type refundInitiatedPayload struct {
	RefundID      string `json:"refund_id"`
	TransactionID string `json:"transaction_id"`
	Amount        int64  `json:"amount"`
	Reason        string `json:"reason"`
}

func (s *Service) InitiateRefund(ctx context.Context, in InitiateInput) (InitiateResult, error) {
	parent, err := s.txns.GetByID(ctx, in.TransactionID)
	if err != nil {
		return InitiateResult{}, fmt.Errorf("refund: load transaction %s: %w", in.TransactionID, err)
	}
	if parent.Status != transaction.StatusSucceeded {
		return InitiateResult{}, ErrNotRefundable
	}

	if s.idem == nil {
		var rf *refund.Refund
		err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
			r, err := s.insertRefund(ctx, in, parent)
			if err != nil {
				return err
			}
			rf = r
			return nil
		})
		if err != nil {
			return InitiateResult{}, s.mapInitiateError(in, err)
		}
		s.logInitiated(rf, in)
		return InitiateResult{Verdict: idempotency.Created, Refund: rf}, nil
	}

	if in.IdempotencyKey == "" {
		return InitiateResult{}, idempotency.ErrKeyRequired
	}
	composite := idempotency.Composite(parent.MerchantID.String(), "initiate_refund", in.IdempotencyKey)
	requestHash := idempotency.RequestHash(
		in.TransactionID.String(), strconv.FormatInt(in.Amount, 10), in.Reason, in.InitiatedBy,
	)

	var created *refund.Refund
	res, err := s.idem.Execute(ctx, composite, requestHash, func(ctx context.Context) ([]byte, error) {
		r, err := s.insertRefund(ctx, in, parent)
		if err != nil {
			return nil, err
		}
		created = r
		return json.Marshal(idempotencyRefundResponse{RefundID: r.ID.String()})
	})
	if err != nil {
		return InitiateResult{}, s.mapInitiateError(in, err)
	}

	switch res.Verdict {
	case idempotency.Created:
		s.logInitiated(created, in)
		return InitiateResult{Verdict: res.Verdict, Refund: created}, nil
	case idempotency.Replayed:
		var stored idempotencyRefundResponse
		if err := json.Unmarshal(res.Response, &stored); err != nil {
			return InitiateResult{}, fmt.Errorf("refund: decode idempotent response: %w", err)
		}
		id, err := uuid.Parse(stored.RefundID)
		if err != nil {
			return InitiateResult{}, fmt.Errorf("refund: bad stored refund id: %w", err)
		}
		rf, err := s.refunds.GetByID(ctx, id)
		if err != nil {
			return InitiateResult{}, fmt.Errorf("refund: reload idempotent refund %s: %w", id, err)
		}
		return InitiateResult{Verdict: res.Verdict, Refund: rf}, nil
	default:
		return InitiateResult{Verdict: res.Verdict}, nil
	}
}

func (s *Service) insertRefund(ctx context.Context, in InitiateInput, parent *transaction.Transaction) (*refund.Refund, error) {
	if err := s.refunds.LockParentTransaction(ctx, in.TransactionID); err != nil {
		return nil, err
	}

	alreadyRefunded, err := s.refunds.SumActiveRefunds(ctx, in.TransactionID)
	if err != nil {
		return nil, err
	}

	r, err := refund.New(in.TransactionID, in.Amount, parent.Amount, alreadyRefunded, in.Reason, in.InitiatedBy)
	if err != nil {
		return nil, err
	}
	r.AttemptedGateway = parent.GatewayID

	if err := s.refunds.Insert(ctx, r); err != nil {
		return nil, fmt.Errorf("insert refund: %w", err)
	}

	payload, err := json.Marshal(refundInitiatedPayload{
		RefundID:      r.ID.String(),
		TransactionID: in.TransactionID.String(),
		Amount:        r.Amount,
		Reason:        r.Reason,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal refund event: %w", err)
	}
	if err := s.outbox.Write(ctx, ports.OutboxEvent{
		AggregateID:   r.ID,
		AggregateType: "refund",
		EventType:     ports.EventTypeRefundInitiated,
		Payload:       payload,
		EventVersion:  1,
	}); err != nil {
		return nil, fmt.Errorf("write refund event: %w", err)
	}
	return r, nil
}

func (s *Service) mapInitiateError(in InitiateInput, err error) error {
	var over refund.ErrOverRefund
	if errors.As(err, &over) {
		s.metrics.Increment(ports.MetricRefundDuplicationBlocked, map[string]string{"reason": "over_refund"})
		s.log.Warn(ports.LogEventRefundOverRefundBlocked, map[string]any{
			ports.FieldTransactionID: in.TransactionID.String(),
		})
		return over
	}
	return fmt.Errorf("refund: initiate for %s: %w", in.TransactionID, err)
}

func (s *Service) logInitiated(rf *refund.Refund, in InitiateInput) {
	s.log.Info(ports.LogEventRefundInitiated, map[string]any{
		ports.FieldRefundID:      rf.ID.String(),
		ports.FieldTransactionID: in.TransactionID.String(),
	})
	s.metrics.Increment(ports.MetricRefundInitiated, map[string]string{"gateway_id": rf.AttemptedGateway})
}
