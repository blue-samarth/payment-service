package webhook

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type Event struct {
	EventID            string
	GatewayReferenceID string
	Status             string
}

type Outcome struct {
	Duplicate  bool
	UnknownTxn bool
	Resolved   bool
	Status     transaction.Status
}

type TransactionRepo interface {
	GetByGatewayReference(ctx context.Context, gatewayID, reference string) (*transaction.Transaction, error)
	UpdateStatus(ctx context.Context, t *transaction.Transaction) error
}

type WebhookRepo interface {
	RecordEvent(ctx context.Context, eventID, gatewayID string) (bool, error)
	InsertRawMetadata(ctx context.Context, transactionID uuid.UUID, gatewayID string, payload []byte) error
}

type EventWriter interface {
	Write(ctx context.Context, event ports.OutboxEvent) error
}

type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type Service struct {
	txns     TransactionRepo
	webhooks WebhookRepo
	outbox   EventWriter
	tx       Transactor
	log      ports.Logger
	metrics  ports.MetricRecorder
}

func NewService(txns TransactionRepo, webhooks WebhookRepo, outbox EventWriter, tx Transactor, log ports.Logger, metrics ports.MetricRecorder) *Service {
	return &Service{txns: txns, webhooks: webhooks, outbox: outbox, tx: tx, log: log, metrics: metrics}
}

type webhookEventPayload struct {
	TransactionID    string `json:"transaction_id"`
	Status           string `json:"status"`
	Gateway          string `json:"gateway"`
	Source           string `json:"source"`
	AggregateVersion int    `json:"aggregate_version"`
}

func (s *Service) Process(ctx context.Context, gatewayID string, ev Event, rawPayload []byte) (Outcome, error) {
	var outcome Outcome
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		recorded, err := s.webhooks.RecordEvent(ctx, ev.EventID, gatewayID)
		if err != nil {
			return err
		}
		if !recorded {
			outcome.Duplicate = true
			return nil
		}

		txn, err := s.txns.GetByGatewayReference(ctx, gatewayID, ev.GatewayReferenceID)
		if err != nil {
			return fmt.Errorf("lookup transaction: %w", err)
		}
		if txn == nil {
			outcome.UnknownTxn = true
			return nil
		}

		if err := s.webhooks.InsertRawMetadata(ctx, txn.ID, gatewayID, rawPayload); err != nil {
			return err
		}

		newStatus, terminal := mapWebhookStatus(ev.Status)
		if !terminal || txn.Status != transaction.StatusProcessing {
			outcome.Status = txn.Status
			return nil
		}

		if err := transaction.TransitionState(txn, newStatus, transaction.ActorGateway); err != nil {
			return err
		}
		if newStatus == transaction.StatusFailed {
			txn.FailureReason = &transaction.FailureReason{
				Category: "gateway_webhook_failed",
				Source:   transaction.FailureReasonSourceGateway,
			}
		}
		if err := s.txns.UpdateStatus(ctx, txn); err != nil {
			return err
		}

		event, err := s.buildEvent(txn, newStatus, gatewayID)
		if err != nil {
			return err
		}
		if err := s.outbox.Write(ctx, event); err != nil {
			return err
		}

		outcome.Resolved = true
		outcome.Status = newStatus
		return nil
	})
	if err != nil {
		return Outcome{}, fmt.Errorf("webhook: process %s/%s: %w", gatewayID, ev.EventID, err)
	}

	switch {
	case outcome.Duplicate:
		s.log.Info(ports.LogEventWebhookInboundDuplicate, map[string]any{ports.FieldGatewayID: gatewayID})
	case outcome.Resolved:
		s.log.Info(ports.LogEventWebhookInboundReceived, map[string]any{
			ports.FieldGatewayID: gatewayID,
			ports.FieldNewState:  string(outcome.Status),
		})
	}
	return outcome, nil
}

func (s *Service) buildEvent(txn *transaction.Transaction, status transaction.Status, gatewayID string) (ports.OutboxEvent, error) {
	eventType := ports.EventTypePaymentFailed
	if status == transaction.StatusSucceeded {
		eventType = ports.EventTypePaymentSucceeded
	}
	payload, err := json.Marshal(webhookEventPayload{
		TransactionID:    txn.ID.String(),
		Status:           string(status),
		Gateway:          gatewayID,
		Source:           "webhook",
		AggregateVersion: txn.Version,
	})
	if err != nil {
		return ports.OutboxEvent{}, fmt.Errorf("marshal webhook event: %w", err)
	}
	return ports.OutboxEvent{
		AggregateID:      txn.ID,
		AggregateType:    "transaction",
		EventType:        eventType,
		Payload:          payload,
		EventVersion:     1,
		AggregateVersion: txn.Version,
	}, nil
}

func mapWebhookStatus(s string) (transaction.Status, bool) {
	switch s {
	case "succeeded", "success", "captured", "paid":
		return transaction.StatusSucceeded, true
	case "failed", "failure":
		return transaction.StatusFailed, true
	default:
		return "", false
	}
}
