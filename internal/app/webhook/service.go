package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type Event struct {
	EventID            string
	GatewayReferenceID string
	TransactionID      string
	Status             string
	Amount             int64
	Currency           string
}

type Outcome struct {
	Duplicate  bool
	UnknownTxn bool
	Resolved   bool
	Mismatch   bool
	Status     transaction.Status
}

type TransactionRepo interface {
	GetByGatewayReference(ctx context.Context, gatewayID, reference string) (*transaction.Transaction, error)
	FindByID(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error)
	UpdateStatus(ctx context.Context, t *transaction.Transaction) error
	MarkReviewRequired(ctx context.Context, id uuid.UUID, reason string) (bool, error)
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

func capturedMismatch(ev Event, txn *transaction.Transaction) string {
	switch {
	case ev.Amount <= 0:
		return "webhook_amount_missing"
	case ev.Amount != txn.Amount:
		return "webhook_amount_mismatch"
	case ev.Currency == "":
		return "webhook_currency_missing"
	case !strings.EqualFold(ev.Currency, txn.Currency):
		return "webhook_currency_mismatch"
	}
	return ""
}

func (s *Service) flagMismatch(ctx context.Context, txn *transaction.Transaction, gatewayID, reason string, ev Event) error {
	if txn.Status == transaction.StatusProcessing {
		if _, err := s.txns.MarkReviewRequired(ctx, txn.ID, reason); err != nil {
			return err
		}
	}
	s.metrics.Increment(ports.MetricWebhookCaptureMismatch, map[string]string{ports.FieldGatewayID: gatewayID, "reason": reason})
	s.log.Error(ports.LogEventWebhookCaptureMismatch, map[string]any{
		ports.FieldErrorCode:     reason,
		ports.FieldTraceID:       "",
		ports.FieldTransactionID: txn.ID.String(),
		ports.FieldGatewayID:     gatewayID,
		"expected_amount":        txn.Amount,
		"expected_currency":      txn.Currency,
		"captured_amount":        ev.Amount,
		"captured_currency":      ev.Currency,
		"status":                 string(txn.Status),
	}, fmt.Errorf("signed webhook reports a capture that does not match the payment"))
	return nil
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

		txn, referenceRecovered, err := s.findTransaction(ctx, gatewayID, ev)
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
		lateCapture := terminal && newStatus == transaction.StatusSucceeded && txn.Status == transaction.StatusFailed
		if !terminal || (txn.Status != transaction.StatusProcessing && !lateCapture) {
			if referenceRecovered && txn.Status == transaction.StatusProcessing {
				if err := s.txns.UpdateStatus(ctx, txn); err != nil {
					return err
				}
			}
			outcome.Status = txn.Status
			return nil
		}

		if newStatus == transaction.StatusSucceeded {
			if reason := capturedMismatch(ev, txn); reason != "" {
				if err := s.flagMismatch(ctx, txn, gatewayID, reason, ev); err != nil {
					return err
				}
				outcome.Mismatch = true
				outcome.Status = txn.Status
				return nil
			}
		}

		if err := transaction.TransitionState(txn, newStatus, transaction.ActorGateway); err != nil {
			return err
		}
		switch {
		case newStatus == transaction.StatusFailed:
			txn.FailureReason = &transaction.FailureReason{
				Category: "gateway_webhook_failed",
				Source:   transaction.FailureReasonSourceGateway,
			}
		case lateCapture:
			txn.FailureReason = nil
			s.metrics.Increment(ports.MetricTransactionLateCapture, map[string]string{ports.FieldGatewayID: gatewayID})
			s.log.Error(ports.LogEventTransactionLateCapture, map[string]any{
				ports.FieldErrorCode:     "late_capture_after_failed",
				ports.FieldTraceID:       "",
				ports.FieldTransactionID: txn.ID.String(),
				ports.FieldGatewayID:     gatewayID,
			}, fmt.Errorf("gateway captured a payment already recorded as FAILED"))
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

func (s *Service) findTransaction(ctx context.Context, gatewayID string, ev Event) (*transaction.Transaction, bool, error) {
	txn, err := s.txns.GetByGatewayReference(ctx, gatewayID, ev.GatewayReferenceID)
	if err != nil || txn != nil {
		return txn, false, err
	}
	if ev.TransactionID == "" || ev.GatewayReferenceID == "" {
		return nil, false, nil
	}
	id, err := uuid.Parse(ev.TransactionID)
	if err != nil {
		return nil, false, nil
	}
	txn, err = s.txns.FindByID(ctx, id)
	if err != nil || txn == nil {
		return nil, false, err
	}
	gateway := txn.ActualGateway
	if gateway == "" {
		gateway = txn.GatewayID
	}
	if gateway != gatewayID || txn.GatewayReferenceID != "" {
		return nil, false, nil
	}
	txn.GatewayReferenceID = ev.GatewayReferenceID
	s.log.Info(ports.LogEventWebhookReferenceRecovered, map[string]any{
		ports.FieldTransactionID: txn.ID.String(),
		ports.FieldGatewayID:     gatewayID,
	})
	return txn, true, nil
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
