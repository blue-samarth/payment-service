package cancel

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type Outcome string

const (
	OutcomeRequested       Outcome = "CANCEL_REQUESTED"
	OutcomeAlreadyTerminal Outcome = "ALREADY_TERMINAL"
)

type Result struct {
	Verdict idempotency.Verdict
	Outcome Outcome
	Status  transaction.Status
}

type TransactionStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error)
	SetCancelIntent(ctx context.Context, id uuid.UUID, by transaction.Actor, via transaction.CancelVia) (bool, error)
}

type Service struct {
	txns    TransactionStore
	idem    *idempotency.Guard
	log     ports.Logger
	metrics ports.MetricRecorder
}

func NewService(txns TransactionStore, log ports.Logger, metrics ports.MetricRecorder) *Service {
	return &Service{txns: txns, log: log, metrics: metrics}
}

func (s *Service) SetIdempotency(g *idempotency.Guard) { s.idem = g }

type CancelInput struct {
	TransactionID  uuid.UUID
	By             transaction.Actor
	Via            transaction.CancelVia
	IdempotencyKey string
}

type idempotencyCancelResponse struct {
	Outcome string `json:"outcome"`
	Status  string `json:"status"`
}

func (s *Service) Cancel(ctx context.Context, in CancelInput) (Result, error) {
	if s.idem == nil {
		outcome, status, err := s.cancel(ctx, in)
		if err != nil {
			return Result{}, err
		}
		return Result{Verdict: idempotency.Created, Outcome: outcome, Status: status}, nil
	}

	if in.IdempotencyKey == "" {
		return Result{}, idempotency.ErrKeyRequired
	}
	composite := idempotency.Composite(in.TransactionID.String(), "cancel_payment", in.IdempotencyKey)
	requestHash := idempotency.RequestHash(in.TransactionID.String(), string(in.By), string(in.Via))

	var outcome Outcome
	var status transaction.Status
	res, err := s.idem.Execute(ctx, composite, requestHash, func(ctx context.Context) ([]byte, error) {
		o, st, err := s.cancel(ctx, in)
		if err != nil {
			return nil, err
		}
		outcome, status = o, st
		return json.Marshal(idempotencyCancelResponse{Outcome: string(o), Status: string(st)})
	})
	if err != nil {
		return Result{}, err
	}

	switch res.Verdict {
	case idempotency.Created:
		return Result{Verdict: res.Verdict, Outcome: outcome, Status: status}, nil
	case idempotency.Replayed:
		var stored idempotencyCancelResponse
		if err := json.Unmarshal(res.Response, &stored); err != nil {
			return Result{}, fmt.Errorf("cancel: decode idempotent response: %w", err)
		}
		return Result{Verdict: res.Verdict, Outcome: Outcome(stored.Outcome), Status: transaction.Status(stored.Status)}, nil
	default:
		return Result{Verdict: res.Verdict}, nil
	}
}

func (s *Service) cancel(ctx context.Context, in CancelInput) (Outcome, transaction.Status, error) {
	txn, err := s.txns.GetByID(ctx, in.TransactionID)
	if err != nil {
		return "", "", fmt.Errorf("cancel: load transaction %s: %w", in.TransactionID, err)
	}

	if txn.Status.IsTerminal() {
		return OutcomeAlreadyTerminal, txn.Status, nil
	}
	if txn.CancelIntent {
		return OutcomeRequested, txn.Status, nil
	}

	set, err := s.txns.SetCancelIntent(ctx, in.TransactionID, in.By, in.Via)
	if err != nil {
		return "", "", fmt.Errorf("cancel: set intent for %s: %w", in.TransactionID, err)
	}
	if set {
		s.log.Info(ports.LogEventCancelIntent, map[string]any{
			ports.FieldTransactionID: in.TransactionID.String(),
			ports.FieldActor:         string(in.By),
		})
	}

	return OutcomeRequested, txn.Status, nil
}
