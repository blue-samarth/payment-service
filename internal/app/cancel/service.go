package cancel

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type Outcome string

const (
	OutcomeRequested       Outcome = "CANCEL_REQUESTED"
	OutcomeAlreadyTerminal Outcome = "ALREADY_TERMINAL"
)

type Result struct {
	Outcome Outcome
	Status  transaction.Status
}

type TransactionStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error)
	SetCancelIntent(ctx context.Context, id uuid.UUID, by transaction.Actor, via transaction.CancelVia) (bool, error)
}

type Service struct {
	txns    TransactionStore
	log     ports.Logger
	metrics ports.MetricRecorder
}

func NewService(txns TransactionStore, log ports.Logger, metrics ports.MetricRecorder) *Service {
	return &Service{txns: txns, log: log, metrics: metrics}
}

type CancelInput struct {
	TransactionID uuid.UUID
	By            transaction.Actor
	Via           transaction.CancelVia
}

func (s *Service) Cancel(ctx context.Context, in CancelInput) (Result, error) {
	txn, err := s.txns.GetByID(ctx, in.TransactionID)
	if err != nil {
		return Result{}, fmt.Errorf("cancel: load transaction %s: %w", in.TransactionID, err)
	}

	if txn.Status.IsTerminal() {
		return Result{Outcome: OutcomeAlreadyTerminal, Status: txn.Status}, nil
	}
	if txn.CancelIntent {
		return Result{Outcome: OutcomeRequested, Status: txn.Status}, nil
	}

	set, err := s.txns.SetCancelIntent(ctx, in.TransactionID, in.By, in.Via)
	if err != nil {
		return Result{}, fmt.Errorf("cancel: set intent for %s: %w", in.TransactionID, err)
	}
	if set {
		s.log.Info(ports.LogEventCancelIntent, map[string]any{
			ports.FieldTransactionID: in.TransactionID.String(),
			ports.FieldActor:         string(in.By),
		})
	}

	return Result{Outcome: OutcomeRequested, Status: txn.Status}, nil
}
