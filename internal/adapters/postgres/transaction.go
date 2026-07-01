package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"samarth/payment-service/internal/domain/transaction"
)

type TransactionRepository struct {
	db *DB
	q  *Queries
}

func NewTransactionRepository(db *DB, q *Queries) *TransactionRepository {
	return &TransactionRepository{db: db, q: q}
}

var ErrNotFound = errors.New("transaction not found")
var ErrVersionConflict = errors.New("transaction version conflict")

func (r *TransactionRepository) Insert(ctx context.Context, t *transaction.Transaction) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	methodDetails, err := json.Marshal(t.MethodDetails)
	if err != nil {
		return fmt.Errorf("transaction: marshal method_details: %w", err)
	}

	failureReason, err := json.Marshal(t.FailureReason)
	if err != nil {
		return fmt.Errorf("transaction: marshal failure_reason: %w", err)
	}

	metadata, err := json.Marshal(t.Metadata)
	if err != nil {
		return fmt.Errorf("transaction: marshal metadata: %w", err)
	}

	_, err = tx.Exec(ctx, r.q.TransactionInsert,
		t.ID, t.MerchantID, t.Amount, t.Currency, t.PaymentMethod, t.Status, t.Version,
		t.GatewayID, t.GatewayReferenceID, t.GatewayIdempotencyKey,
		t.AttemptedGateway, t.ActualGateway, t.OriginalGateway,
		t.EstimatedTimeoutSeconds, string(failureReason), string(methodDetails), string(metadata),
		t.Description, t.CustomerID, t.CustomerEmail,
		t.CancelIntent, nullIfEmpty(string(t.CancelRequestedBy)), t.CancelRequestedAt, nullIfEmpty(string(t.CancelRequestedVia)),
		t.ProcessingStartedAt, t.ProcessingTimeout,
		t.CreatedAt, t.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("transaction: insert %s: %w", t.ID, err)
	}
	return nil
}

func (r *TransactionRepository) GetByID(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error) {
	row := r.db.pool.QueryRow(ctx, r.q.TransactionGetByID, id)
	return scanTransaction(row)
}

func (r *TransactionRepository) GetByGatewayReference(ctx context.Context, gatewayID, reference string) (*transaction.Transaction, error) {
	row := r.db.pool.QueryRow(ctx, r.q.TransactionGetByGatewayRef, gatewayID, reference)
	t, err := scanTransaction(row)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	return t, err
}

func (r *TransactionRepository) UpdateStatus(ctx context.Context, t *transaction.Transaction) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	methodDetails, err := json.Marshal(t.MethodDetails)
	if err != nil {
		return fmt.Errorf("transaction: marshal method_details: %w", err)
	}

	failureReason, err := json.Marshal(t.FailureReason)
	if err != nil {
		return fmt.Errorf("transaction: marshal failure_reason: %w", err)
	}

	var newVersion int
	err = tx.QueryRow(ctx, r.q.TransactionUpdateStatus,
		t.Status,
		t.ActualGateway,
		t.GatewayReferenceID,
		string(failureReason),
		string(methodDetails),
		t.ProcessingStartedAt,
		t.ProcessingTimeout,
		time.Now().UTC(),
		t.ID,
		t.Version,
	).Scan(&newVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrVersionConflict
	}
	if err != nil {
		return fmt.Errorf("transaction: update status %s: %w", t.ID, err)
	}

	t.Version = newVersion
	return nil
}

func (r *TransactionRepository) SetCancelIntent(ctx context.Context, id uuid.UUID, by transaction.Actor, via transaction.CancelVia) (bool, error) {
	tag, err := r.db.pool.Exec(ctx, r.q.TransactionSetCancelIntent, id, by, via)
	if err != nil {
		return false, fmt.Errorf("transaction: set cancel intent %s: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *TransactionRepository) ListExpiredLeases(ctx context.Context) ([]*transaction.Transaction, error) {
	rows, err := r.db.pool.Query(ctx, r.q.TransactionListExpiredLeases)
	if err != nil {
		return nil, fmt.Errorf("transaction: list expired leases: %w", err)
	}
	defer rows.Close()

	var txns []*transaction.Transaction
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("transaction: scan expired lease id: %w", err)
		}
		t, err := r.GetByID(ctx, id)
		if err != nil {
			return nil, err
		}
		txns = append(txns, t)
	}
	return txns, rows.Err()
}

func (r *TransactionRepository) ListExpiredLeaseIDs(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := r.db.pool.Query(ctx, r.q.TransactionListExpiredLeases)
	if err != nil {
		return nil, fmt.Errorf("transaction: list expired lease ids: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("transaction: scan expired lease id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func scanTransaction(row pgx.Row) (*transaction.Transaction, error) {
	var t transaction.Transaction
	var failureReasonRaw []byte
	var methodDetailsRaw []byte
	var processingTimeout *string
	var cancelBy, cancelVia *string

	err := row.Scan(
		&t.ID, &t.MerchantID, &t.Amount, &t.Currency, &t.PaymentMethod, &t.Status, &t.Version,
		&t.GatewayID, &t.GatewayReferenceID, &t.GatewayIdempotencyKey,
		&t.AttemptedGateway, &t.ActualGateway, &t.OriginalGateway,
		&t.EstimatedTimeoutSeconds, &failureReasonRaw, &methodDetailsRaw, &t.Metadata,
		&t.Description, &t.CustomerID, &t.CustomerEmail,
		&t.CancelIntent, &cancelBy, &t.CancelRequestedAt, &cancelVia,
		&t.ProcessingStartedAt, &processingTimeout,
		&t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("transaction: scan: %w", err)
	}

	if cancelBy != nil {
		t.CancelRequestedBy = transaction.Actor(*cancelBy)
	}
	if cancelVia != nil {
		t.CancelRequestedVia = transaction.CancelVia(*cancelVia)
	}

	if len(failureReasonRaw) > 0 && string(failureReasonRaw) != "null" {
		if err := json.Unmarshal(failureReasonRaw, &t.FailureReason); err != nil {
			return nil, fmt.Errorf("transaction: unmarshal failure_reason: %w", err)
		}
	}

	if len(methodDetailsRaw) > 0 && string(methodDetailsRaw) != "null" {
		if err := json.Unmarshal(methodDetailsRaw, &t.MethodDetails); err != nil {
			return nil, fmt.Errorf("transaction: unmarshal method_details: %w", err)
		}
	}

	if processingTimeout != nil {
		d, err := parseInterval(*processingTimeout)
		if err != nil {
			return nil, fmt.Errorf("transaction: parse processing_timeout: %w", err)
		}
		t.ProcessingTimeout = &d
	}

	return &t, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func parseInterval(s string) (time.Duration, error) {
	var days, h, m int
	var sec float64

	if n, _ := fmt.Sscanf(s, "%d days %d:%d:%f", &days, &h, &m, &sec); n == 4 {
		return time.Duration(days)*24*time.Hour +
			time.Duration(h)*time.Hour +
			time.Duration(m)*time.Minute +
			time.Duration(sec*float64(time.Second)), nil
	}

	if n, _ := fmt.Sscanf(s, "%d:%d:%f", &h, &m, &sec); n == 3 {
		return time.Duration(h)*time.Hour +
			time.Duration(m)*time.Minute +
			time.Duration(sec*float64(time.Second)), nil
	}

	return 0, fmt.Errorf("parse interval: unrecognised format %q", s)
}
