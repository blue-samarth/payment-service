package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"samarth/payment-service/internal/domain/refund"
)

type RefundRepository struct {
	db *DB
	q  *Queries
}

func NewRefundRepository(db *DB, q *Queries) *RefundRepository {
	return &RefundRepository{db: db, q: q}
}

var ErrRefundNotFound = errors.New("refund not found")

func (r *RefundRepository) Insert(ctx context.Context, rf *refund.Refund) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	failureReason, err := json.Marshal(rf.FailureReason)
	if err != nil {
		return fmt.Errorf("refund: marshal failure_reason: %w", err)
	}

	_, err = tx.Exec(ctx, r.q.RefundInsert,
		rf.ID, rf.TransactionID, rf.Amount, rf.Reason, rf.Status,
		rf.InitiatedBy, rf.GatewayRefundID, rf.AttemptedGateway, rf.ActualGateway,
		rf.Attempts, string(failureReason), rf.InitiatedAt, rf.ResolvedAt,
	)
	if err != nil {
		return fmt.Errorf("refund: insert %s: %w", rf.ID, err)
	}
	return nil
}

func (r *RefundRepository) GetByID(ctx context.Context, id uuid.UUID) (*refund.Refund, error) {
	row := r.db.pool.QueryRow(ctx, r.q.RefundGetByID, id)
	return scanRefund(row)
}

func (r *RefundRepository) SumActiveRefunds(ctx context.Context, transactionID uuid.UUID) (int64, error) {
	tx, err := txFromContext(ctx)
	if err != nil {
		return 0, err
	}

	var sum int64
	err = tx.QueryRow(ctx, r.q.RefundSumActive, transactionID).Scan(&sum)
	if err != nil {
		return 0, fmt.Errorf("refund: sum active refunds for transaction %s: %w", transactionID, err)
	}
	return sum, nil
}

func (r *RefundRepository) UpdateStatus(ctx context.Context, rf *refund.Refund) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	failureReason, err := json.Marshal(rf.FailureReason)
	if err != nil {
		return fmt.Errorf("refund: marshal failure_reason: %w", err)
	}

	tag, err := tx.Exec(ctx, r.q.RefundUpdateStatus,
		rf.Status, rf.GatewayRefundID, rf.ActualGateway,
		rf.Attempts, string(failureReason), rf.ResolvedAt,
		rf.ID,
	)
	if err != nil {
		return fmt.Errorf("refund: update status %s: %w", rf.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrRefundNotFound
	}
	return nil
}

func (r *RefundRepository) ExistsByReason(ctx context.Context, transactionID uuid.UUID, reason string) (bool, error) {
	var exists bool
	err := r.db.pool.QueryRow(ctx, r.q.RefundExistsByReason, transactionID, reason).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("refund: exists by reason %s/%s: %w", transactionID, reason, err)
	}
	return exists, nil
}

func (r *RefundRepository) LockParentTransaction(ctx context.Context, transactionID uuid.UUID) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	var id uuid.UUID
	err = tx.QueryRow(ctx, r.q.RefundLockParent, transactionID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("refund: lock parent transaction %s: %w", transactionID, err)
	}
	return nil
}

func scanRefund(row pgx.Row) (*refund.Refund, error) {
	var rf refund.Refund
	var failureReasonRaw []byte

	err := row.Scan(
		&rf.ID, &rf.TransactionID, &rf.Amount, &rf.Reason, &rf.Status,
		&rf.InitiatedBy, &rf.GatewayRefundID, &rf.AttemptedGateway, &rf.ActualGateway,
		&rf.Attempts, &failureReasonRaw, &rf.InitiatedAt, &rf.ResolvedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrRefundNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("refund: scan: %w", err)
	}

	if len(failureReasonRaw) > 0 && string(failureReasonRaw) != "null" {
		if err := json.Unmarshal(failureReasonRaw, &rf.FailureReason); err != nil {
			return nil, fmt.Errorf("refund: unmarshal failure_reason: %w", err)
		}
	}

	return &rf, nil
}
