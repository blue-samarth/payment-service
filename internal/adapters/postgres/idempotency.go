package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type IdempotencyRepository struct {
	db *DB
	q  *Queries
}

func NewIdempotencyRepository(db *DB, q *Queries) *IdempotencyRepository {
	return &IdempotencyRepository{db: db, q: q}
}

func (r *IdempotencyRepository) Reserve(ctx context.Context, compositeHash, requestHash string) (bool, error) {
	tag, err := r.db.pool.Exec(ctx, r.q.IdempotencyReserve, compositeHash, requestHash)
	if err != nil {
		return false, fmt.Errorf("idempotency: reserve %s: %w", compositeHash, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *IdempotencyRepository) Lookup(ctx context.Context, compositeHash string) (found bool, requestHash, status string, response []byte, err error) {
	err = r.db.pool.QueryRow(ctx, r.q.IdempotencyLookup, compositeHash).Scan(&requestHash, &status, &response)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", "", nil, nil
	}
	if err != nil {
		return false, "", "", nil, fmt.Errorf("idempotency: lookup %s: %w", compositeHash, err)
	}
	return true, requestHash, status, response, nil
}

func (r *IdempotencyRepository) Complete(ctx context.Context, compositeHash string, response []byte) error {
	if _, err := r.db.pool.Exec(ctx, r.q.IdempotencyComplete, compositeHash, string(response)); err != nil {
		return fmt.Errorf("idempotency: complete %s: %w", compositeHash, err)
	}
	return nil
}

func (r *IdempotencyRepository) Release(ctx context.Context, compositeHash string) error {
	if _, err := r.db.pool.Exec(ctx, r.q.IdempotencyRelease, compositeHash); err != nil {
		return fmt.Errorf("idempotency: release %s: %w", compositeHash, err)
	}
	return nil
}

func (r *IdempotencyRepository) SweepStaleProcessing(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := r.db.pool.Exec(ctx, r.q.IdempotencySweepStaleProcessing, int64(olderThan.Seconds()))
	if err != nil {
		return 0, fmt.Errorf("idempotency: sweep stale processing: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *IdempotencyRepository) DeleteExpired(ctx context.Context) (int64, error) {
	tag, err := r.db.pool.Exec(ctx, r.q.IdempotencyDeleteExpired)
	if err != nil {
		return 0, fmt.Errorf("idempotency: delete expired: %w", err)
	}
	return tag.RowsAffected(), nil
}
