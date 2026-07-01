package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type LeaseRepository struct {
	db *DB
	q  *Queries
}

func NewLeaseRepository(db *DB, q *Queries) *LeaseRepository {
	return &LeaseRepository{db: db, q: q}
}

func (r *LeaseRepository) Acquire(ctx context.Context, leaseKey, paymentIntentID uuid.UUID, ttlSec int) (bool, []byte, error) {
	tx, err := txFromContext(ctx)
	if err != nil {
		return false, nil, err
	}

	tag, err := tx.Exec(ctx, r.q.LeaseAcquire, leaseKey, paymentIntentID, ttlSec)
	if err != nil {
		return false, nil, fmt.Errorf("lease: acquire %s: %w", leaseKey, err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil, nil
	}

	var cached []byte
	err = tx.QueryRow(ctx, r.q.LeaseGetCached, leaseKey).Scan(&cached)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("lease: read cached response %s: %w", leaseKey, err)
	}
	return false, cached, nil
}

func (r *LeaseRepository) WriteCachedResponse(ctx context.Context, leaseKey uuid.UUID, response []byte) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, r.q.LeaseWriteCached, leaseKey, string(response))
	if err != nil {
		return fmt.Errorf("lease: write cached response %s: %w", leaseKey, err)
	}
	return nil
}
