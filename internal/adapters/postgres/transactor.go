package postgres

import (
	"context"
	"fmt"
)

type Transactor struct {
	db *DB
}

func NewTransactor(db *DB) *Transactor {
	return &Transactor{db: db}
}

func (t *Transactor) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := t.db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(WithTx(ctx, tx)); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit tx: %w", err)
	}
	return nil
}
