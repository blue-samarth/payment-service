package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"samarth/payment-service/internal/ports"
)

type OutboxWriter struct {
	db *DB
	q  *Queries
}

type MerchantWebhookWriter struct {
	db *DB
	q  *Queries
}

type txKey struct{}

func NewOutboxWriter(db *DB, q *Queries) *OutboxWriter { return &OutboxWriter{db: db, q: q} }
func NewMerchantWebhookWriter(db *DB, q *Queries) *MerchantWebhookWriter {
	return &MerchantWebhookWriter{db: db, q: q}
}

func WithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func (w *OutboxWriter) Write(ctx context.Context, event ports.OutboxEvent) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	id := event.ID
	if id == uuid.Nil {
		id = uuid.New()
	}

	var nextAttempt any
	if event.NextAttemptAt != nil {
		nextAttempt = *event.NextAttemptAt
	}

	version := event.EventVersion
	if version == 0 {
		version = 1
	}

	_, err = tx.Exec(ctx, w.q.OutboxInsert,
		id, event.AggregateID, event.AggregateType, event.EventType,
		string(event.Payload), version, nextAttempt,
	)
	if err != nil {
		return fmt.Errorf("outbox: write event %s: %w", event.EventType, err)
	}
	return nil
}

func (w *OutboxWriter) MarkPublished(ctx context.Context, id uuid.UUID, createdAt time.Time) error {
	tag, err := w.db.pool.Exec(ctx, w.q.OutboxMarkPublished, id, createdAt)
	if err != nil {
		return fmt.Errorf("outbox: mark published %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("outbox: event %s not found or already published", id)
	}
	return nil
}

func (w *OutboxWriter) MarkFailed(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string, nextAttempt time.Time) error {
	tag, err := w.db.pool.Exec(ctx, w.q.OutboxMarkFailed, id, createdAt, lastErr, nextAttempt)
	if err != nil {
		return fmt.Errorf("outbox: mark failed %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("outbox: event %s not found or not in PENDING state", id)
	}
	return nil
}

func (w *OutboxWriter) MarkExhausted(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string) error {
	return withTx(ctx, w.db.pool, func(tx pgx.Tx) error {
		var event struct {
			AggregateID   uuid.UUID
			AggregateType string
			EventType     string
			Payload       []byte
			EventVersion  int
		}

		err := tx.QueryRow(ctx, w.q.OutboxMarkExhausted, id, createdAt, lastErr).Scan(
			&event.AggregateID, &event.AggregateType,
			&event.EventType, &event.Payload, &event.EventVersion,
		)
		if err != nil {
			return fmt.Errorf("outbox: exhaust event %s: %w", id, err)
		}

		_, err = tx.Exec(ctx, w.q.OutboxDeadLetterInsert,
			id, event.AggregateID, event.AggregateType,
			event.EventType, string(event.Payload), event.EventVersion, lastErr,
		)
		if err != nil {
			return fmt.Errorf("outbox: write dead letter for %s: %w", id, err)
		}

		return nil
	})
}

func (w *OutboxWriter) PollPending(ctx context.Context, shardMin, shardMax, batchSize int) ([]ports.PendingEvent, error) {
	var events []ports.PendingEvent

	err := withTx(ctx, w.db.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, w.q.OutboxPollPending, shardMin, shardMax, batchSize)
		if err != nil {
			return fmt.Errorf("outbox: poll pending: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var e ports.PendingEvent
			if err := rows.Scan(
				&e.ID, &e.AggregateID, &e.AggregateType,
				&e.EventType, &e.Payload, &e.EventVersion, &e.Attempts, &e.CreatedAt,
			); err != nil {
				return fmt.Errorf("outbox: scan pending event: %w", err)
			}
			events = append(events, e)
		}
		return rows.Err()
	})

	return events, err
}

func (w *OutboxWriter) ReplayDeadLetter(ctx context.Context, deadLetterID uuid.UUID, actor, reason string) (uuid.UUID, error) {
	newEventID := uuid.New()

	err := withTx(ctx, w.db.pool, func(tx pgx.Tx) error {
		var dl struct {
			AggregateID   uuid.UUID
			AggregateType string
			EventType     string
			Payload       []byte
			EventVersion  int
			ResolvedAt    *time.Time
		}

		err := tx.QueryRow(ctx, w.q.OutboxDeadLetterGet, deadLetterID).Scan(
			&dl.AggregateID, &dl.AggregateType, &dl.EventType,
			&dl.Payload, &dl.EventVersion, &dl.ResolvedAt,
		)
		if err != nil {
			return fmt.Errorf("outbox: dead letter %s not found: %w", deadLetterID, err)
		}
		if dl.ResolvedAt != nil {
			return fmt.Errorf("outbox: dead letter %s already resolved", deadLetterID)
		}

		_, err = tx.Exec(ctx, w.q.OutboxReplayInsert,
			newEventID, dl.AggregateID, dl.AggregateType,
			dl.EventType, string(dl.Payload), dl.EventVersion,
		)
		if err != nil {
			return fmt.Errorf("outbox: re-enqueue dead letter %s: %w", deadLetterID, err)
		}

		_, err = tx.Exec(ctx, w.q.OutboxDeadLetterResolve, deadLetterID, actor)
		if err != nil {
			return fmt.Errorf("outbox: resolve dead letter %s: %w", deadLetterID, err)
		}

		return nil
	})

	return newEventID, err
}

func (w *MerchantWebhookWriter) WriteDelivery(ctx context.Context, d ports.MerchantWebhookDelivery) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	id := d.ID
	if id == uuid.Nil {
		id = uuid.New()
	}

	_, err = tx.Exec(ctx, w.q.MerchantWebhookInsert,
		id, d.MerchantID, d.TransactionID,
		d.EventType, string(d.Payload), d.EndpointURL,
	)
	if err != nil {
		return fmt.Errorf("merchant webhook: write delivery: %w", err)
	}
	return nil
}

func txFromContext(ctx context.Context) (pgx.Tx, error) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	if !ok || tx == nil {
		return nil, fmt.Errorf("postgres: no transaction in context — call WithTx before writing to outbox")
	}
	return tx, nil
}

func withTx(ctx context.Context, pool interface {
	Begin(context.Context) (pgx.Tx, error)
}, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
