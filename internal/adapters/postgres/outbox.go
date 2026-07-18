package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"samarth/payment-service/internal/ports"
)

const defaultOutboxClaimTTL = 60 * time.Second

type OutboxWriter struct {
	db       *DB
	q        *Queries
	claimTTL time.Duration
}

type MerchantWebhookWriter struct {
	db *DB
	q  *Queries
}

var (
	_ ports.OutboxWriter          = (*OutboxWriter)(nil)
	_ ports.MerchantWebhookWriter = (*MerchantWebhookWriter)(nil)
)

type txKey struct{}

func NewOutboxWriter(db *DB, q *Queries) *OutboxWriter {
	return &OutboxWriter{db: db, q: q, claimTTL: defaultOutboxClaimTTL}
}

// SetClaimTTL controls how long a claimed (PUBLISHING) event stays invisible to
// other pollers before it is treated as an abandoned claim and reclaimed. It
// must comfortably exceed the time to publish one batch.
func (w *OutboxWriter) SetClaimTTL(d time.Duration) {
	if d > 0 {
		w.claimTTL = d
	}
}
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
		string(event.Payload), version, event.AggregateVersion, nextAttempt,
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
		return fmt.Errorf("outbox: event %s not found or not in PUBLISHING state", id)
	}
	return nil
}

func (w *OutboxWriter) MarkExhausted(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string) error {
	return withTx(ctx, w.db.pool, func(tx pgx.Tx) error {
		var event struct {
			AggregateID      uuid.UUID
			AggregateType    string
			EventType        string
			Payload          []byte
			EventVersion     int
			AggregateVersion int
		}

		err := tx.QueryRow(ctx, w.q.OutboxMarkExhausted, id, createdAt, lastErr).Scan(
			&event.AggregateID, &event.AggregateType,
			&event.EventType, &event.Payload, &event.EventVersion, &event.AggregateVersion,
		)
		if err != nil {
			return fmt.Errorf("outbox: exhaust event %s: %w", id, err)
		}

		_, err = tx.Exec(ctx, w.q.OutboxDeadLetterInsert,
			id, event.AggregateID, event.AggregateType,
			event.EventType, string(event.Payload), event.EventVersion, event.AggregateVersion, lastErr,
		)
		if err != nil {
			return fmt.Errorf("outbox: write dead letter for %s: %w", id, err)
		}

		return nil
	})
}

// PollPending atomically claims a batch of due events (PENDING, or a PUBLISHING
// row whose claim has gone stale past claimTTL), transitioning them to
// PUBLISHING and returning them. Because the claim is committed before the
// caller publishes, a second poller no longer sees the same rows — the previous
// FOR UPDATE SKIP LOCKED SELECT released its lock at commit, before publishing,
// so two workers could fetch and publish the same event.
func (w *OutboxWriter) PollPending(ctx context.Context, shards []int, batchSize int) ([]ports.PendingEvent, error) {
	claimTTLSec := int64(w.claimTTL.Seconds())

	if len(shards) == 0 {
		return nil, nil
	}
	shardArg := make([]int32, len(shards))
	for i, s := range shards {
		shardArg[i] = int32(s)
	}

	rows, err := w.db.pool.Query(ctx, w.q.OutboxPollPending, shardArg, batchSize, claimTTLSec)
	if err != nil {
		return nil, fmt.Errorf("outbox: poll pending: %w", err)
	}
	defer rows.Close()

	var events []ports.PendingEvent
	for rows.Next() {
		var e ports.PendingEvent
		if err := rows.Scan(
			&e.ID, &e.AggregateID, &e.AggregateType,
			&e.EventType, &e.Payload, &e.EventVersion, &e.AggregateVersion, &e.Attempts, &e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("outbox: scan pending event: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (w *OutboxWriter) ReplayDeadLetter(ctx context.Context, deadLetterID uuid.UUID, actor, reason string) (uuid.UUID, error) {
	newEventID := uuid.New()

	err := withTx(ctx, w.db.pool, func(tx pgx.Tx) error {
		var dl struct {
			AggregateID      uuid.UUID
			AggregateType    string
			EventType        string
			Payload          []byte
			EventVersion     int
			AggregateVersion int
			ResolvedAt       *time.Time
		}

		err := tx.QueryRow(ctx, w.q.OutboxDeadLetterGet, deadLetterID).Scan(
			&dl.AggregateID, &dl.AggregateType, &dl.EventType,
			&dl.Payload, &dl.EventVersion, &dl.AggregateVersion, &dl.ResolvedAt,
		)
		if err != nil {
			return fmt.Errorf("outbox: dead letter %s not found: %w", deadLetterID, err)
		}
		if dl.ResolvedAt != nil {
			return fmt.Errorf("outbox: dead letter %s already resolved", deadLetterID)
		}

		_, err = tx.Exec(ctx, w.q.OutboxReplayInsert,
			newEventID, dl.AggregateID, dl.AggregateType,
			dl.EventType, string(dl.Payload), dl.EventVersion, dl.AggregateVersion,
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

type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func queryer(ctx context.Context, pool *pgxpool.Pool) Queryer {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok && tx != nil {
		return tx
	}
	return pool
}

func withTx(ctx context.Context, pool interface {
	Begin(context.Context) (pgx.Tx, error)
}, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
