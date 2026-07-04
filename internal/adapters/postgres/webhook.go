package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

type WebhookRepository struct {
	db *DB
	q  *Queries
}

func NewWebhookRepository(db *DB, q *Queries) *WebhookRepository {
	return &WebhookRepository{db: db, q: q}
}

func (r *WebhookRepository) RecordEvent(ctx context.Context, eventID, gatewayID string) (bool, error) {
	tx, err := txFromContext(ctx)
	if err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, r.q.WebhookEventRecord, eventID, gatewayID)
	if err != nil {
		return false, fmt.Errorf("webhook: record event %s/%s: %w", gatewayID, eventID, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *WebhookRepository) InsertRawMetadata(ctx context.Context, transactionID uuid.UUID, gatewayID string, payload []byte) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, r.q.RawMetadataInsert, transactionID, gatewayID, string(payload)); err != nil {
		return fmt.Errorf("webhook: insert raw metadata for %s: %w", transactionID, err)
	}
	return nil
}
