package ports

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type OutboxWriter interface {
	Write(ctx context.Context, event OutboxEvent) error
	MarkPublished(ctx context.Context, id uuid.UUID, createdAt time.Time) error
	MarkFailed(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string, nextAttempt time.Time) error
	MarkExhausted(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string) error
	PollPending(ctx context.Context, shardMin, shardMax, batchSize int) ([]PendingEvent, error)
	ReplayDeadLetter(ctx context.Context, deadLetterID uuid.UUID, actor, reason string) (uuid.UUID, error)
}

type PendingEvent struct {
	ID            uuid.UUID
	AggregateID   uuid.UUID
	AggregateType string
	EventType     string
	Payload       []byte
	EventVersion  int
	Attempts      int
	CreatedAt     time.Time
}

type OutboxEvent struct {
	ID            uuid.UUID
	AggregateID   uuid.UUID
	AggregateType string // e.g. "transaction", "refund"
	EventType     string
	Payload       []byte // must be JSON-encoded; maps to JSONB column
	EventVersion  int
	NextAttemptAt *time.Time // nil = publish immediately (next_attempt_at = NOW())
}

const (
	EventTypePaymentCreated   = "PAYMENT_CREATED"
	EventTypePaymentSucceeded = "PAYMENT_SUCCEEDED"
	EventTypePaymentFailed    = "PAYMENT_FAILED"
	EventTypePaymentCancelled = "PAYMENT_CANCELLED"

	EventTypeRefundInitiated = "REFUND_INITIATED"
	EventTypeRefundSucceeded = "REFUND_SUCCEEDED"
	EventTypeRefundFailed    = "REFUND_FAILED"

	EventTypeAuditStateChange = "AUDIT_STATE_CHANGE"
)

type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "PENDING"
	OutboxStatusPublished OutboxStatus = "PUBLISHED"
	OutboxStatusFailed    OutboxStatus = "FAILED"
)

type DeadLetter struct {
	ID              uuid.UUID
	OriginalEventID uuid.UUID
	AggregateID     uuid.UUID
	AggregateType   string
	EventType       string
	Payload         []byte
	FailureReason   string
	FailedAt        time.Time
	ResolvedAt      *time.Time
	ResolvedBy      string
}

type MerchantWebhookWriter interface {
	WriteDelivery(ctx context.Context, delivery MerchantWebhookDelivery) error
}

type MerchantWebhookDelivery struct {
	ID            uuid.UUID
	MerchantID    uuid.UUID
	TransactionID uuid.UUID
	EventType     string
	Payload       []byte
	EndpointURL   string
}
