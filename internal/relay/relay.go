package relay

import (
	"context"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/ports"
)

type OutboxReader interface {
	PollPending(ctx context.Context, shardMin, shardMax, batchSize int) ([]ports.PendingEvent, error)
	MarkPublished(ctx context.Context, id uuid.UUID, createdAt time.Time) error
	MarkFailed(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string, nextAttempt time.Time) error
	MarkExhausted(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string) error
}

type Publisher interface {
	Publish(ctx context.Context, event ports.PendingEvent) error
}

type Config struct {
	ShardMin     int
	ShardMax     int
	BatchSize    int
	MaxAttempts  int
	PollInterval time.Duration
	BaseBackoff  time.Duration
	MaxBackoff   time.Duration
}

type Worker struct {
	outbox    OutboxReader
	publisher Publisher
	log       ports.Logger
	metrics   ports.MetricRecorder
	cfg       Config
}

func NewWorker(outbox OutboxReader, publisher Publisher, log ports.Logger, metrics ports.MetricRecorder, cfg Config) *Worker {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Minute
	}
	return &Worker{outbox: outbox, publisher: publisher, log: log, metrics: metrics, cfg: cfg}
}

func (w *Worker) Run(ctx context.Context) error {
	w.log.Info(ports.LogEventRelayModeSwitch, map[string]any{
		ports.FieldRelayMode: "polling",
		"shard_min":          w.cfg.ShardMin,
		"shard_max":          w.cfg.ShardMax,
	})

	for {
		n, err := w.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.log.Error(ports.LogEventOutboxPublishFailure, map[string]any{
				ports.FieldErrorCode:     "outbox_poll_failed",
				ports.FieldTraceID:       "",
				ports.FieldTransactionID: "",
			}, err)
		}

		if err == nil && n >= w.cfg.BatchSize {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				continue
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.cfg.PollInterval):
		}
	}
}

func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	events, err := w.outbox.PollPending(ctx, w.cfg.ShardMin, w.cfg.ShardMax, w.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	for _, e := range events {
		w.processEvent(ctx, e)
	}
	return len(events), nil
}

func (w *Worker) processEvent(ctx context.Context, e ports.PendingEvent) {
	start := time.Now()
	err := w.publisher.Publish(ctx, e)
	w.metrics.Histogram(ports.MetricOutboxPublishLatencyMs, float64(time.Since(start).Milliseconds()), map[string]string{
		"event_type": e.EventType,
	})

	if err == nil {
		if mErr := w.outbox.MarkPublished(ctx, e.ID, e.CreatedAt); mErr != nil {
			w.log.Error(ports.LogEventOutboxPublish, map[string]any{
				ports.FieldErrorCode:     "mark_published_failed",
				ports.FieldTraceID:       "",
				ports.FieldTransactionID: e.AggregateID.String(),
			}, mErr)
			return
		}
		w.log.Info(ports.LogEventOutboxPublish, map[string]any{
			"event_type":   e.EventType,
			"aggregate_id": e.AggregateID.String(),
		})
		return
	}

	if e.Attempts+1 >= w.cfg.MaxAttempts {
		if mErr := w.outbox.MarkExhausted(ctx, e.ID, e.CreatedAt, err.Error()); mErr != nil {
			w.log.Error(ports.LogEventOutboxDeadLetter, map[string]any{
				ports.FieldErrorCode:     "mark_exhausted_failed",
				ports.FieldTraceID:       "",
				ports.FieldTransactionID: e.AggregateID.String(),
			}, mErr)
			return
		}
		w.metrics.Increment(ports.MetricOutboxDeadLetter, map[string]string{"event_type": e.EventType})
		w.log.Error(ports.LogEventOutboxDeadLetter, map[string]any{
			ports.FieldErrorCode:     "publish_exhausted",
			ports.FieldTraceID:       "",
			ports.FieldTransactionID: e.AggregateID.String(),
			"event_type":             e.EventType,
		}, err)
		return
	}

	nextAttempt := time.Now().Add(w.backoff(e.Attempts))
	if mErr := w.outbox.MarkFailed(ctx, e.ID, e.CreatedAt, err.Error(), nextAttempt); mErr != nil {
		w.log.Error(ports.LogEventOutboxPublishFailure, map[string]any{
			ports.FieldErrorCode:     "mark_failed_failed",
			ports.FieldTraceID:       "",
			ports.FieldTransactionID: e.AggregateID.String(),
		}, mErr)
		return
	}
	w.metrics.Increment(ports.MetricOutboxPublishFailure, map[string]string{"event_type": e.EventType})
	w.log.Warn(ports.LogEventOutboxPublishFailure, map[string]any{
		"event_type":             e.EventType,
		"aggregate_id":           e.AggregateID.String(),
		ports.FieldAttemptNumber: e.Attempts + 1,
	})
}

func (w *Worker) backoff(attempts int) time.Duration {
	d := w.cfg.BaseBackoff << attempts
	if d <= 0 || d > w.cfg.MaxBackoff {
		return w.cfg.MaxBackoff
	}
	return d
}
