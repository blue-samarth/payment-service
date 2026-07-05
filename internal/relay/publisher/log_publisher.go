package publisher

import (
	"context"

	"samarth/payment-service/internal/ports"
)

type LogPublisher struct {
	log ports.Logger
}

func NewLogPublisher(log ports.Logger) *LogPublisher {
	return &LogPublisher{log: log}
}

func (p *LogPublisher) Publish(ctx context.Context, event ports.PendingEvent) error {
	p.log.Info(ports.LogEventOutboxPublish, map[string]any{
		"event_type":    event.EventType,
		"aggregate_id":  event.AggregateID.String(),
		"event_version": event.EventVersion,
		"sink":          "log",
	})
	return nil
}
