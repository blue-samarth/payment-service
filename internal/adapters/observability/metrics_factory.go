package observability

import (
	"context"
	"fmt"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/observability/otel"
	"samarth/payment-service/internal/ports"
)

type MetricsCloser func(context.Context) error

func NewMetrics(ctx context.Context, cfg *config.Config) (ports.MetricRecorder, MetricsCloser, error) {
	noClose := func(context.Context) error { return nil }

	switch cfg.Observability.Backend {
	case "otel", "otlp":
		rec, err := otel.New(ctx, otel.Config{
			Endpoint:       cfg.Observability.OTLPEndpoint,
			Protocol:       cfg.Observability.OTLPProtocol,
			ServiceName:    "payment-service",
			ServiceVersion: cfg.App.ServiceVersion,
			Environment:    cfg.App.Environment,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("observability: init otel metrics: %w", err)
		}
		return rec, rec.Shutdown, nil
	case "", "stdout", "noop":
		return NewNoopMetrics(), noClose, nil
	default:
		return nil, nil, fmt.Errorf("observability: unknown OBSERVABILITY_BACKEND %q (want otel, stdout, or noop)", cfg.Observability.Backend)
	}
}
