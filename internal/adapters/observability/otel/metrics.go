package otel

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"samarth/payment-service/internal/ports"
)

type Config struct {
	Endpoint       string
	Protocol       string
	ServiceName    string
	ServiceVersion string
	Environment    string
	ExportInterval time.Duration
}

type Recorder struct {
	meter    metric.Meter
	provider *sdkmetric.MeterProvider

	mu         sync.Mutex
	counters   map[string]metric.Float64Counter
	histograms map[string]metric.Float64Histogram
	gauges     map[string]metric.Float64Gauge
}

func New(ctx context.Context, cfg Config) (*Recorder, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("otel: OTLP endpoint is required for the otel metrics backend")
	}

	exporter, err := newExporter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("otel: build exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
		semconv.DeploymentEnvironment(cfg.Environment),
	))
	if err != nil {
		return nil, fmt.Errorf("otel: build resource: %w", err)
	}

	interval := cfg.ExportInterval
	if interval <= 0 {
		interval = 10 * time.Second
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(interval))),
	)

	return &Recorder{
		meter:      provider.Meter(cfg.ServiceName),
		provider:   provider,
		counters:   map[string]metric.Float64Counter{},
		histograms: map[string]metric.Float64Histogram{},
		gauges:     map[string]metric.Float64Gauge{},
	}, nil
}

func newExporter(ctx context.Context, cfg Config) (sdkmetric.Exporter, error) {
	switch cfg.Protocol {
	case "grpc":
		return otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpointURL(cfg.Endpoint))
	default:
		return otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(cfg.Endpoint))
	}
}

func (r *Recorder) Increment(name string, tags map[string]string) {
	c, err := r.counter(name)
	if err != nil {
		return
	}
	c.Add(context.Background(), 1, metric.WithAttributes(toAttributes(tags)...))
}

func (r *Recorder) Histogram(name string, value float64, tags map[string]string) {
	h, err := r.histogram(name)
	if err != nil {
		return
	}
	h.Record(context.Background(), value, metric.WithAttributes(toAttributes(tags)...))
}

func (r *Recorder) Gauge(name string, value float64, tags map[string]string) {
	g, err := r.gauge(name)
	if err != nil {
		return
	}
	g.Record(context.Background(), value, metric.WithAttributes(toAttributes(tags)...))
}

func (r *Recorder) Shutdown(ctx context.Context) error {
	return r.provider.Shutdown(ctx)
}

func (r *Recorder) counter(name string) (metric.Float64Counter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c, nil
	}
	c, err := r.meter.Float64Counter(name)
	if err != nil {
		return nil, err
	}
	r.counters[name] = c
	return c, nil
}

func (r *Recorder) histogram(name string) (metric.Float64Histogram, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.histograms[name]; ok {
		return h, nil
	}
	h, err := r.meter.Float64Histogram(name)
	if err != nil {
		return nil, err
	}
	r.histograms[name] = h
	return h, nil
}

func (r *Recorder) gauge(name string) (metric.Float64Gauge, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauges[name]; ok {
		return g, nil
	}
	g, err := r.meter.Float64Gauge(name)
	if err != nil {
		return nil, err
	}
	r.gauges[name] = g
	return g, nil
}

func toAttributes(tags map[string]string) []attribute.KeyValue {
	if len(tags) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(tags))
	for k, v := range tags {
		out = append(out, attribute.String(k, v))
	}
	return out
}

var _ ports.MetricRecorder = (*Recorder)(nil)
