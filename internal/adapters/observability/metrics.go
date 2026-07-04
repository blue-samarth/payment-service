package observability

import "samarth/payment-service/internal/ports"

type NoopMetrics struct{}

func NewNoopMetrics() *NoopMetrics { return &NoopMetrics{} }

func (NoopMetrics) Increment(metric string, tags map[string]string)                {}
func (NoopMetrics) Histogram(metric string, value float64, tags map[string]string) {}
func (NoopMetrics) Gauge(metric string, value float64, tags map[string]string)     {}

var _ ports.MetricRecorder = (*NoopMetrics)(nil)
