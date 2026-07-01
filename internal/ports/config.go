package ports

import (
	"context"
	"fmt"
	"time"
)

type ConfigStore interface {
	GetGatewayConfig(ctx context.Context, gatewayID string) (*GatewayConfig, error)
	ListActiveGateways(ctx context.Context, paymentMethod string) ([]*GatewayConfig, error)
	GetFeeModel(ctx context.Context, gatewayID, paymentMethod string) (*GatewayFeeModel, error)
	GetMetadataSchema(ctx context.Context, gatewayID string) (*GatewayMetadataSchema, error)
	GetRoutingWeights(ctx context.Context, merchantTier string) (*RoutingWeights, error)
	GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error)
}

type GatewayConfig struct {
	GatewayID             string
	DisplayName           string
	IsActive              bool
	MinAmount             int64
	MaxAmount             int64
	SupportedCurrencies   []string
	SupportedMethods      []string
	IdempotencyCapable    bool
	SupportsCancel        bool
	SupportsPartialRefund bool
	EstimatedTimeouts     map[string]time.Duration
	CircuitBreaker        CircuitBreakerState
	Priority              int
	Metrics               GatewayMetrics
	UpdatedAt             time.Time
}

type CircuitBreakerState struct {
	State         string
	CooldownUntil time.Time
}

func (cb CircuitBreakerState) IsOpen() bool {
	return cb.State == "OPEN" && time.Now().UTC().Before(cb.CooldownUntil)
}

type GatewayMetrics struct {
	DiscrepancyRate24h float64
	P99LatencyMs       int
	Volume7d           int64
	FXEfficiencyRatio  float64 // pre-computed mid_rate / worst_rate_7d; 1.0 for domestic
	LastUpdated        time.Time
}

type GatewayFeeModel struct {
	GatewayID                    string
	PaymentMethod                string
	FixedPaise                   int64
	PercentageBPS                int64
	InterchangeCapPaise          *int64
	DiscountVolumeThresholdPaise int64
}

func (f *GatewayFeeModel) CalculateFee(amountPaise, discountVolumePaise int64) int64 {
	fee := (amountPaise*f.PercentageBPS)/10000 + f.FixedPaise

	if f.InterchangeCapPaise != nil && fee > *f.InterchangeCapPaise {
		fee = *f.InterchangeCapPaise
	}

	if f.DiscountVolumeThresholdPaise > 0 &&
		discountVolumePaise > f.DiscountVolumeThresholdPaise {
		fee = (fee * 95) / 100
	}

	return fee
}

type GatewayMetadataSchema struct {
	GatewayID    string
	AllowedKeys  []string
	RequiredKeys []string
	MaxSizeBytes int
}

type RoutingWeights struct {
	MerchantTier      string
	VolumeScore       float64
	CostScore         float64
	ReliabilityScore  float64
	FXEfficiencyScore float64
	LatencyScore      float64
}

func (w *RoutingWeights) Validate() error {
	const tolerance = 0.001

	weights := []float64{w.VolumeScore, w.CostScore, w.ReliabilityScore, w.FXEfficiencyScore, w.LatencyScore}

	for _, v := range weights {
		if v < 0 {
			return fmt.Errorf("routing weights must be >= 0, got %f", v)
		}
	}

	sum := w.VolumeScore + w.CostScore + w.ReliabilityScore +
		w.FXEfficiencyScore + w.LatencyScore
	if sum < 1.0-tolerance || sum > 1.0+tolerance {
		return fmt.Errorf("routing weights must sum to 1.00, got %.4f", sum)
	}

	zeros := 0
	for _, v := range weights {
		if v == 0 {
			zeros++
		}
	}
	if zeros > 2 {
		return fmt.Errorf("at most 2 routing weights may be zero, got %d", zeros)
	}

	return nil
}
