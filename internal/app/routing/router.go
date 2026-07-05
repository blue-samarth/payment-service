package routing

import (
	"context"
	"fmt"
	"time"

	"samarth/payment-service/internal/app/payment"
	domainrouting "samarth/payment-service/internal/domain/routing"
	"samarth/payment-service/internal/ports"
)

const (
	defaultSnapshotTTLSeconds = 30
	defaultP99SLAMs           = 2000
)

type ConfigSource interface {
	ListActiveGateways(ctx context.Context, paymentMethod string) ([]*ports.GatewayConfig, error)
	GetFeeModel(ctx context.Context, gatewayID, paymentMethod string) (*ports.GatewayFeeModel, error)
	GetRoutingWeights(ctx context.Context, merchantTier string) (*ports.RoutingWeights, error)
}
type BreakerStateReader interface {
	BreakerState(ctx context.Context, gatewayID string) (state string, cooldownUntil time.Time, err error)
}

type Router struct {
	config            ConfigSource
	breaker           BreakerStateReader
	snapshotTTLSecond int
	p99SLAMs          int
}

func NewRouter(config ConfigSource) *Router {
	return &Router{
		config:            config,
		snapshotTTLSecond: defaultSnapshotTTLSeconds,
		p99SLAMs:          defaultP99SLAMs,
	}
}

func (r *Router) SetBreakerState(b BreakerStateReader) { r.breaker = b }

func (r *Router) Route(ctx context.Context, in payment.RouteInput) (*domainrouting.Decision, error) {
	method := string(in.PaymentMethod)

	configs, err := r.config.ListActiveGateways(ctx, method)
	if err != nil {
		return nil, fmt.Errorf("routing: list active gateways: %w", err)
	}
	configs = excludeGateways(configs, in.ExcludeGateways)
	if len(configs) == 0 {
		return nil, domainrouting.ErrNoCandidate{}
	}

	weights, err := r.config.GetRoutingWeights(ctx, in.MerchantTier)
	if err != nil {
		return nil, fmt.Errorf("routing: get weights for tier %q: %w", in.MerchantTier, err)
	}
	if err := weights.Validate(); err != nil {
		return nil, fmt.Errorf("routing: invalid weights for tier %q: %w", in.MerchantTier, err)
	}

	candidates := make([]domainrouting.Candidate, 0, len(configs))
	var maxFee, maxVolume int64
	for _, cfg := range configs {
		fee, err := r.config.GetFeeModel(ctx, cfg.GatewayID, method)
		if err != nil {
			return nil, fmt.Errorf("routing: fee model for %s/%s: %w", cfg.GatewayID, method, err)
		}
		calculatedFee := fee.CalculateFee(in.Amount, cfg.Metrics.Volume7d)

		cbState, cbCooldown := r.liveBreakerState(ctx, cfg.GatewayID, cfg.CircuitBreaker)

		candidates = append(candidates, domainrouting.Candidate{
			GatewayID:           cfg.GatewayID,
			IsActive:            cfg.IsActive,
			MinAmountPaise:      cfg.MinAmount,
			MaxAmountPaise:      cfg.MaxAmount,
			SupportedCurrencies: cfg.SupportedCurrencies,
			CooldownUntil:       cbCooldown,
			CircuitBreakerState: cbState,
			DiscrepancyRate24h:  cfg.Metrics.DiscrepancyRate24h,
			P99LatencyMs:        cfg.Metrics.P99LatencyMs,
			Volume7dPaise:       cfg.Metrics.Volume7d,
			FXEfficiencyRatio:   cfg.Metrics.FXEfficiencyRatio,
			CalculatedFeePaise:  calculatedFee,
			Priority:            cfg.Priority,
		})

		if calculatedFee > maxFee {
			maxFee = calculatedFee
		}
		if cfg.Metrics.Volume7d > maxVolume {
			maxVolume = cfg.Metrics.Volume7d
		}
	}

	for i := range candidates {
		candidates[i].MaxFeePaise = maxFee
	}

	scoringCtx := domainrouting.ScoringContext{
		AmountPaise:     in.Amount,
		Currency:        in.Currency,
		IsDomestic:      in.IsDomestic,
		Volume7dPaise:   maxVolume,
		P99LatencySLAMs: r.p99SLAMs,
	}

	decision, _, err := domainrouting.Select(candidates, scoringCtx, toWeights(weights), r.snapshotTTLSecond)
	if err != nil {
		return nil, err
	}
	return decision, nil
}

func (r *Router) liveBreakerState(ctx context.Context, gatewayID string, fallback ports.CircuitBreakerState) (string, time.Time) {
	if r.breaker == nil {
		return fallback.State, fallback.CooldownUntil
	}
	state, cooldown, err := r.breaker.BreakerState(ctx, gatewayID)
	if err != nil {
		return fallback.State, fallback.CooldownUntil
	}
	if state == "OPEN" && !cooldown.IsZero() && !time.Now().UTC().Before(cooldown) {
		return "HALF_OPEN", time.Time{}
	}
	return state, cooldown
}

func excludeGateways(configs []*ports.GatewayConfig, exclude []string) []*ports.GatewayConfig {
	if len(exclude) == 0 {
		return configs
	}
	skip := make(map[string]struct{}, len(exclude))
	for _, id := range exclude {
		skip[id] = struct{}{}
	}
	out := make([]*ports.GatewayConfig, 0, len(configs))
	for _, cfg := range configs {
		if _, excluded := skip[cfg.GatewayID]; excluded {
			continue
		}
		out = append(out, cfg)
	}
	return out
}

func toWeights(w *ports.RoutingWeights) domainrouting.Weights {
	return domainrouting.Weights{
		Volume:       w.VolumeScore,
		Cost:         w.CostScore,
		Reliability:  w.ReliabilityScore,
		FXEfficiency: w.FXEfficiencyScore,
		Latency:      w.LatencyScore,
	}
}

var _ payment.Router = (*Router)(nil)
