package routing

import (
	"context"
	"errors"
	"testing"
	"time"

	"samarth/payment-service/internal/app/payment"
	domainrouting "samarth/payment-service/internal/domain/routing"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type fakeBreakerState struct {
	state    string
	cooldown time.Time
	err      error
}

func (f fakeBreakerState) BreakerState(context.Context, string) (string, time.Time, error) {
	return f.state, f.cooldown, f.err
}

type fakeConfig struct {
	gateways []*ports.GatewayConfig
	fees     map[string]*ports.GatewayFeeModel
	weights  *ports.RoutingWeights
	listErr  error
}

func (f *fakeConfig) ListActiveGateways(ctx context.Context, method string) ([]*ports.GatewayConfig, error) {
	return f.gateways, f.listErr
}

func (f *fakeConfig) GetFeeModel(ctx context.Context, gatewayID, method string) (*ports.GatewayFeeModel, error) {
	if m, ok := f.fees[gatewayID]; ok {
		return m, nil
	}
	return &ports.GatewayFeeModel{}, nil
}

func (f *fakeConfig) GetRoutingWeights(ctx context.Context, tier string) (*ports.RoutingWeights, error) {
	return f.weights, nil
}

func gatewayConfig(id string, fee int64) *ports.GatewayConfig {
	return &ports.GatewayConfig{
		GatewayID:           id,
		IsActive:            true,
		MinAmount:           0,
		MaxAmount:           10000000,
		SupportedCurrencies: []string{"INR"},
		CircuitBreaker:      ports.CircuitBreakerState{State: "CLOSED"},
		Metrics: ports.GatewayMetrics{
			DiscrepancyRate24h: 0.01,
			P99LatencyMs:       100,
			Volume7d:           500000,
			FXEfficiencyRatio:  1.0,
		},
	}
}

func equalWeights() *ports.RoutingWeights {
	return &ports.RoutingWeights{
		VolumeScore: 0.2, CostScore: 0.2, ReliabilityScore: 0.2, FXEfficiencyScore: 0.2, LatencyScore: 0.2,
	}
}

func validInput() payment.RouteInput {
	return payment.RouteInput{
		Amount:        50000,
		Currency:      "INR",
		PaymentMethod: transaction.PaymentMethodCard,
		MerchantTier:  "standard",
		IsDomestic:    true,
	}
}

func TestRouter_SelectsCheapestGateway(t *testing.T) {
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("expensive", 0), gatewayConfig("cheap", 0)},
		fees: map[string]*ports.GatewayFeeModel{
			"expensive": {PercentageBPS: 300},
			"cheap":     {PercentageBPS: 50},
		},
		weights: equalWeights(),
	}
	r := NewRouter(cfg)

	decision, err := r.Route(context.Background(), validInput())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision.SelectedGateway != "cheap" {
		t.Errorf("expected cheapest gateway selected, got %s", decision.SelectedGateway)
	}
}

func TestRouter_NoActiveGateways(t *testing.T) {
	r := NewRouter(&fakeConfig{gateways: nil, weights: equalWeights()})
	_, err := r.Route(context.Background(), validInput())
	if !errors.As(err, &domainrouting.ErrNoCandidate{}) {
		t.Fatalf("expected ErrNoCandidate, got %v", err)
	}
}

func TestRouter_AllFilteredOut(t *testing.T) {
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("g1", 0)},
		weights:  equalWeights(),
	}
	cfg.gateways[0].SupportedCurrencies = []string{"USD"}
	r := NewRouter(cfg)

	_, err := r.Route(context.Background(), validInput())
	if !errors.As(err, &domainrouting.ErrNoCandidate{}) {
		t.Fatalf("expected ErrNoCandidate when all filtered, got %v", err)
	}
}

func TestRouter_InvalidWeightsRejected(t *testing.T) {
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("g1", 0)},
		weights:  &ports.RoutingWeights{VolumeScore: 0.5},
	}
	r := NewRouter(cfg)

	if _, err := r.Route(context.Background(), validInput()); err == nil {
		t.Error("expected error for weights not summing to 1.0")
	}
}

func TestRouter_ProducesFreshDecisionTTL(t *testing.T) {
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("g1", 0)},
		weights:  equalWeights(),
	}
	r := NewRouter(cfg)

	decision, err := r.Route(context.Background(), validInput())
	if err != nil {
		t.Fatal(err)
	}
	if decision.IsExpired() {
		t.Error("freshly produced decision should not be expired")
	}
	if decision.TTLSeconds != defaultSnapshotTTLSeconds {
		t.Errorf("expected TTL %d, got %d", defaultSnapshotTTLSeconds, decision.TTLSeconds)
	}
}

func TestRouter_LiveOpenBreakerExcludesGateway(t *testing.T) {
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("g1", 0)},
		weights:  equalWeights(),
	}
	r := NewRouter(cfg)
	// The config snapshot says CLOSED, but the live breaker is OPEN with an
	// active cooldown — the gateway must be filtered out.
	r.SetBreakerState(fakeBreakerState{state: "OPEN", cooldown: time.Now().UTC().Add(time.Hour)})

	if _, err := r.Route(context.Background(), validInput()); !errors.As(err, &domainrouting.ErrNoCandidate{}) {
		t.Fatalf("expected ErrNoCandidate when the only gateway's live breaker is OPEN, got %v", err)
	}
}

func TestRouter_LiveOpenBreakerReroutesToHealthy(t *testing.T) {
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("down", 0), gatewayConfig("healthy", 0)},
		weights:  equalWeights(),
	}
	r := NewRouter(cfg)
	r.SetBreakerState(stateByGateway{open: map[string]bool{"down": true}})

	decision, err := r.Route(context.Background(), validInput())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision.SelectedGateway != "healthy" {
		t.Errorf("an OPEN breaker on 'down' should route to 'healthy', got %s", decision.SelectedGateway)
	}
}

func TestRouter_ExpiredCooldownProbesHalfOpen(t *testing.T) {
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("g1", 0)},
		weights:  equalWeights(),
	}
	r := NewRouter(cfg)
	// OPEN but the cooldown has elapsed: routing should allow a probe so the
	// breaker can recover, rather than excluding the gateway forever.
	r.SetBreakerState(fakeBreakerState{state: "OPEN", cooldown: time.Now().UTC().Add(-time.Hour)})

	decision, err := r.Route(context.Background(), validInput())
	if err != nil {
		t.Fatalf("expected a probe to be routable after cooldown, got %v", err)
	}
	if decision.SelectedGateway != "g1" {
		t.Errorf("expected g1 to be probed, got %s", decision.SelectedGateway)
	}
}

func TestRouter_BreakerReaderErrorFailsOpen(t *testing.T) {
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("g1", 0)},
		weights:  equalWeights(),
	}
	r := NewRouter(cfg)
	// A breaker-store outage must not take routing down; fall back to config (CLOSED).
	r.SetBreakerState(fakeBreakerState{err: errors.New("redis down")})

	if _, err := r.Route(context.Background(), validInput()); err != nil {
		t.Fatalf("breaker reader error should fail open to config, got %v", err)
	}
}

type stateByGateway struct {
	open map[string]bool
}

func (s stateByGateway) BreakerState(_ context.Context, gatewayID string) (string, time.Time, error) {
	if s.open[gatewayID] {
		return "OPEN", time.Now().UTC().Add(time.Hour), nil
	}
	return "CLOSED", time.Time{}, nil
}
