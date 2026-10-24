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

type fakeProbe struct {
	free  map[string]bool
	err   error
	calls []string
}

func (p *fakeProbe) AcquireProbe(_ context.Context, gatewayID string, _ time.Duration) (bool, error) {
	p.calls = append(p.calls, gatewayID)
	if p.err != nil {
		return false, p.err
	}
	ok := p.free[gatewayID]
	p.free[gatewayID] = false
	return ok, nil
}

type halfOpenGateways map[string]bool

func (h halfOpenGateways) BreakerState(_ context.Context, gatewayID string) (string, time.Time, error) {
	if h[gatewayID] {
		return "OPEN", time.Now().UTC().Add(-time.Minute), nil
	}
	return "CLOSED", time.Time{}, nil
}

func TestRouter_HalfOpenAdmitsASingleProbe(t *testing.T) {
	cfg := &fakeConfig{gateways: []*ports.GatewayConfig{gatewayConfig("g1", 0)}, weights: equalWeights()}
	r := NewRouter(cfg)
	r.SetBreakerState(halfOpenGateways{"g1": true})
	probe := &fakeProbe{free: map[string]bool{"g1": true}}
	r.SetBreakerProbe(probe, time.Minute)

	if d, err := r.Route(context.Background(), validInput()); err != nil || d.SelectedGateway != "g1" {
		t.Fatalf("the first request after the cooldown must probe g1, got %v %v", d, err)
	}
	var none domainrouting.ErrNoCandidate
	if _, err := r.Route(context.Background(), validInput()); !errors.As(err, &none) {
		t.Fatalf("while the probe is out, a half-open gateway must not take other traffic, got %v", err)
	}
}

func TestRouter_LosingTheProbeFallsBackToAnotherGateway(t *testing.T) {
	healthy := gatewayConfig("healthy", 0)
	healthy.Metrics.P99LatencyMs = 1900
	healthy.Metrics.DiscrepancyRate24h = 0.15
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("recovering", 0), healthy},
		weights:  equalWeights(),
	}
	r := NewRouter(cfg)
	r.SetBreakerState(halfOpenGateways{"recovering": true})
	probe := &fakeProbe{free: map[string]bool{}}
	r.SetBreakerProbe(probe, time.Minute)

	d, err := r.Route(context.Background(), validInput())
	if err != nil {
		t.Fatal(err)
	}
	if d.SelectedGateway != "healthy" {
		t.Fatalf("with the probe held elsewhere, routing must pick another gateway, got %s", d.SelectedGateway)
	}
	if len(probe.calls) != 1 || probe.calls[0] != "recovering" {
		t.Fatalf("expected exactly one probe attempt, on the better-scored half-open gateway, got %v", probe.calls)
	}
}

func TestRouter_ProbeStoreErrorFailsOpen(t *testing.T) {
	cfg := &fakeConfig{gateways: []*ports.GatewayConfig{gatewayConfig("g1", 0)}, weights: equalWeights()}
	r := NewRouter(cfg)
	r.SetBreakerState(halfOpenGateways{"g1": true})
	r.SetBreakerProbe(&fakeProbe{err: errors.New("redis down"), free: map[string]bool{}}, time.Minute)

	if _, err := r.Route(context.Background(), validInput()); err != nil {
		t.Fatalf("a probe-store error must not take routing down, got %v", err)
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

type fakeIntents struct {
	counts map[string]int
	err    error
}

func (f fakeIntents) ActiveIntents(_ context.Context, gatewayIDs []string) (map[string]int, error) {
	return f.counts, f.err
}

func TestRouter_ActiveIntentsTieBreakToLowerLoad(t *testing.T) {
	// Two identical-score gateways: the lexicographic tie-break would pick
	// "alpha", but the live in-flight load makes "beta" the lighter choice.
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("alpha", 0), gatewayConfig("beta", 0)},
		weights:  equalWeights(),
	}
	r := NewRouter(cfg)
	r.SetActiveIntents(fakeIntents{counts: map[string]int{"alpha": 10, "beta": 2}})

	decision, err := r.Route(context.Background(), validInput())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if decision.SelectedGateway != "beta" {
		t.Errorf("expected lower-load beta to win the tie, got %s", decision.SelectedGateway)
	}
}

func TestRouter_ActiveIntentsReaderErrorFailsOpen(t *testing.T) {
	// An intents-store outage must not take routing down; the load signal is
	// simply skipped and the tie falls back to lexicographic order.
	cfg := &fakeConfig{
		gateways: []*ports.GatewayConfig{gatewayConfig("alpha", 0), gatewayConfig("beta", 0)},
		weights:  equalWeights(),
	}
	r := NewRouter(cfg)
	r.SetActiveIntents(fakeIntents{err: errors.New("redis down")})

	decision, err := r.Route(context.Background(), validInput())
	if err != nil {
		t.Fatalf("intents reader error should not fail routing, got %v", err)
	}
	if decision.SelectedGateway != "alpha" {
		t.Errorf("expected lexicographic fallback to alpha, got %s", decision.SelectedGateway)
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

// Availability backs /health-gateways. It must never disagree with Route: a
// gateway reported available has to be one Route would actually select.
func TestAvailability_AgreesWithRoute(t *testing.T) {
	now := time.Now().UTC()
	cases := []struct {
		name        string
		active      bool
		discrepancy float64
		breaker     fakeBreakerState
		wantState   string
		wantReason  string
		wantErr     bool
	}{
		{"closed", true, 0.01, fakeBreakerState{state: "CLOSED"}, "CLOSED", "", false},
		{"open within cooldown", true, 0.01, fakeBreakerState{state: "OPEN", cooldown: now.Add(time.Hour)}, "OPEN", "circuit_breaker_cooldown", false},
		{"open past cooldown probes half-open", true, 0.01, fakeBreakerState{state: "OPEN", cooldown: now.Add(-time.Hour)}, "HALF_OPEN", "", false},
		{"open without cooldown", true, 0.01, fakeBreakerState{state: "OPEN"}, "OPEN", "circuit_breaker_open", false},
		{"breaker store down falls back to config", true, 0.01, fakeBreakerState{err: errors.New("redis down")}, "CLOSED", "", true},
		{"inactive", false, 0.01, fakeBreakerState{state: "CLOSED"}, "CLOSED", "inactive", false},
		{"discrepancy too high", true, 0.30, fakeBreakerState{state: "CLOSED"}, "CLOSED", "discrepancy_rate_exceeded_0.30", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := gatewayConfig("g1", 0)
			cfg.IsActive = tc.active
			cfg.Metrics.DiscrepancyRate24h = tc.discrepancy
			r := NewRouter(&fakeConfig{gateways: []*ports.GatewayConfig{cfg}, weights: equalWeights()})
			r.SetBreakerState(tc.breaker)

			avail, err := r.Availability(context.Background(), cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("breaker error = %v, want error %v", err, tc.wantErr)
			}
			if avail.BreakerState != tc.wantState || avail.Reason != tc.wantReason {
				t.Fatalf("got state=%q reason=%q, want state=%q reason=%q", avail.BreakerState, avail.Reason, tc.wantState, tc.wantReason)
			}

			_, routeErr := r.Route(context.Background(), validInput())
			if routable := routeErr == nil; routable != (avail.Reason == "") {
				t.Errorf("Availability (reason %q) and Route (err %v) disagree", avail.Reason, routeErr)
			}
		})
	}
}
