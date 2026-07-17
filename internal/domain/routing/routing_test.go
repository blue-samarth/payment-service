package routing

import (
	"errors"
	"testing"
	"time"
)

func baseCandidate(id string) Candidate {
	return Candidate{
		GatewayID:           id,
		IsActive:            true,
		MinAmountPaise:      0,
		MaxAmountPaise:      1000000,
		SupportedCurrencies: []string{"INR"},
		CooldownUntil:       time.Time{},
		CircuitBreakerState: "CLOSED",
		DiscrepancyRate24h:  0.01,
		P99LatencyMs:        100,
		Volume7dPaise:       500000,
		FXEfficiencyRatio:   1.0,
		CalculatedFeePaise:  1000,
		MaxFeePaise:         2000,
	}
}

func baseContext() ScoringContext {
	return ScoringContext{
		AmountPaise:     50000,
		Currency:        "INR",
		IsDomestic:      true,
		Volume7dPaise:   1000000,
		P99LatencySLAMs: 500,
	}
}

func equalWeights() Weights {
	return Weights{Volume: 0.2, Cost: 0.2, Reliability: 0.2, FXEfficiency: 0.2, Latency: 0.2}
}

func TestScore_InRange(t *testing.T) {
	score, snap := Score(baseCandidate("g"), baseContext(), equalWeights())
	if score < 0 || score > 10000 {
		t.Errorf("composite score out of range [0,10000]: %d", score)
	}
	if snap.GatewayID != "g" {
		t.Errorf("snapshot gateway mismatch: %s", snap.GatewayID)
	}
	for _, c := range []int{snap.CostScore, snap.ReliabilityScore, snap.FXEfficiencyScore, snap.LatencyScore, snap.VolumeScore} {
		if c < 0 || c > 100 {
			t.Errorf("component score out of range [0,100]: %d", c)
		}
	}
}

func TestScore_DomesticFXIsMax(t *testing.T) {
	_, snap := Score(baseCandidate("g"), baseContext(), equalWeights())
	if snap.FXEfficiencyScore != 100 {
		t.Errorf("domestic FX score should be 100, got %d", snap.FXEfficiencyScore)
	}
}

func TestScore_ZeroMaxFeeGivesFullCostScore(t *testing.T) {
	c := baseCandidate("g")
	c.MaxFeePaise = 0
	_, snap := Score(c, baseContext(), equalWeights())
	if snap.CostScore != 100 {
		t.Errorf("zero max fee should yield cost score 100, got %d", snap.CostScore)
	}
}

func TestSelect_FiltersIneligible(t *testing.T) {
	ctx := baseContext()

	cases := []struct {
		name   string
		mutate func(c *Candidate)
		reason string
	}{
		{"inactive", func(c *Candidate) { c.IsActive = false }, "inactive"},
		{"amount below min", func(c *Candidate) { c.MinAmountPaise = 60000 }, "amount_out_of_range"},
		{"amount above max", func(c *Candidate) { c.MaxAmountPaise = 40000 }, "amount_out_of_range"},
		{"currency unsupported", func(c *Candidate) { c.SupportedCurrencies = []string{"USD"} }, "currency_not_supported"},
		{"cooldown active", func(c *Candidate) { c.CooldownUntil = time.Now().UTC().Add(time.Hour) }, "circuit_breaker_cooldown"},
		{"circuit open", func(c *Candidate) { c.CircuitBreakerState = "OPEN" }, "circuit_breaker_open"},
		{"discrepancy too high", func(c *Candidate) { c.DiscrepancyRate24h = 0.30 }, "discrepancy_rate_exceeded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := baseCandidate("g")
			tc.mutate(&c)
			_, filters, err := Select([]Candidate{c}, ctx, equalWeights(), 30)
			if !errors.As(err, &ErrNoCandidate{}) {
				t.Fatalf("expected ErrNoCandidate, got %v", err)
			}
			if len(filters) != 1 || !filters[0].Excluded {
				t.Fatalf("expected one excluded filter result, got %+v", filters)
			}
			if got := filters[0].Reason; len(got) < len(tc.reason) || got[:len(tc.reason)] != tc.reason {
				t.Errorf("expected reason prefixed %q, got %q", tc.reason, got)
			}
		})
	}
}

func TestSelect_PicksHighestScore(t *testing.T) {
	cheap := baseCandidate("cheap")
	cheap.CalculatedFeePaise = 100

	expensive := baseCandidate("expensive")
	expensive.CalculatedFeePaise = 1900

	decision, _, err := Select([]Candidate{expensive, cheap}, baseContext(), equalWeights(), 30)
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedGateway != "cheap" {
		t.Errorf("expected cheaper gateway to win on cost, got %s", decision.SelectedGateway)
	}
}

func TestSelect_TieBreakLowerActiveIntents(t *testing.T) {
	a := baseCandidate("alpha")
	a.ActivePaymentIntents = 5
	b := baseCandidate("beta")
	b.ActivePaymentIntents = 2

	decision, _, err := Select([]Candidate{a, b}, baseContext(), equalWeights(), 30)
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedGateway != "beta" {
		t.Errorf("tie should break to fewer active intents (beta), got %s", decision.SelectedGateway)
	}
}

func TestSelect_TieBreakDoesNotWalkBelowGlobalMax(t *testing.T) {
	// Descending scores with descending load. Each consecutive gap is <= 100
	// (80), but the cumulative drop from top to bottom is 160. The tie-break
	// must only trade load for score within 100 of the global max, so gamma
	// (160 below max) must never be selected even though it has the lowest load.
	alpha := baseCandidate("alpha") // p99 100 -> composite 7580, highest load
	alpha.P99LatencyMs = 100
	alpha.ActivePaymentIntents = 30

	beta := baseCandidate("beta") // p99 120 -> composite 7500 (80 below max)
	beta.P99LatencyMs = 120
	beta.ActivePaymentIntents = 20

	gamma := baseCandidate("gamma") // p99 140 -> composite 7420 (160 below max)
	gamma.P99LatencyMs = 140
	gamma.ActivePaymentIntents = 10

	decision, _, err := Select([]Candidate{alpha, beta, gamma}, baseContext(), equalWeights(), 30)
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedGateway == "gamma" {
		t.Fatalf("selected gamma, which is 160 below the top score; tie-break walked below the global max")
	}
	if decision.SelectedGateway != "beta" {
		t.Errorf("expected beta (lowest load within 100 of the top score), got %s", decision.SelectedGateway)
	}
}

func TestSelect_TieBreakLexicographic(t *testing.T) {
	zzz := baseCandidate("zzz")
	aaa := baseCandidate("aaa")

	decision, _, err := Select([]Candidate{zzz, aaa}, baseContext(), equalWeights(), 30)
	if err != nil {
		t.Fatal(err)
	}
	if decision.SelectedGateway != "aaa" {
		t.Errorf("full tie should break lexicographically (aaa), got %s", decision.SelectedGateway)
	}
}

func TestSelect_NoCandidatesAtAll(t *testing.T) {
	_, _, err := Select(nil, baseContext(), equalWeights(), 30)
	if !errors.As(err, &ErrNoCandidate{}) {
		t.Fatalf("expected ErrNoCandidate, got %v", err)
	}
}

func TestDecision_IsExpired(t *testing.T) {
	expired := &Decision{DecidedAt: time.Now().UTC().Add(-time.Minute), TTLSeconds: 30}
	if !expired.IsExpired() {
		t.Error("expected decision to be expired")
	}
	fresh := &Decision{DecidedAt: time.Now().UTC(), TTLSeconds: 30}
	if fresh.IsExpired() {
		t.Error("expected decision to be fresh")
	}
}
