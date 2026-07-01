package routing

import (
	"fmt"
	"time"
)

type Decision struct {
	SelectedGateway string
	Score           int // 0–10000 (scaled by 100 for integer precision)
	Snapshot        Snapshot
	DecidedAt       time.Time
	TTLSeconds      int
}

type Snapshot struct {
	GatewayID         string
	SuccessRate       float64
	ErrorRate         float64
	P99LatencyMs      int
	DiscrepancyRate   float64
	CostScore         int
	ReliabilityScore  int
	FXEfficiencyScore int
	LatencyScore      int
	VolumeScore       int
	CompositeScore    int
	SnapshotAt        time.Time
}

type Candidate struct {
	GatewayID                 string
	IsActive                  bool
	MinAmountPaise            int64
	MaxAmountPaise            int64
	SupportedCurrencies       []string
	CooldownUntil             time.Time
	CircuitBreakerState       string
	LastKnownReliabilityScore int
	DiscrepancyRate24h        float64
	P99LatencyMs              int
	SuccessRate               float64
	ErrorRate                 float64
	Volume7dPaise             int64
	FXEfficiencyRatio         float64 // pre-computed mid_rate / worst_rate_7d; 1.0 for domestic
	CalculatedFeePaise        int64
	MaxFeePaise               int64 // max fee across all candidates in this scoring set
	ActivePaymentIntents      int
	Priority                  int
}

type FilterResult struct {
	GatewayID string
	Excluded  bool
	Reason    string
}

type Weights struct {
	Volume       float64
	Cost         float64
	Reliability  float64
	FXEfficiency float64
	Latency      float64
}

type ScoringContext struct {
	AmountPaise     int64
	Currency        string
	IsDomestic      bool
	Volume7dPaise   int64
	P99LatencySLAMs int
}

type ErrNoCandidate struct{ Filters []FilterResult }

func (e ErrNoCandidate) Error() string {
	return "routing: no eligible gateway candidate after filtering"
}

func (d *Decision) IsExpired() bool {
	return time.Now().UTC().After(d.DecidedAt.Add(time.Duration(d.TTLSeconds) * time.Second))
}

func Score(c Candidate, ctx ScoringContext, w Weights) (int, Snapshot) {
	volume := volumeScore(c.Volume7dPaise, ctx.Volume7dPaise)
	cost := costScore(c.CalculatedFeePaise, c.MaxFeePaise)
	reliability := reliabilityScore(c.DiscrepancyRate24h, c.CircuitBreakerState, c.LastKnownReliabilityScore)
	fx := fxEfficiencyScore(c.FXEfficiencyRatio, ctx.IsDomestic)
	latency := latencyScore(c.P99LatencyMs, ctx.P99LatencySLAMs)

	composite := int(
		w.Volume*float64(volume)*100 +
			w.Cost*float64(cost)*100 +
			w.Reliability*float64(reliability)*100 +
			w.FXEfficiency*float64(fx)*100 +
			w.Latency*float64(latency)*100,
	)

	snap := Snapshot{
		GatewayID:         c.GatewayID,
		SuccessRate:       c.SuccessRate,
		ErrorRate:         c.ErrorRate,
		P99LatencyMs:      c.P99LatencyMs,
		DiscrepancyRate:   c.DiscrepancyRate24h,
		CostScore:         cost,
		ReliabilityScore:  reliability,
		FXEfficiencyScore: fx,
		LatencyScore:      latency,
		VolumeScore:       volume,
		CompositeScore:    composite,
		SnapshotAt:        time.Now().UTC(),
	}

	return composite, snap
}

func Select(
	candidates []Candidate,
	ctx ScoringContext,
	w Weights,
	ttlSeconds int,
) (*Decision, []FilterResult, error) {
	filters, surviving := filter(candidates, ctx)

	if len(surviving) == 0 {
		return nil, filters, ErrNoCandidate{Filters: filters}
	}

	type scored struct {
		candidate Candidate
		score     int
		snapshot  Snapshot
	}

	results := make([]scored, 0, len(surviving))
	for _, c := range surviving {
		s, snap := Score(c, ctx, w)
		results = append(results, scored{c, s, snap})
	}

	best := results[0]
	for _, r := range results[1:] {
		if r.score > best.score {
			best = r
			continue
		}
		if best.score-r.score <= 100 { // within 1 point (scaled by 100)
			if r.candidate.ActivePaymentIntents < best.candidate.ActivePaymentIntents {
				best = r
				continue
			}
			if r.candidate.ActivePaymentIntents == best.candidate.ActivePaymentIntents &&
				r.candidate.GatewayID < best.candidate.GatewayID {
				best = r
			}
		}
	}

	return &Decision{
		SelectedGateway: best.candidate.GatewayID,
		Score:           best.score,
		Snapshot:        best.snapshot,
		DecidedAt:       time.Now().UTC(),
		TTLSeconds:      ttlSeconds,
	}, filters, nil
}

func filter(candidates []Candidate, ctx ScoringContext) ([]FilterResult, []Candidate) {
	results := make([]FilterResult, 0, len(candidates))
	surviving := make([]Candidate, 0, len(candidates))

	for _, c := range candidates {
		reason := filterReason(c, ctx)
		results = append(results, FilterResult{
			GatewayID: c.GatewayID,
			Excluded:  reason != "",
			Reason:    reason,
		})
		if reason == "" {
			surviving = append(surviving, c)
		}
	}

	return results, surviving
}

func filterReason(c Candidate, ctx ScoringContext) string {
	if !c.IsActive {
		return "inactive"
	}
	if ctx.AmountPaise < c.MinAmountPaise || (c.MaxAmountPaise > 0 && ctx.AmountPaise > c.MaxAmountPaise) {
		return fmt.Sprintf("amount_out_of_range_%d", ctx.AmountPaise)
	}
	if !supportsCurrency(c.SupportedCurrencies, ctx.Currency) {
		return fmt.Sprintf("currency_not_supported_%s", ctx.Currency)
	}
	if !c.CooldownUntil.IsZero() && time.Now().UTC().Before(c.CooldownUntil) {
		return "circuit_breaker_cooldown"
	}
	if c.CircuitBreakerState == "OPEN" {
		return "circuit_breaker_open"
	}
	if c.DiscrepancyRate24h > 0.20 {
		return fmt.Sprintf("discrepancy_rate_exceeded_%.2f", c.DiscrepancyRate24h)
	}
	return ""
}

func supportsCurrency(supported []string, currency string) bool {
	for _, c := range supported {
		if c == currency {
			return true
		}
	}
	return false
}

func volumeScore(gatewayVolume7d, highestVolume7d int64) int {
	if highestVolume7d == 0 || gatewayVolume7d == 0 {
		return 0
	}
	return clamp(int((gatewayVolume7d*100)/highestVolume7d), 0, 100)
}

func costScore(calculatedFee, maxFee int64) int {
	if maxFee == 0 {
		return 100
	}
	return clamp(int(((maxFee-calculatedFee)*100)/maxFee), 0, 100)
}

func reliabilityScore(discrepancyRate24h float64, circuitBreakerState string, lastKnown int) int {
	if circuitBreakerState == "OPEN" {
		return clamp(lastKnown, 0, 100)
	}
	if discrepancyRate24h > 0.20 {
		return 0
	}
	return clamp(int((1.0-discrepancyRate24h)*100), 0, 100)
}

func fxEfficiencyScore(fxEfficiencyRatio float64, isDomestic bool) int {
	if isDomestic {
		return 100
	}
	return clamp(int(fxEfficiencyRatio*100), 0, 100)
}

func latencyScore(p99Ms, slaMs int) int {
	if slaMs <= 0 {
		return 0
	}
	return clamp(int(float64(slaMs-p99Ms)/float64(slaMs)*100), 0, 100)
}

func clamp(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
