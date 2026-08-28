//go:build performance

package performance

import (
	"testing"
	"time"

	"samarth/payment-service/internal/domain/routing"
)

func candidate(id string) routing.Candidate {
	return routing.Candidate{
		GatewayID:                 id,
		IsActive:                  true,
		MinAmountPaise:            100,
		MaxAmountPaise:            100_000_000,
		SupportedCurrencies:       []string{"INR", "USD"},
		CircuitBreakerState:       "CLOSED",
		LastKnownReliabilityScore: 9200,
		DiscrepancyRate24h:        0.001,
		P99LatencyMs:              420,
		SuccessRate:               0.978,
		ErrorRate:                 0.022,
		Volume7dPaise:             85_000_000,
		FXEfficiencyRatio:         1.0,
		CalculatedFeePaise:        1850,
		MaxFeePaise:               4000,
		ActivePaymentIntents:      12,
		Priority:                  1,
	}
}

func scoringContext() routing.ScoringContext {
	return routing.ScoringContext{
		AmountPaise:     250_000,
		Currency:        "INR",
		IsDomestic:      true,
		Volume7dPaise:   85_000_000,
		P99LatencySLAMs: 800,
	}
}

func weights() routing.Weights {
	return routing.Weights{
		Volume:       0.15,
		Cost:         0.30,
		Reliability:  0.30,
		FXEfficiency: 0.10,
		Latency:      0.15,
	}
}

func TestPerf_RoutingScore_SingleCandidate(t *testing.T) {
	c := candidate("stripe")
	ctx := scoringContext()
	w := weights()

	enforce(t, "routing.Score", Baseline{NsPerOp: 42, AllocsPerOp: 0, BytesPerOp: 0}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			score, snap := routing.Score(c, ctx, w)
			if score < 0 || snap.GatewayID == "" {
				b.Fatal("unexpected scoring result")
			}
		}
	})
}

func TestPerf_RoutingScore_FullCandidateSet(t *testing.T) {
	ids := []string{"stripe", "razorpay", "payu", "upi", "cashfree", "billdesk"}
	cands := make([]routing.Candidate, 0, len(ids))
	for _, id := range ids {
		cands = append(cands, candidate(id))
	}
	ctx := scoringContext()
	w := weights()

	enforce(t, "routing.Score x6 (one payment)", Baseline{NsPerOp: 292, AllocsPerOp: 0, BytesPerOp: 0}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			best := -1
			bestID := ""
			for _, c := range cands {
				score, snap := routing.Score(c, ctx, w)
				if score > best {
					best, bestID = score, snap.GatewayID
				}
			}
			if bestID == "" {
				b.Fatal("no gateway selected")
			}
		}
	})
}

func TestPerf_DecisionIsExpired(t *testing.T) {
	d := &routing.Decision{
		SelectedGateway: "stripe",
		Score:           8800,
		DecidedAt:       time.Now(),
		TTLSeconds:      30,
	}

	enforce(t, "routing.Decision.IsExpired", Baseline{NsPerOp: 29, AllocsPerOp: 0, BytesPerOp: 0}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			sinkBool = d.IsExpired()
		}
	})
}
