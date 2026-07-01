package ports

import (
	"testing"
	"time"
)

func TestRoutingWeights_Validate(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		w := &RoutingWeights{VolumeScore: 0.2, CostScore: 0.2, ReliabilityScore: 0.2, FXEfficiencyScore: 0.2, LatencyScore: 0.2}
		if err := w.Validate(); err != nil {
			t.Errorf("expected valid, got %v", err)
		}
	})
	t.Run("valid with two zeros", func(t *testing.T) {
		w := &RoutingWeights{VolumeScore: 0.4, CostScore: 0.3, ReliabilityScore: 0.3, FXEfficiencyScore: 0, LatencyScore: 0}
		if err := w.Validate(); err != nil {
			t.Errorf("expected valid, got %v", err)
		}
	})
	t.Run("negative weight", func(t *testing.T) {
		w := &RoutingWeights{VolumeScore: -0.1, CostScore: 0.4, ReliabilityScore: 0.3, FXEfficiencyScore: 0.2, LatencyScore: 0.2}
		if err := w.Validate(); err == nil {
			t.Error("expected error for negative weight")
		}
	})
	t.Run("sum not one", func(t *testing.T) {
		w := &RoutingWeights{VolumeScore: 0.3, CostScore: 0.3, ReliabilityScore: 0.3, FXEfficiencyScore: 0.3, LatencyScore: 0.3}
		if err := w.Validate(); err == nil {
			t.Error("expected error for sum != 1.0")
		}
	})
	t.Run("too many zeros", func(t *testing.T) {
		w := &RoutingWeights{VolumeScore: 0.5, CostScore: 0.5, ReliabilityScore: 0, FXEfficiencyScore: 0, LatencyScore: 0}
		if err := w.Validate(); err == nil {
			t.Error("expected error for more than two zero weights")
		}
	})
}

func TestGatewayFeeModel_CalculateFee(t *testing.T) {
	t.Run("percentage plus fixed", func(t *testing.T) {
		f := &GatewayFeeModel{PercentageBPS: 200, FixedPaise: 100}
		if got := f.CalculateFee(100000, 0); got != 2100 {
			t.Errorf("expected 2100, got %d", got)
		}
	})
	t.Run("interchange cap applied", func(t *testing.T) {
		cap := int64(500)
		f := &GatewayFeeModel{PercentageBPS: 200, FixedPaise: 100, InterchangeCapPaise: &cap}
		if got := f.CalculateFee(100000, 0); got != 500 {
			t.Errorf("expected capped 500, got %d", got)
		}
	})
	t.Run("volume discount applied", func(t *testing.T) {
		f := &GatewayFeeModel{PercentageBPS: 200, FixedPaise: 100, MonthlyDiscountVolumeThresholdPaise: 50000}
		if got := f.CalculateFee(100000, 60000); got != 1995 {
			t.Errorf("expected discounted 1995, got %d", got)
		}
	})
	t.Run("no discount below threshold", func(t *testing.T) {
		f := &GatewayFeeModel{PercentageBPS: 200, FixedPaise: 100, MonthlyDiscountVolumeThresholdPaise: 50000}
		if got := f.CalculateFee(100000, 40000); got != 2100 {
			t.Errorf("expected undiscounted 2100, got %d", got)
		}
	})
}

func TestCircuitBreakerState_IsOpen(t *testing.T) {
	t.Run("open within cooldown", func(t *testing.T) {
		cb := CircuitBreakerState{State: "OPEN", CooldownUntil: time.Now().UTC().Add(time.Minute)}
		if !cb.IsOpen() {
			t.Error("expected open")
		}
	})
	t.Run("open past cooldown", func(t *testing.T) {
		cb := CircuitBreakerState{State: "OPEN", CooldownUntil: time.Now().UTC().Add(-time.Minute)}
		if cb.IsOpen() {
			t.Error("expected not open once cooldown elapsed")
		}
	})
	t.Run("closed", func(t *testing.T) {
		cb := CircuitBreakerState{State: "CLOSED", CooldownUntil: time.Now().UTC().Add(time.Minute)}
		if cb.IsOpen() {
			t.Error("closed breaker is never open")
		}
	})
}
