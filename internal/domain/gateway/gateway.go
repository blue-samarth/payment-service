package gateway

import (
	"fmt"
	"math"
	"time"
)

type CircuitState string
type AlertLevel string

type ErrInvalidTransition struct {
	From CircuitState
	To   CircuitState
}

type CircuitBreaker struct {
	GatewayID                 string
	State                     CircuitState
	CooldownUntil             time.Time
	ConsecutiveFailures       int
	LastKnownReliabilityScore int
}

type DiscrepancyMetrics struct {
	GatewayID            string
	Rate5Min             float64
	Rate30Min            float64
	Rate24H              float64
	DaysSinceDiscrepancy int
	LastDiscrepancyAt    *time.Time
	LastUpdatedAt        time.Time
}

const (
	AlertLevelNone        AlertLevel = "none"
	AlertLevelInvestigate AlertLevel = "investigate"
	AlertLevelAlert       AlertLevel = "alert"
	AlertLevelAutoDisable AlertLevel = "auto_disable"
)

const (
	StateClosed   CircuitState = "CLOSED"
	StateOpen     CircuitState = "OPEN"
	StateHalfOpen CircuitState = "HALF_OPEN"
)

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("circuit breaker: invalid transition %s → %s", e.From, e.To)
}

var transitionTable = map[CircuitState][]CircuitState{
	StateClosed:   {StateOpen},
	StateOpen:     {StateHalfOpen},
	StateHalfOpen: {StateClosed, StateOpen},
}

func CooldownDuration(consecutiveFailures int) time.Duration {
	if consecutiveFailures <= 0 {
		consecutiveFailures = 1
	}
	base := 60 * time.Second
	d := base * time.Duration(1<<(consecutiveFailures-1))
	max := 240 * time.Second
	if d > max {
		return max
	}
	return d
}

func (cb *CircuitBreaker) Transition(to CircuitState) error {
	allowed, ok := transitionTable[cb.State]
	if !ok {
		return ErrInvalidTransition{From: cb.State, To: to}
	}
	valid := false
	for _, s := range allowed {
		if s == to {
			valid = true
			break
		}
	}
	if !valid {
		return ErrInvalidTransition{From: cb.State, To: to}
	}

	from := cb.State
	cb.State = to

	switch {
	case from == StateClosed && to == StateOpen:
		cb.ConsecutiveFailures++
		cb.CooldownUntil = time.Now().UTC().Add(CooldownDuration(cb.ConsecutiveFailures))

	case from == StateOpen && to == StateHalfOpen:
		cb.CooldownUntil = time.Time{}

	case from == StateHalfOpen && to == StateClosed:
		cb.ConsecutiveFailures = 0
		cb.CooldownUntil = time.Time{}

	case from == StateHalfOpen && to == StateOpen:
		cb.ConsecutiveFailures++
		cb.CooldownUntil = time.Now().UTC().Add(CooldownDuration(cb.ConsecutiveFailures))
	}

	return nil
}

func (cb *CircuitBreaker) IsRoutable() bool {
	return cb.State == StateClosed || cb.State == StateHalfOpen
}

func (cb *CircuitBreaker) ShouldTransitionToHalfOpen() bool {
	return cb.State == StateOpen && time.Now().UTC().After(cb.CooldownUntil)
}

func (d *DiscrepancyMetrics) EffectiveRate() float64 {
	if d.DaysSinceDiscrepancy < 1 {
		return d.Rate24H
	}
	decayFactor := math.Pow(0.5, float64(d.DaysSinceDiscrepancy-1)/7.0)
	return d.Rate24H * decayFactor
}

func (d *DiscrepancyMetrics) IsResolved() bool {
	return d.DaysSinceDiscrepancy >= 1 && d.EffectiveRate() < 0.001
}

func (d *DiscrepancyMetrics) ReliabilityScore() int {
	if d.Rate24H > 0.20 {
		return 0
	}
	score := int((1.0 - d.Rate24H) * 100)
	if score > 100 {
		return 100
	}
	return score
}

func (d *DiscrepancyMetrics) CurrentAlertLevel() AlertLevel {
	switch {
	case d.Rate5Min > 0.20:
		return AlertLevelAutoDisable
	case d.Rate5Min > 0.05:
		return AlertLevelAlert
	case d.Rate24H > 0.02:
		return AlertLevelInvestigate
	default:
		return AlertLevelNone
	}
}
