package gateway

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker_ValidTransitions(t *testing.T) {
	cb := &CircuitBreaker{State: StateClosed}

	if err := cb.Transition(StateOpen); err != nil {
		t.Fatalf("CLOSED → OPEN: %v", err)
	}
	if cb.ConsecutiveFailures != 1 {
		t.Errorf("expected failures incremented to 1, got %d", cb.ConsecutiveFailures)
	}
	if cb.CooldownUntil.IsZero() {
		t.Error("expected cooldown set on OPEN")
	}

	if err := cb.Transition(StateHalfOpen); err != nil {
		t.Fatalf("OPEN → HALF_OPEN: %v", err)
	}
	if !cb.CooldownUntil.IsZero() {
		t.Error("expected cooldown cleared on HALF_OPEN")
	}

	if err := cb.Transition(StateClosed); err != nil {
		t.Fatalf("HALF_OPEN → CLOSED: %v", err)
	}
	if cb.ConsecutiveFailures != 0 {
		t.Errorf("expected failures reset on CLOSED, got %d", cb.ConsecutiveFailures)
	}
}

func TestCircuitBreaker_HalfOpenToOpenIncrementsFailures(t *testing.T) {
	cb := &CircuitBreaker{State: StateHalfOpen, ConsecutiveFailures: 1}
	if err := cb.Transition(StateOpen); err != nil {
		t.Fatal(err)
	}
	if cb.ConsecutiveFailures != 2 {
		t.Errorf("expected failures 2, got %d", cb.ConsecutiveFailures)
	}
}

func TestCircuitBreaker_InvalidTransitions(t *testing.T) {
	cases := []struct {
		from CircuitState
		to   CircuitState
	}{
		{StateClosed, StateHalfOpen},
		{StateClosed, StateClosed},
		{StateOpen, StateClosed},
		{StateOpen, StateOpen},
	}
	for _, c := range cases {
		cb := &CircuitBreaker{State: c.from}
		err := cb.Transition(c.to)
		var invalid ErrInvalidTransition
		if !errors.As(err, &invalid) {
			t.Errorf("%s → %s: expected ErrInvalidTransition, got %v", c.from, c.to, err)
		}
	}
}

func TestCooldownDuration(t *testing.T) {
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{0, 60 * time.Second},
		{1, 60 * time.Second},
		{2, 120 * time.Second},
		{3, 240 * time.Second},
		{4, 240 * time.Second},
		{10, 240 * time.Second},
	}
	for _, c := range cases {
		if got := CooldownDuration(c.failures); got != c.want {
			t.Errorf("CooldownDuration(%d) = %v, want %v", c.failures, got, c.want)
		}
	}
}

func TestCircuitBreaker_IsRoutable(t *testing.T) {
	want := map[CircuitState]bool{StateClosed: true, StateHalfOpen: true, StateOpen: false}
	for s, w := range want {
		if (&CircuitBreaker{State: s}).IsRoutable() != w {
			t.Errorf("%s IsRoutable() = %v, want %v", s, !w, w)
		}
	}
}

func TestCircuitBreaker_ShouldTransitionToHalfOpen(t *testing.T) {
	past := &CircuitBreaker{State: StateOpen, CooldownUntil: time.Now().UTC().Add(-time.Minute)}
	if !past.ShouldTransitionToHalfOpen() {
		t.Error("OPEN past cooldown should transition to HALF_OPEN")
	}
	future := &CircuitBreaker{State: StateOpen, CooldownUntil: time.Now().UTC().Add(time.Minute)}
	if future.ShouldTransitionToHalfOpen() {
		t.Error("OPEN within cooldown should not transition")
	}
	closed := &CircuitBreaker{State: StateClosed, CooldownUntil: time.Now().UTC().Add(-time.Minute)}
	if closed.ShouldTransitionToHalfOpen() {
		t.Error("non-OPEN should never transition to HALF_OPEN")
	}
}

func TestDiscrepancyMetrics_EffectiveRate(t *testing.T) {
	t.Run("recent uses raw 24h rate", func(t *testing.T) {
		d := &DiscrepancyMetrics{Rate24H: 0.1, DaysSinceDiscrepancy: 0}
		if d.EffectiveRate() != 0.1 {
			t.Errorf("expected raw rate 0.1, got %v", d.EffectiveRate())
		}
	})
	t.Run("decays after a week", func(t *testing.T) {
		d := &DiscrepancyMetrics{Rate24H: 0.1, DaysSinceDiscrepancy: 8}
		got := d.EffectiveRate()
		if got >= 0.1 || got <= 0 {
			t.Errorf("expected decayed rate in (0, 0.1), got %v", got)
		}
	})
}

func TestDiscrepancyMetrics_ReliabilityScore(t *testing.T) {
	cases := []struct {
		rate float64
		want int
	}{
		{0.0, 100},
		{0.10, 90},
		{0.25, 0},
	}
	for _, c := range cases {
		d := &DiscrepancyMetrics{Rate24H: c.rate}
		if got := d.ReliabilityScore(); got != c.want {
			t.Errorf("ReliabilityScore(rate=%v) = %d, want %d", c.rate, got, c.want)
		}
	}
}

func TestDiscrepancyMetrics_CurrentAlertLevel(t *testing.T) {
	cases := []struct {
		name              string
		r5min, r24h       float64
		want              AlertLevel
	}{
		{"auto disable", 0.25, 0, AlertLevelAutoDisable},
		{"alert", 0.10, 0, AlertLevelAlert},
		{"investigate", 0.0, 0.05, AlertLevelInvestigate},
		{"none", 0.0, 0.0, AlertLevelNone},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := &DiscrepancyMetrics{Rate5Min: c.r5min, Rate24H: c.r24h}
			if got := d.CurrentAlertLevel(); got != c.want {
				t.Errorf("CurrentAlertLevel() = %s, want %s", got, c.want)
			}
		})
	}
}
