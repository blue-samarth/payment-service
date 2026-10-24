//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	"samarth/payment-service/internal/adapters/redis"
	"samarth/payment-service/internal/domain/gateway"
	"samarth/payment-service/internal/testsupport"
)

func TestCircuitBreaker_TripsAtThreshold(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewCircuitBreakerStore(c)
	ctx := context.Background()
	const gw = "trip-gw"
	const threshold = 3

	for i := 1; i < threshold; i++ {
		opened, fails, err := store.RecordFailure(ctx, gw, threshold)
		if err != nil {
			t.Fatal(err)
		}
		if opened {
			t.Fatalf("breaker opened early at failure %d", i)
		}
		if fails != i {
			t.Errorf("expected failure count %d, got %d", i, fails)
		}
	}

	opened, fails, err := store.RecordFailure(ctx, gw, threshold)
	if err != nil {
		t.Fatal(err)
	}
	if !opened || fails != threshold {
		t.Fatalf("expected breaker to open at threshold, opened=%v fails=%d", opened, fails)
	}

	cb, err := store.Get(ctx, gw)
	if err != nil {
		t.Fatal(err)
	}
	if cb.State != gateway.StateOpen || cb.CooldownUntil.IsZero() {
		t.Errorf("breaker should be OPEN with a cooldown, got state=%s cooldownZero=%v", cb.State, cb.CooldownUntil.IsZero())
	}
}

func TestCircuitBreaker_SuccessResetsFailures(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewCircuitBreakerStore(c)
	ctx := context.Background()
	const gw = "reset-gw"

	_, _, _ = store.RecordFailure(ctx, gw, 5)
	_, _, _ = store.RecordFailure(ctx, gw, 5)

	if err := store.RecordSuccess(ctx, gw); err != nil {
		t.Fatal(err)
	}

	cb, _ := store.Get(ctx, gw)
	if cb.ConsecutiveFailures != 0 || cb.State != gateway.StateClosed {
		t.Errorf("success should reset to CLOSED with 0 failures, got state=%s fails=%d", cb.State, cb.ConsecutiveFailures)
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewCircuitBreakerStore(c)
	ctx := context.Background()
	const gw = "halfopen-gw"

	// Trip it, move to HALF_OPEN, then a single failure must re-open immediately.
	_, _, _ = store.RecordFailure(ctx, gw, 1)
	cb, _ := store.Get(ctx, gw)
	if err := store.Transition(ctx, cb, gateway.StateHalfOpen); err != nil {
		t.Fatal(err)
	}

	opened, _, err := store.RecordFailure(ctx, gw, 5)
	if err != nil {
		t.Fatal(err)
	}
	if !opened {
		t.Error("a failure while HALF_OPEN should immediately re-open the breaker")
	}
}

func TestCircuitBreaker_CooldownEscalatesOnReopen(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewCircuitBreakerStore(c)
	ctx := context.Background()
	const gw = "escalate-gw"
	const threshold = 1 // open on the first failure

	// First open: consecutive_failures = 1 -> CooldownDuration(1) = 60s.
	if _, _, err := store.RecordFailure(ctx, gw, threshold); err != nil {
		t.Fatal(err)
	}
	first, _ := store.Get(ctx, gw)
	firstCooldown := time.Until(first.CooldownUntil)

	// Move OPEN -> HALF_OPEN, then fail again: failures = 2 -> CooldownDuration(2) = 120s.
	if err := store.Transition(ctx, first, gateway.StateHalfOpen); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordFailure(ctx, gw, threshold); err != nil {
		t.Fatal(err)
	}
	second, _ := store.Get(ctx, gw)
	secondCooldown := time.Until(second.CooldownUntil)

	// Previously RecordFailure always used CooldownDuration(threshold), so the
	// cooldown never escalated across re-opens.
	if secondCooldown <= firstCooldown {
		t.Errorf("cooldown should escalate on re-open, got first=%v second=%v", firstCooldown, secondCooldown)
	}
}

type clock struct{ now time.Time }

func (c *clock) Now() time.Time          { return c.now }
func (c *clock) Advance(d time.Duration) { c.now = c.now.Add(d) }
func (c *clock) cooldownLeft(cb *gateway.CircuitBreaker) time.Duration {
	return cb.CooldownUntil.Sub(c.now)
}

func newClockedStore(t *testing.T) (*redis.CircuitBreakerStore, *clock) {
	t.Helper()
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewCircuitBreakerStore(c)
	clk := &clock{now: time.Unix(1_800_000_000, 0).UTC()}
	store.SetClock(clk.Now)
	return store, clk
}

func tripAt(t *testing.T, store *redis.CircuitBreakerStore, gw string, threshold int) {
	t.Helper()
	for i := 0; i < threshold; i++ {
		if _, _, err := store.RecordFailure(context.Background(), gw, threshold); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCircuitBreaker_RecoversAfterCooldown(t *testing.T) {
	store, clk := newClockedStore(t)
	ctx := context.Background()
	const gw = "recover-gw"
	tripAt(t, store, gw, 5)

	tripped, _ := store.Get(ctx, gw)
	clk.now = tripped.CooldownUntil.Add(time.Second)
	if err := store.RecordSuccess(ctx, gw); err != nil {
		t.Fatal(err)
	}
	cb, _ := store.Get(ctx, gw)
	if cb.State != gateway.StateClosed || !cb.CooldownUntil.IsZero() || cb.Trips != 0 || cb.ConsecutiveFailures != 0 {
		t.Fatalf("a success after the cooldown must close and reset the breaker, got %+v", cb)
	}
}

func TestCircuitBreaker_SuccessDuringCooldownDoesNotLatchOpen(t *testing.T) {
	store, _ := newClockedStore(t)
	ctx := context.Background()
	const gw = "latch-gw"
	tripAt(t, store, gw, 5)
	before, _ := store.Get(ctx, gw)

	if err := store.RecordSuccess(ctx, gw); err != nil {
		t.Fatal(err)
	}
	cb, _ := store.Get(ctx, gw)
	if cb.State != gateway.StateOpen || !cb.CooldownUntil.Equal(before.CooldownUntil) {
		t.Fatalf("a late success from an in-flight request must leave OPEN and its cooldown alone, got %+v (cooldown was %s)", cb, before.CooldownUntil)
	}
	if got := gateway.EffectiveState(cb.State, cb.CooldownUntil, before.CooldownUntil); got != gateway.StateHalfOpen {
		t.Fatalf("once the cooldown lapses the gateway must be probeable, got %s; the old script zeroed the cooldown here and latched it OPEN", got)
	}
}

func TestCircuitBreaker_FailureAfterCooldownReTrips(t *testing.T) {
	store, clk := newClockedStore(t)
	ctx := context.Background()
	const gw = "retrip-gw"
	tripAt(t, store, gw, 5)

	tripped, _ := store.Get(ctx, gw)
	clk.now = tripped.CooldownUntil.Add(time.Second)
	opened, _, err := store.RecordFailure(ctx, gw, 5)
	if err != nil {
		t.Fatal(err)
	}
	cb, _ := store.Get(ctx, gw)
	if !opened || cb.State != gateway.StateOpen || !cb.CooldownUntil.After(clk.now) {
		t.Fatalf("a failure after the cooldown is a failed probe and must re-trip at once, got opened=%v %+v", opened, cb)
	}
}

func TestCircuitBreaker_FailedProbesEscalateCooldown(t *testing.T) {
	store, clk := newClockedStore(t)
	ctx := context.Background()
	const gw = "escalate-probe-gw"
	tripAt(t, store, gw, 5)

	cb, _ := store.Get(ctx, gw)
	if left := clk.cooldownLeft(cb); left != gateway.CooldownBase {
		t.Fatalf("the first trip must use the base cooldown even at threshold 5, got %s", left)
	}

	for _, want := range []time.Duration{2 * gateway.CooldownBase, gateway.CooldownMax, gateway.CooldownMax} {
		clk.now = cb.CooldownUntil.Add(time.Second)
		opened, _, err := store.RecordFailure(ctx, gw, 5)
		if err != nil {
			t.Fatal(err)
		}
		if !opened {
			t.Fatal("a failed probe after the cooldown must re-open immediately, without counting to the threshold")
		}
		cb, _ = store.Get(ctx, gw)
		if left := clk.cooldownLeft(cb); left != want {
			t.Fatalf("trip %d: cooldown %s, want %s", cb.Trips, left, want)
		}
	}
}

func TestCircuitBreaker_FailuresDuringCooldownDoNotExtendIt(t *testing.T) {
	store, clk := newClockedStore(t)
	ctx := context.Background()
	const gw = "straggler-gw"
	tripAt(t, store, gw, 5)
	before, _ := store.Get(ctx, gw)

	clk.Advance(10 * time.Second)
	opened, _, err := store.RecordFailure(ctx, gw, 5)
	if err != nil {
		t.Fatal(err)
	}
	cb, _ := store.Get(ctx, gw)
	if opened || !cb.CooldownUntil.Equal(before.CooldownUntil) || cb.Trips != before.Trips {
		t.Fatalf("a late failure during the cooldown must not re-trip, got opened=%v %+v", opened, cb)
	}
}

func TestCircuitBreaker_ProbeIsReleasedByTheProbeOutcome(t *testing.T) {
	store, clk := newClockedStore(t)
	ctx := context.Background()
	const gw = "probe-release-gw"
	tripAt(t, store, gw, 1)

	clk.Advance(gateway.CooldownBase + time.Second)
	if ok, _ := store.AdmitCall(ctx, gw, time.Hour); !ok {
		t.Fatal("expected the first call after the cooldown to be admitted as the probe")
	}
	if ok, _ := store.AdmitCall(ctx, gw, time.Hour); ok {
		t.Fatal("the probe must be single-flight")
	}
	if held, _ := store.ProbeHeld(ctx, gw); !held {
		t.Fatal("routing must be able to see the probe is out")
	}
	if _, _, err := store.RecordFailure(ctx, gw, 1); err != nil {
		t.Fatal(err)
	}

	cb, _ := store.Get(ctx, gw)
	clk.now = cb.CooldownUntil.Add(time.Second)
	if ok, _ := store.AdmitCall(ctx, gw, time.Hour); !ok {
		t.Fatal("a failed probe must release the lock so the next cooldown can be probed without waiting for its TTL")
	}
	if err := store.RecordSuccess(ctx, gw); err != nil {
		t.Fatal(err)
	}
	if held, _ := store.ProbeHeld(ctx, gw); held {
		t.Fatal("a successful probe must release the lock")
	}
}

func TestCircuitBreaker_AdmitCall(t *testing.T) {
	store, clk := newClockedStore(t)
	ctx := context.Background()

	if ok, err := store.AdmitCall(ctx, "never-seen", time.Minute); err != nil || !ok {
		t.Fatalf("a gateway with no breaker record must be admitted, got %v %v", ok, err)
	}

	const gw = "admit-gw"
	tripAt(t, store, gw, 1)
	if ok, _ := store.AdmitCall(ctx, gw, time.Minute); ok {
		t.Fatal("a call routed before the trip must be refused while the cooldown is active")
	}
	if held, _ := store.ProbeHeld(ctx, gw); held {
		t.Fatal("a refused call during the cooldown must not take the probe")
	}

	clk.Advance(gateway.CooldownBase + time.Second)
	if ok, _ := store.AdmitCall(ctx, gw, 0); !ok {
		t.Fatal("expected the probe")
	}
	ttl, err := testsupport.RequireRedis(t).State.PTTL(ctx, "gateway:probe:"+gw).Result()
	if err != nil || ttl <= 0 {
		t.Fatalf("a zero TTL must be floored, never stored as a lock without expiry; got ttl=%s err=%v", ttl, err)
	}
}

func TestCircuitBreaker_SetLastKnownScorePreservesState(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewCircuitBreakerStore(c)
	ctx := context.Background()
	const gw = "score-gw"

	_, _, _ = store.RecordFailure(ctx, gw, 5)
	_, _, _ = store.RecordFailure(ctx, gw, 5) // consecutive_failures = 2, still CLOSED

	if err := store.SetLastKnownScore(ctx, gw, 42); err != nil {
		t.Fatal(err)
	}

	cb, err := store.Get(ctx, gw)
	if err != nil {
		t.Fatal(err)
	}
	if cb.ConsecutiveFailures != 2 {
		t.Errorf("score update must not clobber the failure count, got %d", cb.ConsecutiveFailures)
	}
	if cb.State != gateway.StateClosed {
		t.Errorf("score update must preserve state, got %s", cb.State)
	}
	if cb.LastKnownReliabilityScore != 42 {
		t.Errorf("expected score 42 persisted, got %d", cb.LastKnownReliabilityScore)
	}
}
