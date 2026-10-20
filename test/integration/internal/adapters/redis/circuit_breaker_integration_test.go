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
