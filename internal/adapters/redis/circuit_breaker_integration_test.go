//go:build integration

package redis_test

import (
	"context"
	"testing"

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
