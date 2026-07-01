//go:build integration

package redis_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/redis"
	"samarth/payment-service/internal/domain/gateway"
	"samarth/payment-service/internal/testsupport"
)

func rateLimiter(c *redis.Client) *redis.RateLimiter {
	return redis.NewRateLimiter(c, config.RateLimitConfig{
		FallbackMultiplier:  0.5,
		LocalMaxBuckets:     1000,
		HealthCheckInterval: time.Second,
	}, nil, nil)
}

func TestRateLimiter_AdmitsUpToCapacityThenDenies(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()

	const capacity = 5
	for i := 0; i < capacity; i++ {
		if res := rl.Allow(ctx, "u1", "m1", "ip1", capacity, 0.0001); !res.Allowed {
			t.Fatalf("request %d should be allowed within capacity", i+1)
		}
	}
	res := rl.Allow(ctx, "u1", "m1", "ip1", capacity, 0.0001)
	if res.Allowed {
		t.Fatal("request beyond capacity should be denied")
	}
	if res.RetryAfter <= 0 {
		t.Error("denied request should report a positive RetryAfter")
	}
}

func TestRateLimiter_ConcurrentNoOverAdmit(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()

	const capacity = 10
	const goroutines = 50

	var allowed int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rl.Allow(ctx, "uc", "mc", "ipc", capacity, 0.0001).Allowed {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()

	if allowed != capacity {
		t.Errorf("expected exactly %d admitted under contention (atomic Lua), got %d", capacity, allowed)
	}
}

func TestRateLimiter_RefillsOverTime(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()

	if !rl.Allow(ctx, "ur", "mr", "ipr", 1, 100).Allowed {
		t.Fatal("first request should be allowed")
	}
	if rl.Allow(ctx, "ur", "mr", "ipr", 1, 100).Allowed {
		t.Fatal("second immediate request should be denied (bucket empty)")
	}

	time.Sleep(50 * time.Millisecond)

	if !rl.Allow(ctx, "ur", "mr", "ipr", 1, 100).Allowed {
		t.Error("request after refill window should be allowed")
	}
}

func TestCircuitBreaker_ValidTransitionChain(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewCircuitBreakerStore(c)
	ctx := context.Background()
	const gw = "stripe"

	cb, err := store.Get(ctx, gw)
	if err != nil {
		t.Fatal(err)
	}
	if cb.State != gateway.StateClosed {
		t.Fatalf("fresh breaker should be CLOSED, got %s", cb.State)
	}

	if err := store.Transition(ctx, cb, gateway.StateOpen); err != nil {
		t.Fatalf("CLOSED -> OPEN: %v", err)
	}
	if cb.State != gateway.StateOpen || cb.ConsecutiveFailures != 1 || cb.CooldownUntil.IsZero() {
		t.Errorf("after OPEN: state=%s fails=%d cooldownZero=%v", cb.State, cb.ConsecutiveFailures, cb.CooldownUntil.IsZero())
	}

	reloaded, err := store.Get(ctx, gw)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.State != gateway.StateOpen {
		t.Errorf("OPEN state should persist, got %s", reloaded.State)
	}

	if err := store.Transition(ctx, cb, gateway.StateHalfOpen); err != nil {
		t.Fatalf("OPEN -> HALF_OPEN: %v", err)
	}
	if err := store.Transition(ctx, cb, gateway.StateClosed); err != nil {
		t.Fatalf("HALF_OPEN -> CLOSED: %v", err)
	}
	if cb.ConsecutiveFailures != 0 {
		t.Errorf("CLOSED should reset failures, got %d", cb.ConsecutiveFailures)
	}
}

func TestCircuitBreaker_InvalidTransitionRejected(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewCircuitBreakerStore(c)
	ctx := context.Background()

	cb, err := store.Get(ctx, "razorpay")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Transition(ctx, cb, gateway.StateHalfOpen); err == nil {
		t.Fatal("CLOSED -> HALF_OPEN is illegal and must be rejected by the script")
	}
}

func TestCircuitBreaker_AcquireProbeSingleFlight(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewCircuitBreakerStore(c)
	ctx := context.Background()

	first, err := store.AcquireProbe(ctx, "payu", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.AcquireProbe(ctx, "payu", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !first || second {
		t.Errorf("probe should be single-flight: first=%v second=%v (want true,false)", first, second)
	}
}
