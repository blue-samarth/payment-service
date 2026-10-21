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

func dims(user, merchant, ip string) []redis.Bucket {
	return []redis.Bucket{{Namespace: "user", ID: user}, {Namespace: "merchant", ID: merchant}, {Namespace: "ip", ID: ip}}
}

func takeAll(rl *redis.RateLimiter, ctx context.Context, buckets []redis.Bucket, capacity int64, rate float64, cost int64) redis.RateLimitResult {
	limited := make([]redis.Bucket, len(buckets))
	for i, b := range buckets {
		b.Capacity, b.RefillPerSec = capacity, rate
		limited[i] = b
	}
	return rl.Take(ctx, limited, cost)
}

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
		if res := takeAll(rl, ctx, dims("u1", "m1", "ip1"), capacity, 0.0001, 1); !res.Allowed {
			t.Fatalf("request %d should be allowed within capacity", i+1)
		}
	}
	res := takeAll(rl, ctx, dims("u1", "m1", "ip1"), capacity, 0.0001, 1)
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
			if takeAll(rl, ctx, dims("uc", "mc", "ipc"), capacity, 0.0001, 1).Allowed {
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

	if !takeAll(rl, ctx, dims("ur", "mr", "ipr"), 1, 100, 1).Allowed {
		t.Fatal("first request should be allowed")
	}
	if takeAll(rl, ctx, dims("ur", "mr", "ipr"), 1, 100, 1).Allowed {
		t.Fatal("second immediate request should be denied (bucket empty)")
	}

	time.Sleep(50 * time.Millisecond)

	if !takeAll(rl, ctx, dims("ur", "mr", "ipr"), 1, 100, 1).Allowed {
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

func TestRateLimiter_DeniedDimensionDoesNotDrainOthers(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()

	const capacity = 1
	const rate = 0.0001 // effectively no refill within the test window

	// Exhaust the shared IP bucket with u1/m1.
	if !takeAll(rl, ctx, dims("u1", "m1", "shared-ip"), capacity, rate, 1).Allowed {
		t.Fatal("first request should be allowed")
	}
	// u2/m2 share the now-exhausted IP: the request must be denied.
	if takeAll(rl, ctx, dims("u2", "m2", "shared-ip"), capacity, rate, 1).Allowed {
		t.Fatal("second request on the exhausted IP should be denied")
	}
	// That denial must NOT have consumed u2's or m2's tokens: a fresh request
	// for u2/m2 on a different IP is still allowed. Under the old per-dimension
	// pipeline this failed, because the denied request debited every dimension.
	if !takeAll(rl, ctx, dims("u2", "m2", "fresh-ip"), capacity, rate, 1).Allowed {
		t.Error("a denial on one dimension must not drain the other dimensions' buckets")
	}
}

func TestRateLimiter_NonPositiveRateDoesNotCrash(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()

	// rate = 0 must not divide-by-zero in the script; a full bucket still admits.
	if !takeAll(rl, ctx, dims("u", "m", "ip"), 2, 0, 1).Allowed {
		t.Fatal("first request against a full bucket should be allowed even at rate 0")
	}
}

func TestRateLimiter_ZeroCostChecksWithoutConsuming(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()
	b := []redis.Bucket{{Namespace: "auth_failure", ID: "203.0.113.5"}}

	for range 5 {
		if !takeAll(rl, ctx, b, 1, 0.0001, 0).Allowed {
			t.Fatal("the Lua script must treat cost 0 as a check that consumes nothing")
		}
	}
	if !takeAll(rl, ctx, b, 1, 0.0001, 1).Allowed {
		t.Fatal("the single token must still be there after repeated checks")
	}
	if res := takeAll(rl, ctx, b, 1, 0.0001, 0); res.Allowed || res.RetryAfter <= 0 {
		t.Fatalf("an empty bucket must fail the check with a retry hint, got %+v", res)
	}
}

func TestRateLimiter_OnlyTheGivenDimensionsAreLimited(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()
	ipOnly := []redis.Bucket{{Namespace: "ip", ID: "198.51.100.1"}}

	if !takeAll(rl, ctx, ipOnly, 1, 0.0001, 1).Allowed {
		t.Fatal("first request should be allowed")
	}
	if !takeAll(rl, ctx, []redis.Bucket{{Namespace: "ip", ID: "198.51.100.2"}}, 1, 0.0001, 1).Allowed {
		t.Fatal("a request with no user or merchant dimension must not share an empty-string bucket with others")
	}
}

func TestRateLimiter_DeniedRequestsWriteNoKeys(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()
	merchant := redis.Bucket{Namespace: "merchant", ID: "m", Capacity: 1, RefillPerSec: 0.0001}

	if !rl.Take(ctx, []redis.Bucket{merchant}, 1).Allowed {
		t.Fatal("first request should be allowed")
	}
	before, err := c.RateLimit.DBSize(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		user := redis.Bucket{Namespace: "user", ID: "m\x00random-" + string(rune('a'+i)), Capacity: 10, RefillPerSec: 1}
		if rl.Take(ctx, []redis.Bucket{merchant, user}, 1).Allowed {
			t.Fatal("the merchant bucket is empty, so every request is denied")
		}
	}
	after, err := c.RateLimit.DBSize(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("denied requests with random user ids must not create keys, keys went %d -> %d", before, after)
	}

	auth := redis.Bucket{Namespace: "auth_failure", ID: "cert:new", Capacity: 5, RefillPerSec: 1}
	if !rl.Take(ctx, []redis.Bucket{auth}, 0).Allowed {
		t.Fatal("a zero-cost check on a fresh bucket passes")
	}
	if n, _ := c.RateLimit.DBSize(ctx).Result(); n != after {
		t.Fatalf("a zero-cost check must not create a key, keys went %d -> %d", after, n)
	}
}

func TestRateLimiter_PerBucketLimits(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()
	merchant := redis.Bucket{Namespace: "merchant", ID: "m", Capacity: 100, RefillPerSec: 0.0001}
	user := redis.Bucket{Namespace: "user", ID: "m\x00u", Capacity: 2, RefillPerSec: 0.0001}

	for i := range 2 {
		if !rl.Take(ctx, []redis.Bucket{merchant, user}, 1).Allowed {
			t.Fatalf("request %d is within the user limit", i+1)
		}
	}
	if rl.Take(ctx, []redis.Bucket{merchant, user}, 1).Allowed {
		t.Fatal("the script must apply each bucket's own capacity: the user limit binds first")
	}
}

func TestRateLimiter_EmptyBucketListFailsClosed(t *testing.T) {
	c := testsupport.RequireRedis(t)
	rl := rateLimiter(c)
	if res := rl.Take(context.Background(), nil, 1); res.Allowed {
		t.Fatal("nothing to key on must be refused")
	}
}
