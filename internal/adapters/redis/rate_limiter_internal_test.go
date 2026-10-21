package redis

import (
	"container/list"
	"context"
	"testing"

	"samarth/payment-service/config"
)

// RL-16: a single transient Redis error must not flip the whole limiter to
// fallback — only sustained failures crossing the threshold do.
func TestRateLimiter_RequiresConsecutiveFailuresToFlip(t *testing.T) {
	r := &RateLimiter{available: true, failureThreshold: 3}

	r.recordRedisFailure()
	r.recordRedisFailure()
	if !r.IsAvailable() {
		t.Fatal("below the failure threshold the limiter must stay on Redis")
	}

	r.recordRedisFailure()
	if r.IsAvailable() {
		t.Fatal("reaching the failure threshold should flip to local fallback")
	}
}

func TestRateLimiter_SuccessResetsFailureCount(t *testing.T) {
	r := &RateLimiter{available: true, failureThreshold: 2}

	r.recordRedisFailure()
	r.recordRedisSuccess() // a good request in between clears the streak
	r.recordRedisFailure()

	if !r.IsAvailable() {
		t.Fatal("an interleaved success should reset the consecutive-failure count")
	}
}

func TestRateLimiter_MarkAvailableClearsFailureStreak(t *testing.T) {
	r := &RateLimiter{available: false, failureThreshold: 2, consecutiveFailures: 5}
	r.MarkAvailable()
	if r.consecutiveFailures != 0 {
		t.Errorf("recovery should reset the failure counter, got %d", r.consecutiveFailures)
	}
}

// RL-6: a non-positive health-check interval must not panic in NewTicker, and
// Close must stop the goroutine and be safe to call twice.
func TestNewRateLimiter_ZeroIntervalDoesNotPanicAndCloses(t *testing.T) {
	c, err := New(config.RedisConfig{Addrs: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	rl := NewRateLimiter(c, config.RateLimitConfig{HealthCheckInterval: 0}, nil, nil)
	rl.Close()
	rl.Close() // idempotent
}

// RL-15: the local fallback bucket must pick up a reconfigured capacity/rate
// rather than keeping the values captured when it was first created.
func TestRateLimiter_LocalBucketPicksUpNewLimits(t *testing.T) {
	r := &RateLimiter{
		available: false, // force the local path
		lruOrder:  list.New(),
		cfg:       config.RateLimitConfig{FallbackMultiplier: 1, LocalMaxBuckets: 100},
	}
	ctx := context.Background()

	user := Bucket{Namespace: "user", ID: "u"}
	r.Take(ctx, []Bucket{user, {Namespace: "ip", ID: "ip"}}, 10, 5, 1) // creates buckets with capacity 10
	r.Take(ctx, []Bucket{user, {Namespace: "ip", ID: "ip"}}, 2, 1, 1)  // reconfigure to capacity 2

	entry, ok := r.localBuckets.Load(user.localKey())
	if !ok {
		t.Fatal("expected a local bucket for the user dimension")
	}
	if got := entry.(*lruBucketEntry).bucket.capacity; got != 2 {
		t.Errorf("local bucket should adopt the new capacity (2), got %v", got)
	}
}

func localLimiter() *RateLimiter {
	return &RateLimiter{
		available: false,
		lruOrder:  list.New(),
		cfg:       config.RateLimitConfig{FallbackMultiplier: 1, LocalMaxBuckets: 100},
	}
}

func TestRateLimiter_LocalFallbackKeepsNamespacesApart(t *testing.T) {
	r := localLimiter()
	ctx := context.Background()

	if !r.Take(ctx, []Bucket{{Namespace: "user", ID: "1.2.3.4"}}, 1, 0.0001, 1).Allowed {
		t.Fatal("first request must pass")
	}
	if !r.Take(ctx, []Bucket{{Namespace: "ip", ID: "1.2.3.4"}}, 1, 0.0001, 1).Allowed {
		t.Fatal("a user id equal to an IP must not drain that IP's bucket during Redis fallback")
	}
}

func TestRateLimiter_ZeroCostChecksWithoutConsuming(t *testing.T) {
	r := localLimiter()
	ctx := context.Background()
	b := []Bucket{{Namespace: "auth_failure", ID: "1.2.3.4"}}

	for range 5 {
		if !r.Take(ctx, b, 1, 0.0001, 0).Allowed {
			t.Fatal("a zero-cost check must not consume the token it checks for")
		}
	}
	if !r.Take(ctx, b, 1, 0.0001, 1).Allowed {
		t.Fatal("the single token must still be there")
	}
	if res := r.Take(ctx, b, 1, 0.0001, 0); res.Allowed || res.RetryAfter <= 0 {
		t.Fatalf("an empty bucket must fail the check with a retry hint, got %+v", res)
	}
}

func TestRateLimiter_NoBucketsIsAllowed(t *testing.T) {
	if !localLimiter().Take(context.Background(), nil, 1, 1, 1).Allowed {
		t.Fatal("a request with no dimensions to limit is allowed")
	}
}
