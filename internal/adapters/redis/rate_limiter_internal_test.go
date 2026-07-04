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

	r.Allow(ctx, "u", "m", "ip", 10, 5) // creates buckets with capacity 10
	r.Allow(ctx, "u", "m", "ip", 2, 1)  // reconfigure to capacity 2

	entry, ok := r.localBuckets.Load("u")
	if !ok {
		t.Fatal("expected a local bucket for the user dimension")
	}
	if got := entry.(*lruBucketEntry).bucket.capacity; got != 2 {
		t.Errorf("local bucket should adopt the new capacity (2), got %v", got)
	}
}
