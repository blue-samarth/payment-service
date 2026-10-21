package redis

import (
	"container/list"
	"context"
	"strings"
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

func localLimiter() *RateLimiter {
	return &RateLimiter{
		available: false,
		lruOrder:  list.New(),
		cfg:       config.RateLimitConfig{FallbackMultiplier: 1, LocalMaxBuckets: 100},
	}
}

func bucket(namespace, id string, capacity int64, rate float64) Bucket {
	return Bucket{Namespace: namespace, ID: id, Capacity: capacity, RefillPerSec: rate}
}

// RL-15: the local fallback bucket must pick up a reconfigured capacity/rate
// rather than keeping the values captured when it was first created.
func TestRateLimiter_LocalBucketPicksUpNewLimits(t *testing.T) {
	r := localLimiter()
	ctx := context.Background()

	r.Take(ctx, []Bucket{bucket("user", "u", 10, 5)}, 1)
	r.Take(ctx, []Bucket{bucket("user", "u", 2, 1)}, 1)

	entry, ok := r.localBuckets.Load(bucket("user", "u", 0, 0).localKey())
	if !ok {
		t.Fatal("expected a local bucket for the user dimension")
	}
	if got := entry.(*lruBucketEntry).bucket.capacity; got != 2 {
		t.Errorf("local bucket should adopt the new capacity (2), got %v", got)
	}
}

func TestRateLimiter_LocalFallbackKeepsNamespacesApart(t *testing.T) {
	r := localLimiter()
	ctx := context.Background()

	if !r.Take(ctx, []Bucket{bucket("user", "1.2.3.4", 1, 0.0001)}, 1).Allowed {
		t.Fatal("first request must pass")
	}
	if !r.Take(ctx, []Bucket{bucket("ip", "1.2.3.4", 1, 0.0001)}, 1).Allowed {
		t.Fatal("a user id equal to an IP must not drain that IP's bucket during Redis fallback")
	}
}

func TestRateLimiter_LocalKeysAreFixedSize(t *testing.T) {
	huge := bucket("user", strings.Repeat("x", 1<<20), 1, 1)
	if got, want := len(huge.localKey()), len(bucket("user", "u", 1, 1).localKey()); got != want {
		t.Fatalf("a 1 MiB id must not become a 1 MiB map key during Redis fallback, got %d bytes, want %d", got, want)
	}
}

func TestRateLimiter_EachBucketUsesItsOwnLimit(t *testing.T) {
	r := localLimiter()
	ctx := context.Background()
	merchant := bucket("merchant", "m", 100, 0.0001)
	user := bucket("user", "m\x00u", 2, 0.0001)

	for i := range 2 {
		if !r.Take(ctx, []Bucket{merchant, user}, 1).Allowed {
			t.Fatalf("request %d is within the user limit", i+1)
		}
	}
	if r.Take(ctx, []Bucket{merchant, user}, 1).Allowed {
		t.Fatal("the smaller user limit must bind before the merchant limit")
	}
	if !r.Take(ctx, []Bucket{merchant, bucket("user", "m\x00other", 2, 0.0001)}, 1).Allowed {
		t.Fatal("another user of the same merchant still has its own allowance")
	}
}

func TestRateLimiter_ZeroCostChecksWithoutConsuming(t *testing.T) {
	r := localLimiter()
	ctx := context.Background()
	b := []Bucket{bucket("auth_failure", "cert:abc", 1, 0.0001)}

	for range 5 {
		if !r.Take(ctx, b, 0).Allowed {
			t.Fatal("a zero-cost check must not consume the token it checks for")
		}
	}
	if !r.Take(ctx, b, 1).Allowed {
		t.Fatal("the single token must still be there")
	}
	if res := r.Take(ctx, b, 0); res.Allowed || res.RetryAfter <= 0 {
		t.Fatalf("an empty bucket must fail the check with a retry hint, got %+v", res)
	}
}

func TestRateLimiter_NoBucketsFailsClosed(t *testing.T) {
	if res := localLimiter().Take(context.Background(), nil, 1); res.Allowed || res.RetryAfter <= 0 {
		t.Fatalf("a request with nothing to key on must be refused, not waved through, got %+v", res)
	}
}
