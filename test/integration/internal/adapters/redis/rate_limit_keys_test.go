//go:build integration

package redis_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"samarth/payment-service/internal/adapters/redis"
	"samarth/payment-service/internal/api/middleware"
	"samarth/payment-service/internal/ports"
	"samarth/payment-service/internal/testsupport"
)

type limiter struct{ rl *redis.RateLimiter }

func (l limiter) Take(ctx context.Context, buckets []middleware.Bucket, capacity int64, rate float64, cost int64) middleware.RateDecision {
	keys := make([]redis.Bucket, len(buckets))
	for i, b := range buckets {
		keys[i] = redis.Bucket{Namespace: b.Namespace, ID: b.ID}
	}
	res := l.rl.Take(ctx, keys, capacity, rate, cost)
	return middleware.RateDecision{Allowed: res.Allowed, RetryAfter: res.RetryAfter}
}

type quietLog struct{}

func (quietLog) Info(string, map[string]any)         {}
func (quietLog) Warn(string, map[string]any)         {}
func (quietLog) Error(string, map[string]any, error) {}
func (quietLog) Debug(string, map[string]any)        {}
func (quietLog) Trace(string, map[string]any)        {}
func (l quietLog) With(map[string]any) ports.Logger  { return l }

var (
	tokenA = "svc-a-" + strings.Repeat("a", 32)
	tokenB = "svc-b-" + strings.Repeat("b", 32)
)

const oneShot = 0.0001

func merchantStack(lim middleware.Limiter, capacity int64, throttle *middleware.AuthThrottle) http.Handler {
	provider := middleware.NewStaticTokenProvider(map[string]string{tokenA: "merchant-a", tokenB: "merchant-b"}, nil)
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	limited := middleware.RateLimit(lim, middleware.RateLimitConfig{Capacity: capacity, RefillPerSec: oneShot}, quietLog{}, middleware.RequestKeys(nil))(ok)
	return middleware.Authenticate(provider, quietLog{}, throttle)(limited)
}

func send(h http.Handler, token, userID, remote string) int {
	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	if token != "" {
		req.Header.Set("X-Service-Token", token)
	}
	if userID != "" {
		req.Header.Set("X-User-ID", userID)
	}
	req.RemoteAddr = remote
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestRateLimitKeys_MerchantsWithoutUserIDDoNotThrottleEachOther(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	h := merchantStack(limiter{rateLimiter(c)}, 1, nil)

	if code := send(h, tokenA, "", "203.0.113.1:1"); code != http.StatusOK {
		t.Fatalf("merchant A first request: %d", code)
	}
	if code := send(h, tokenB, "", "203.0.113.2:1"); code != http.StatusOK {
		t.Fatalf("merchant B must not be throttled by A's traffic through a shared empty user bucket, got %d", code)
	}
}

func TestRateLimitKeys_MerchantCannotDrainAWebhookBucket(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	lim := limiter{rateLimiter(c)}
	merchant := merchantStack(lim, 100, nil)

	for range 5 {
		send(merchant, tokenA, "webhook:198.51.100.7", "203.0.113.1:1")
	}

	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	webhook := middleware.RateLimit(lim, middleware.RateLimitConfig{Capacity: 1, RefillPerSec: oneShot}, quietLog{}, middleware.WebhookKeys(nil))(ok)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/stripe", nil)
	req.RemoteAddr = "198.51.100.7:1"
	rec := httptest.NewRecorder()
	webhook.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("the gateway's webhook bucket must be untouched by merchant X-User-ID values, got %d", rec.Code)
	}
}

func TestRateLimitKeys_AuthFailuresAreThrottledPerIP(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	lim := limiter{rateLimiter(c)}
	throttle := &middleware.AuthThrottle{Limiter: lim, Capacity: 2, RefillPerSec: oneShot}
	h := merchantStack(lim, 100, throttle)

	for i := range 2 {
		if code := send(h, "wrong-token", "", "203.0.113.9:1"); code != http.StatusUnauthorized {
			t.Fatalf("failure %d within the limit is a 401, got %d", i+1, code)
		}
	}
	if code := send(h, "wrong-token", "", "203.0.113.9:1"); code != http.StatusTooManyRequests {
		t.Fatalf("once the IP is out of auth-failure tokens it gets 429, got %d", code)
	}
	if code := send(h, tokenA, "", "203.0.113.9:1"); code != http.StatusTooManyRequests {
		t.Fatalf("the block applies before the token is checked, got %d", code)
	}
	if code := send(h, tokenA, "", "203.0.113.10:1"); code != http.StatusOK {
		t.Fatalf("other addresses are unaffected, got %d", code)
	}
}
