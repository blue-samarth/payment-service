package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"samarth/payment-service/internal/ports"
)

type fakeLimiter struct {
	allow       bool
	retryAfter  time.Duration
	gotIP       string
	gotMerchant string
}

func (f *fakeLimiter) Allow(_ context.Context, userID, merchantID, ip string, _ int64, _ float64) RateDecision {
	f.gotIP = ip
	f.gotMerchant = merchantID
	return RateDecision{Allowed: f.allow, RetryAfter: f.retryAfter}
}

type noopLog struct{}

func (noopLog) Info(string, map[string]any)         {}
func (noopLog) Warn(string, map[string]any)         {}
func (noopLog) Error(string, map[string]any, error) {}
func (noopLog) Debug(string, map[string]any)        {}
func (noopLog) Trace(string, map[string]any)        {}
func (l noopLog) With(map[string]any) ports.Logger  { return l }

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	})
}

func TestRateLimit_Allows(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := RateLimit(lim, RateLimitConfig{Capacity: 10, RefillPerSec: 5}, noopLog{})(okHandler())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.RemoteAddr = "203.0.113.5:4444"
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if lim.gotIP != "203.0.113.5" {
		t.Errorf("expected IP parsed to 203.0.113.5, got %q", lim.gotIP)
	}
}

func TestRateLimit_Rejects429(t *testing.T) {
	lim := &fakeLimiter{allow: false, retryAfter: 2 * time.Second}
	h := RateLimit(lim, RateLimitConfig{Capacity: 10, RefillPerSec: 5}, noopLog{})(okHandler())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/payments", nil))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "2" {
		t.Errorf("expected Retry-After: 2, got %q", rec.Header().Get("Retry-After"))
	}
}

func TestRateLimit_MerchantBucketFromTokenNotHeader(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	provider := NewStaticTokenProvider(map[string]string{"svc-secret": "attacker"}, nil)
	// Authenticate resolves the token to merchant "attacker"; RateLimit must key
	// on that, ignoring the spoofed X-Merchant-ID header naming the victim.
	h := Authenticate(provider, noopLog{})(
		RateLimit(lim, RateLimitConfig{Capacity: 10, RefillPerSec: 5}, noopLog{})(okHandler()),
	)

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Service-Token", "svc-secret")
	req.Header.Set("X-Merchant-ID", "victim")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if lim.gotMerchant == "victim" {
		t.Fatal("a spoofed X-Merchant-ID must not reach the bucket key")
	}
	if lim.gotMerchant != "attacker" {
		t.Errorf("bucket must key on the token-bound merchant, got %q", lim.gotMerchant)
	}
}

func TestRateLimit_PrefersForwardedFor(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := RateLimit(lim, RateLimitConfig{Capacity: 10, RefillPerSec: 5}, noopLog{})(okHandler())

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Forwarded-For", "198.51.100.7, 10.0.0.1")
	req.RemoteAddr = "10.0.0.1:5555"
	h.ServeHTTP(httptest.NewRecorder(), req)

	if lim.gotIP != "198.51.100.7" {
		t.Errorf("expected client IP from X-Forwarded-For, got %q", lim.gotIP)
	}
}
