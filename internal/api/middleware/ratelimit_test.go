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

var testCfg = RateLimitConfig{Capacity: 10, RefillPerSec: 5}

func TestRateLimit_Allows(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := RateLimit(lim, testCfg, noopLog{}, RequestKeys(nil))(okHandler())

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
	h := RateLimit(lim, testCfg, noopLog{}, RequestKeys(nil))(okHandler())

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
		RateLimit(lim, testCfg, noopLog{}, RequestKeys(nil))(okHandler()),
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

func TestRateLimit_IgnoresForwardedForFromUntrustedPeer(t *testing.T) {
	for name, keys := range map[string]RateLimitKeys{
		"merchant": RequestKeys(NewClientIP(nil)),
		"webhook":  WebhookKeys(NewClientIP(nil)),
	} {
		t.Run(name, func(t *testing.T) {
			lim := &fakeLimiter{allow: true}
			h := RateLimit(lim, testCfg, noopLog{}, keys)(okHandler())

			for _, spoofed := range []string{"198.51.100.7", "198.51.100.8", "198.51.100.9"} {
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Forwarded-For", spoofed)
				req.RemoteAddr = "203.0.113.5:5555"
				h.ServeHTTP(httptest.NewRecorder(), req)

				if lim.gotIP != "203.0.113.5" {
					t.Fatalf("a rotated X-Forwarded-For must not change the IP bucket, got %q", lim.gotIP)
				}
			}
		})
	}
}

func TestRateLimit_IPv6SharesOneBucketPer64(t *testing.T) {
	for name, keys := range map[string]RateLimitKeys{
		"merchant": RequestKeys(nil),
		"webhook":  WebhookKeys(nil),
	} {
		t.Run(name, func(t *testing.T) {
			lim := &fakeLimiter{allow: true}
			h := RateLimit(lim, testCfg, noopLog{}, keys)(okHandler())

			for _, remote := range []string{"[2001:db8:1:2::1]:1", "[2001:db8:1:2:ffff:ffff:ffff:ffff]:1"} {
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.RemoteAddr = remote
				h.ServeHTTP(httptest.NewRecorder(), req)
				if lim.gotIP != "2001:db8:1:2::/64" {
					t.Fatalf("addresses in one /64 must share a bucket, got %q", lim.gotIP)
				}
			}

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.RemoteAddr = "[2001:db8:1:3::1]:1"
			h.ServeHTTP(httptest.NewRecorder(), req)
			if lim.gotIP != "2001:db8:1:3::/64" {
				t.Fatalf("a different /64 gets its own bucket, got %q", lim.gotIP)
			}

			req = httptest.NewRequest(http.MethodPost, "/", nil)
			req.RemoteAddr = "203.0.113.5:1"
			h.ServeHTTP(httptest.NewRecorder(), req)
			if lim.gotIP != "203.0.113.5" {
				t.Fatalf("IPv4 stays keyed per address, got %q", lim.gotIP)
			}
		})
	}
}

func TestRateLimit_WebhookKeysArePerIP(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	keys := WebhookKeys(nil)
	h := RateLimit(lim, testCfg, noopLog{}, keys)(okHandler())

	req := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/stripe", nil)
	req.RemoteAddr = "203.0.113.5:4444"
	req.Header.Set("X-User-ID", "someone")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if lim.gotIP != "203.0.113.5" || lim.gotMerchant != "webhook:203.0.113.5" {
		t.Errorf("webhook buckets must be keyed on the caller IP, got ip=%q merchant=%q", lim.gotIP, lim.gotMerchant)
	}
	if user, _, _ := keys(req); user != "webhook:203.0.113.5" {
		t.Errorf("the client-supplied X-User-ID must not key webhook buckets, got %q", user)
	}
}
