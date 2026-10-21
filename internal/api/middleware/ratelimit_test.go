package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"samarth/payment-service/internal/ports"
)

type take struct {
	buckets []Bucket
	cost    int64
}

type fakeLimiter struct {
	allow      bool
	retryAfter time.Duration
	calls      []take
}

func (f *fakeLimiter) Take(_ context.Context, buckets []Bucket, _ int64, _ float64, cost int64) RateDecision {
	f.calls = append(f.calls, take{buckets: buckets, cost: cost})
	return RateDecision{Allowed: f.allow, RetryAfter: f.retryAfter}
}

func (f *fakeLimiter) last() []Bucket {
	return f.calls[len(f.calls)-1].buckets
}

func (f *fakeLimiter) bucket(namespace string) (string, bool) {
	for _, b := range f.last() {
		if b.Namespace == namespace {
			return b.ID, true
		}
	}
	return "", false
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

var (
	tokenA = "svc-a-" + strings.Repeat("a", 32)
	tokenB = "svc-b-" + strings.Repeat("b", 32)
)

func merchantChain(lim *fakeLimiter, keys RateLimitKeys) http.Handler {
	provider := NewStaticTokenProvider(map[string]string{tokenA: "merchant-a", tokenB: "merchant-b"}, nil)
	return Authenticate(provider, noopLog{}, nil)(RateLimit(lim, testCfg, noopLog{}, keys)(okHandler()))
}

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
	if ip, _ := lim.bucket(NamespaceIP); ip != "203.0.113.5" {
		t.Errorf("expected IP parsed to 203.0.113.5, got %q", ip)
	}
	if lim.calls[0].cost != 1 {
		t.Errorf("a request costs one token, got %d", lim.calls[0].cost)
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
	h := merchantChain(lim, RequestKeys(nil))

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Service-Token", tokenA)
	req.Header.Set("X-Merchant-ID", "victim")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if got, _ := lim.bucket(NamespaceMerchant); got != "merchant-a" {
		t.Errorf("bucket must key on the token-bound merchant, not X-Merchant-ID, got %q", got)
	}
}

func TestRateLimit_MissingUserIDSkipsTheUserDimension(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := merchantChain(lim, RequestKeys(nil))

	users := map[string]bool{}
	for _, token := range []string{tokenA, tokenB} {
		req := httptest.NewRequest(http.MethodPost, "/payments", nil)
		req.Header.Set("X-Service-Token", token)
		req.RemoteAddr = "203.0.113.5:1"
		h.ServeHTTP(httptest.NewRecorder(), req)
		if id, ok := lim.bucket(NamespaceUser); ok {
			users[id] = true
		}
	}
	if len(users) != 0 {
		t.Fatalf("two merchants sending no X-User-ID must not share an empty-string user bucket, got %v", users)
	}
}

func TestRateLimit_UserIDIsScopedToTheMerchant(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := merchantChain(lim, RequestKeys(nil))

	ids := map[string]string{}
	for name, token := range map[string]string{"a": tokenA, "b": tokenB} {
		req := httptest.NewRequest(http.MethodPost, "/payments", nil)
		req.Header.Set("X-Service-Token", token)
		req.Header.Set("X-User-ID", "customer-42")
		h.ServeHTTP(httptest.NewRecorder(), req)
		ids[name], _ = lim.bucket(NamespaceUser)
	}
	if ids["a"] == "" || ids["a"] == ids["b"] {
		t.Fatalf("the same X-User-ID under two merchants must be two buckets, got %v", ids)
	}
}

func TestRateLimit_MerchantCannotReachAWebhookBucket(t *testing.T) {
	merchant := &fakeLimiter{allow: true}
	merchantReq := httptest.NewRequest(http.MethodPost, "/payments", nil)
	merchantReq.Header.Set("X-Service-Token", tokenA)
	merchantReq.Header.Set("X-User-ID", "webhook:198.51.100.7")
	merchantReq.RemoteAddr = "203.0.113.5:1"
	merchantChain(merchant, RequestKeys(nil)).ServeHTTP(httptest.NewRecorder(), merchantReq)

	webhook := &fakeLimiter{allow: true}
	webhookReq := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/stripe", nil)
	webhookReq.RemoteAddr = "198.51.100.7:1"
	RateLimit(webhook, testCfg, noopLog{}, WebhookKeys(nil))(okHandler()).ServeHTTP(httptest.NewRecorder(), webhookReq)

	for _, mb := range merchant.last() {
		for _, wb := range webhook.last() {
			if mb == wb {
				t.Fatalf("a merchant header reached the webhook bucket %+v", wb)
			}
		}
	}
}

func TestRateLimit_NoPrincipalUsesIPAndScopedUserOnly(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := RateLimit(lim, testCfg, noopLog{}, RequestKeys(nil))(okHandler())

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-User-ID", "u1")
	req.RemoteAddr = "203.0.113.5:1"
	h.ServeHTTP(httptest.NewRecorder(), req)

	if _, ok := lim.bucket(NamespaceMerchant); ok {
		t.Error("without a principal there is no merchant to key on; the dimension must be skipped, not keyed on \"\"")
	}
	if user, _ := lim.bucket(NamespaceUser); !strings.HasPrefix(user, unauthenticatedScope) {
		t.Errorf("an unauthenticated user key must be scoped apart from merchant users, got %q", user)
	}
}

func TestRateLimit_IgnoresForwardedForFromUntrustedPeer(t *testing.T) {
	for name, tc := range map[string]struct {
		keys      RateLimitKeys
		namespace string
	}{
		"merchant": {RequestKeys(NewClientIP(nil)), NamespaceIP},
		"webhook":  {WebhookKeys(NewClientIP(nil)), NamespaceWebhookIP},
	} {
		t.Run(name, func(t *testing.T) {
			lim := &fakeLimiter{allow: true}
			h := RateLimit(lim, testCfg, noopLog{}, tc.keys)(okHandler())

			for _, spoofed := range []string{"198.51.100.7", "198.51.100.8", "198.51.100.9"} {
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Forwarded-For", spoofed)
				req.RemoteAddr = "203.0.113.5:5555"
				h.ServeHTTP(httptest.NewRecorder(), req)

				if ip, _ := lim.bucket(tc.namespace); ip != "203.0.113.5" {
					t.Fatalf("a rotated X-Forwarded-For must not change the IP bucket, got %q", ip)
				}
			}
		})
	}
}

func TestRateLimit_IPv6SharesOneBucketPer64(t *testing.T) {
	for name, tc := range map[string]struct {
		keys      RateLimitKeys
		namespace string
	}{
		"merchant": {RequestKeys(nil), NamespaceIP},
		"webhook":  {WebhookKeys(nil), NamespaceWebhookIP},
	} {
		t.Run(name, func(t *testing.T) {
			lim := &fakeLimiter{allow: true}
			h := RateLimit(lim, testCfg, noopLog{}, tc.keys)(okHandler())

			serve := func(remote string) string {
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.RemoteAddr = remote
				h.ServeHTTP(httptest.NewRecorder(), req)
				ip, _ := lim.bucket(tc.namespace)
				return ip
			}
			for _, remote := range []string{"[2001:db8:1:2::1]:1", "[2001:db8:1:2:ffff:ffff:ffff:ffff]:1"} {
				if got := serve(remote); got != "2001:db8:1:2::/64" {
					t.Fatalf("addresses in one /64 must share a bucket, got %q", got)
				}
			}
			if got := serve("[2001:db8:1:3::1]:1"); got != "2001:db8:1:3::/64" {
				t.Fatalf("a different /64 gets its own bucket, got %q", got)
			}
			if got := serve("203.0.113.5:1"); got != "203.0.113.5" {
				t.Fatalf("IPv4 stays keyed per address, got %q", got)
			}
		})
	}
}

func TestRateLimit_WebhookKeysAreOneIPBucket(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := RateLimit(lim, testCfg, noopLog{}, WebhookKeys(nil))(okHandler())

	req := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/stripe", nil)
	req.RemoteAddr = "203.0.113.5:4444"
	req.Header.Set("X-User-ID", "someone")
	h.ServeHTTP(httptest.NewRecorder(), req)

	got := lim.last()
	if len(got) != 1 || got[0] != (Bucket{Namespace: NamespaceWebhookIP, ID: "203.0.113.5"}) {
		t.Errorf("webhooks are limited per caller IP only, in their own namespace, got %+v", got)
	}
}

func authThrottled(lim *fakeLimiter) http.Handler {
	provider := NewStaticTokenProvider(map[string]string{tokenA: "merchant-a"}, nil)
	throttle := &AuthThrottle{Limiter: lim, Capacity: 10, RefillPerSec: 0.1}
	return Authenticate(provider, noopLog{}, throttle)(okHandler())
}

func TestAuthThrottle_FailureConsumesAndSuccessDoesNot(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := authThrottled(lim)

	bad := httptest.NewRequest(http.MethodPost, "/payments", nil)
	bad.Header.Set("X-Service-Token", "wrong")
	bad.RemoteAddr = "203.0.113.5:1"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, bad)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("a bad token is still a 401 while under the limit, got %d", rec.Code)
	}
	if len(lim.calls) != 2 || lim.calls[0].cost != 0 || lim.calls[1].cost != 1 {
		t.Fatalf("expected a zero-cost check then one consumed token, got %+v", lim.calls)
	}
	if b := lim.calls[1].buckets; len(b) != 1 || b[0] != (Bucket{Namespace: NamespaceAuthFailure, ID: "203.0.113.5"}) {
		t.Errorf("failures are counted per client IP in their own namespace, got %+v", b)
	}

	lim.calls = nil
	good := httptest.NewRequest(http.MethodPost, "/payments", nil)
	good.Header.Set("X-Service-Token", tokenA)
	good.RemoteAddr = "203.0.113.5:1"
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, good)
	if rec.Code != http.StatusOK || len(lim.calls) != 1 || lim.calls[0].cost != 0 {
		t.Fatalf("a valid token must pass and consume nothing, got %d %+v", rec.Code, lim.calls)
	}
}

func TestAuthThrottle_ExhaustedIPIsRejectedBeforeTheTokenIsChecked(t *testing.T) {
	lim := &fakeLimiter{allow: false, retryAfter: 30 * time.Second}
	h := authThrottled(lim)

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Service-Token", tokenA)
	req.RemoteAddr = "203.0.113.5:1"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "30" {
		t.Fatalf("an IP out of auth-failure tokens gets 429 with Retry-After, got %d %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if len(lim.calls) != 1 {
		t.Errorf("a rejected request must not consume a further token, got %+v", lim.calls)
	}
}

func TestAuthThrottle_NilDisablesThrottling(t *testing.T) {
	provider := NewStaticTokenProvider(map[string]string{tokenA: "merchant-a"}, nil)
	h := Authenticate(provider, noopLog{}, nil)(okHandler())

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Service-Token", "wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("got %d", rec.Code)
	}
}
