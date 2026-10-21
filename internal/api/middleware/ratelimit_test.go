package middleware

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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

func (f *fakeLimiter) Take(_ context.Context, buckets []Bucket, cost int64) RateDecision {
	f.calls = append(f.calls, take{buckets: buckets, cost: cost})
	return RateDecision{Allowed: f.allow, RetryAfter: f.retryAfter}
}

func (f *fakeLimiter) last() []Bucket {
	return f.calls[len(f.calls)-1].buckets
}

func (f *fakeLimiter) bucket(namespace string) (Bucket, bool) {
	for _, b := range f.last() {
		if b.Namespace == namespace {
			return b, true
		}
	}
	return Bucket{}, false
}

type atomicLimiter struct {
	mu     sync.Mutex
	tokens map[string]int64
}

func (a *atomicLimiter) Take(_ context.Context, buckets []Bucket, cost int64) RateDecision {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, b := range buckets {
		if _, ok := a.tokens[b.ID]; !ok {
			a.tokens[b.ID] = b.Capacity
		}
		if a.tokens[b.ID] < max(cost, 1) {
			return RateDecision{Allowed: false, RetryAfter: time.Second}
		}
	}
	for _, b := range buckets {
		a.tokens[b.ID] -= cost
	}
	return RateDecision{Allowed: true}
}

type noopLog struct{}

func (noopLog) Info(string, map[string]any)         {}
func (noopLog) Warn(string, map[string]any)         {}
func (noopLog) Error(string, map[string]any, error) {}
func (noopLog) Debug(string, map[string]any)        {}
func (noopLog) Trace(string, map[string]any)        {}
func (l noopLog) With(map[string]any) ports.Logger  { return l }

type capturingLog struct {
	noopLog
	mu     sync.Mutex
	fields map[string]map[string]any
}

func (c *capturingLog) Warn(event string, f map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fields[event] = f
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	})
}

var testLimits = Limits{
	Merchant: Limit{Capacity: 100, RefillPerSec: 50},
	User:     Limit{Capacity: 20, RefillPerSec: 5},
	IP:       Limit{Capacity: 100, RefillPerSec: 50},
}

var webhookLimit = Limit{Capacity: 300, RefillPerSec: 100}

var (
	tokenA = "svc-a-" + strings.Repeat("a", 32)
	tokenB = "svc-b-" + strings.Repeat("b", 32)
)

func merchantChain(lim Limiter, keys RateLimitKeys, throttle *AuthThrottle) http.Handler {
	provider := NewStaticTokenProvider(map[string]string{tokenA: "merchant-a", tokenB: "merchant-b"}, nil)
	return Authenticate(provider, noopLog{}, nil, throttle)(RateLimit(lim, noopLog{}, nil, keys)(okHandler()))
}

func request(token, userID, remote string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	if token != "" {
		req.Header.Set("X-Service-Token", token)
	}
	if userID != "" {
		req.Header.Set("X-User-ID", userID)
	}
	if remote != "" {
		req.RemoteAddr = remote
	}
	return req
}

func TestRateLimit_Allows(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := RateLimit(lim, noopLog{}, nil, RequestKeys(nil, testLimits))(okHandler())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, request("", "", "203.0.113.5:4444"))

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if ip, _ := lim.bucket(NamespaceIP); ip.ID != "203.0.113.5" || ip.Limit != testLimits.IP {
		t.Errorf("expected the IP bucket with the IP limit, got %+v", ip)
	}
	if lim.calls[0].cost != 1 {
		t.Errorf("a request costs one token, got %d", lim.calls[0].cost)
	}
}

func TestRateLimit_Rejects429AndLogsTheClientIP(t *testing.T) {
	lim := &fakeLimiter{allow: false, retryAfter: 2 * time.Second}
	log := &capturingLog{fields: map[string]map[string]any{}}
	metrics := &countingMetrics{counts: map[string]map[string]string{}}
	h := RateLimit(lim, log, metrics, RequestKeys(NewClientIP(nil), testLimits))(okHandler())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, request("", "", "203.0.113.5:4444"))

	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("expected 429 with Retry-After 2, got %d %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if got := log.fields[ports.LogEventRateLimitRejected]["client_ip"]; got != "203.0.113.5" {
		t.Errorf("the rejection log must carry the derived client IP, got %v", got)
	}
	if tags, ok := metrics.counts[ports.MetricRateLimitRejected]; !ok || tags["listener"] != "merchant" {
		t.Errorf("a rejection must be counted with its listener for alerting, got %v", metrics.counts)
	}
}

type countingMetrics struct {
	mu     sync.Mutex
	counts map[string]map[string]string
}

func (m *countingMetrics) Increment(name string, tags map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[name] = tags
}
func (m *countingMetrics) Histogram(string, float64, map[string]string) {}
func (m *countingMetrics) Gauge(string, float64, map[string]string)     {}

func TestRateLimit_EachDimensionCarriesItsOwnLimit(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	merchantChain(lim, RequestKeys(nil, testLimits), nil).ServeHTTP(httptest.NewRecorder(), request(tokenA, "customer-1", "203.0.113.5:1"))

	m, _ := lim.bucket(NamespaceMerchant)
	u, _ := lim.bucket(NamespaceUser)
	if m.Limit != testLimits.Merchant || u.Limit != testLimits.User {
		t.Errorf("merchant and user buckets must carry their own limits, got merchant=%+v user=%+v", m.Limit, u.Limit)
	}
}

func TestRateLimit_MerchantBucketFromTokenNotHeader(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	req := request(tokenA, "", "")
	req.Header.Set("X-Merchant-ID", "victim")
	merchantChain(lim, RequestKeys(nil, testLimits), nil).ServeHTTP(httptest.NewRecorder(), req)

	if got, _ := lim.bucket(NamespaceMerchant); got.ID != "merchant-a" {
		t.Errorf("bucket must key on the token-bound merchant, not X-Merchant-ID, got %q", got.ID)
	}
}

func TestRateLimit_MissingUserIDSkipsTheUserDimension(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := merchantChain(lim, RequestKeys(nil, testLimits), nil)

	for _, token := range []string{tokenA, tokenB} {
		h.ServeHTTP(httptest.NewRecorder(), request(token, "", "203.0.113.5:1"))
		if _, ok := lim.bucket(NamespaceUser); ok {
			t.Fatal("a request without X-User-ID must not be keyed on an empty user bucket")
		}
	}
}

func TestRateLimit_UserIDIsScopedToTheMerchant(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := merchantChain(lim, RequestKeys(nil, testLimits), nil)

	ids := map[string]string{}
	for name, token := range map[string]string{"a": tokenA, "b": tokenB} {
		h.ServeHTTP(httptest.NewRecorder(), request(token, "customer-42", ""))
		b, _ := lim.bucket(NamespaceUser)
		ids[name] = b.ID
	}
	if ids["a"] == "" || ids["a"] == ids["b"] {
		t.Fatalf("the same X-User-ID under two merchants must be two buckets, got %v", ids)
	}
}

func TestRateLimit_UserBucketSkippedWhenItCannotBind(t *testing.T) {
	equal := testLimits
	equal.User = equal.Merchant

	lim := &fakeLimiter{allow: true}
	merchantChain(lim, RequestKeys(nil, equal), nil).ServeHTTP(httptest.NewRecorder(), request(tokenA, "customer-1", ""))
	if _, ok := lim.bucket(NamespaceUser); ok {
		t.Error("a user bucket with the merchant's own limit can never bind, so it must not cost a Redis write")
	}

	RateLimit(lim, noopLog{}, nil, RequestKeys(nil, equal))(okHandler()).ServeHTTP(httptest.NewRecorder(), request("", "customer-1", ""))
	if _, ok := lim.bucket(NamespaceUser); !ok {
		t.Error("without a merchant the user bucket spans every IP, so it can still bind and must be kept")
	}
}

func TestRateLimit_UserBucketKeptOnlyWhenSomeDimensionIsTighter(t *testing.T) {
	cases := map[string]struct {
		user Limit
		keep bool
	}{
		"same limit":                {testLimits.Merchant, false},
		"higher refill, same burst": {Limit{Capacity: testLimits.Merchant.Capacity, RefillPerSec: testLimits.Merchant.RefillPerSec * 2}, false},
		"smaller burst":             {Limit{Capacity: testLimits.Merchant.Capacity / 2, RefillPerSec: testLimits.Merchant.RefillPerSec}, true},
		"slower refill":             {Limit{Capacity: testLimits.Merchant.Capacity, RefillPerSec: testLimits.Merchant.RefillPerSec / 2}, true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			limits := testLimits
			limits.User = tc.user
			lim := &fakeLimiter{allow: true}
			merchantChain(lim, RequestKeys(nil, limits), nil).ServeHTTP(httptest.NewRecorder(), request(tokenA, "customer-1", ""))
			if _, ok := lim.bucket(NamespaceUser); ok != tc.keep {
				t.Errorf("user bucket present=%v, want %v", ok, tc.keep)
			}
		})
	}
}

func TestRateLimit_OverlongUserIDIsRejected(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	rec := httptest.NewRecorder()
	merchantChain(lim, RequestKeys(nil, testLimits), nil).ServeHTTP(rec, request(tokenA, strings.Repeat("u", MaxUserIDLength+1), ""))

	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "invalid_user_id") {
		t.Fatalf("an X-User-ID over %d characters must be a 400, got %d %s", MaxUserIDLength, rec.Code, rec.Body.String())
	}
	if len(lim.calls) != 0 {
		t.Error("a rejected X-User-ID must not reach the limiter")
	}

	rec = httptest.NewRecorder()
	merchantChain(lim, RequestKeys(nil, testLimits), nil).ServeHTTP(rec, request(tokenA, strings.Repeat("u", MaxUserIDLength), ""))
	if rec.Code != http.StatusOK {
		t.Fatalf("exactly %d characters is allowed, got %d", MaxUserIDLength, rec.Code)
	}
}

func TestRateLimit_MerchantCannotReachAWebhookBucket(t *testing.T) {
	merchant := &fakeLimiter{allow: true}
	merchantChain(merchant, RequestKeys(nil, testLimits), nil).ServeHTTP(httptest.NewRecorder(), request(tokenA, "webhook:198.51.100.7", "203.0.113.5:1"))

	webhook := &fakeLimiter{allow: true}
	req := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/stripe", nil)
	req.RemoteAddr = "198.51.100.7:1"
	RateLimit(webhook, noopLog{}, nil, WebhookKeys(nil, webhookLimit))(okHandler()).ServeHTTP(httptest.NewRecorder(), req)

	for _, mb := range merchant.last() {
		for _, wb := range webhook.last() {
			if mb.Namespace == wb.Namespace && mb.ID == wb.ID {
				t.Fatalf("a merchant header reached the webhook bucket %+v", wb)
			}
		}
	}
}

func TestRateLimit_NoPrincipalUsesIPAndScopedUserOnly(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	RateLimit(lim, noopLog{}, nil, RequestKeys(nil, testLimits))(okHandler()).ServeHTTP(httptest.NewRecorder(), request("", "u1", "203.0.113.5:1"))

	if _, ok := lim.bucket(NamespaceMerchant); ok {
		t.Error("without a principal the merchant dimension must be skipped, not keyed on \"\"")
	}
	if user, _ := lim.bucket(NamespaceUser); !strings.HasPrefix(user.ID, unauthenticatedScope) {
		t.Errorf("an unauthenticated user key must be scoped apart from merchant users, got %q", user.ID)
	}
}

func TestRateLimit_UnparseablePeerStillGetsAnIPBucket(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	RateLimit(lim, noopLog{}, nil, RequestKeys(nil, testLimits))(okHandler()).ServeHTTP(httptest.NewRecorder(), request("", "", ""))
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.RemoteAddr = ""
	RateLimit(lim, noopLog{}, nil, WebhookKeys(nil, webhookLimit))(okHandler()).ServeHTTP(httptest.NewRecorder(), req)

	if b, ok := lim.bucket(NamespaceWebhookIP); !ok || b.ID != unknownClient {
		t.Fatalf("a request with no usable address is limited under a shared fallback key, not waved through, got %+v", lim.last())
	}
}

func TestRateLimit_IgnoresForwardedForFromUntrustedPeer(t *testing.T) {
	for name, tc := range map[string]struct {
		keys      RateLimitKeys
		namespace string
	}{
		"merchant": {RequestKeys(NewClientIP(nil), testLimits), NamespaceIP},
		"webhook":  {WebhookKeys(NewClientIP(nil), webhookLimit), NamespaceWebhookIP},
	} {
		t.Run(name, func(t *testing.T) {
			lim := &fakeLimiter{allow: true}
			h := RateLimit(lim, noopLog{}, nil, tc.keys)(okHandler())

			for _, spoofed := range []string{"198.51.100.7", "198.51.100.8", "198.51.100.9"} {
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.Header.Set("X-Forwarded-For", spoofed)
				req.RemoteAddr = "203.0.113.5:5555"
				h.ServeHTTP(httptest.NewRecorder(), req)

				if ip, _ := lim.bucket(tc.namespace); ip.ID != "203.0.113.5" {
					t.Fatalf("a rotated X-Forwarded-For must not change the IP bucket, got %q", ip.ID)
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
		"merchant": {RequestKeys(nil, testLimits), NamespaceIP},
		"webhook":  {WebhookKeys(nil, webhookLimit), NamespaceWebhookIP},
	} {
		t.Run(name, func(t *testing.T) {
			lim := &fakeLimiter{allow: true}
			h := RateLimit(lim, noopLog{}, nil, tc.keys)(okHandler())

			serve := func(remote string) string {
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.RemoteAddr = remote
				h.ServeHTTP(httptest.NewRecorder(), req)
				ip, _ := lim.bucket(tc.namespace)
				return ip.ID
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

func TestRateLimit_WebhookKeysAreOneIPBucketWithTheWebhookLimit(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := RateLimit(lim, noopLog{}, nil, WebhookKeys(nil, webhookLimit))(okHandler())

	req := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/stripe", nil)
	req.RemoteAddr = "203.0.113.5:4444"
	req.Header.Set("X-User-ID", "someone")
	h.ServeHTTP(httptest.NewRecorder(), req)

	got := lim.last()
	want := Bucket{Namespace: NamespaceWebhookIP, ID: "203.0.113.5", Limit: webhookLimit}
	if len(got) != 1 || got[0] != want {
		t.Errorf("webhooks are limited per caller IP only, with their own limit, got %+v", got)
	}
}

func authThrottled(lim Limiter, capacity int64) http.Handler {
	provider := NewStaticTokenProvider(map[string]string{tokenA: "merchant-a"}, nil)
	throttle := &AuthThrottle{Limiter: lim, Limit: Limit{Capacity: capacity, RefillPerSec: 0.0001}}
	return Authenticate(provider, noopLog{}, nil, throttle)(okHandler())
}

func TestAuthThrottle_OneCallPerRequest(t *testing.T) {
	lim := &fakeLimiter{allow: true}
	h := authThrottled(lim, 10)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, request("wrong", "", "203.0.113.5:1"))
	if rec.Code != http.StatusUnauthorized || len(lim.calls) != 1 || lim.calls[0].cost != 1 {
		t.Fatalf("a bad token is one call that consumes one token, got %d %+v", rec.Code, lim.calls)
	}

	lim.calls = nil
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, request(tokenA, "", "203.0.113.5:1"))
	if rec.Code != http.StatusOK || len(lim.calls) != 0 {
		t.Fatalf("a valid token never touches the limiter, got %d %+v", rec.Code, lim.calls)
	}
}

func TestAuthThrottle_ConcurrentFailuresAreCappedAtCapacity(t *testing.T) {
	lim := &atomicLimiter{tokens: map[string]int64{}}
	h := authThrottled(lim, 5)

	const parallel = 100
	codes := make(chan int, parallel)
	var wg sync.WaitGroup
	for range parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, request("wrong", "", "203.0.113.5:1"))
			codes <- rec.Code
		}()
	}
	wg.Wait()
	close(codes)

	answered := 0
	for code := range codes {
		switch code {
		case http.StatusUnauthorized:
			answered++
		case http.StatusTooManyRequests:
		default:
			t.Fatalf("unexpected status %d", code)
		}
	}
	if answered != 5 {
		t.Fatalf("%d parallel guesses must get exactly capacity (5) answers, the rest 429; got %d answered", parallel, answered)
	}
}

func TestAuthThrottle_ValidTokenPassesAnExhaustedIdentity(t *testing.T) {
	lim := &fakeLimiter{allow: false, retryAfter: 30 * time.Second}
	h := authThrottled(lim, 10)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, request("wrong", "", "203.0.113.5:1"))
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "30" {
		t.Fatalf("a bad token on an exhausted identity gets 429 with Retry-After, got %d %q", rec.Code, rec.Header().Get("Retry-After"))
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, request(tokenA, "", "203.0.113.5:1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("a stale-token retry loop on the same certificate must not take the merchant's valid traffic offline, got %d", rec.Code)
	}
}

func withCert(req *http.Request, raw []byte) *http.Request {
	req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{{Raw: raw}}}
	return req
}

func TestAuthThrottle_KeysOnTheClientCertificateNotTheIP(t *testing.T) {
	lim := &atomicLimiter{tokens: map[string]int64{}}
	h := authThrottled(lim, 1)

	h.ServeHTTP(httptest.NewRecorder(), withCert(request("wrong", "", "203.0.113.5:1"), []byte("merchant-x-cert")))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, withCert(request("wrong", "", "203.0.113.5:1"), []byte("merchant-x-cert")))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("the failing certificate is throttled, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, withCert(request("wrong", "", "203.0.113.5:1"), []byte("merchant-y-cert")))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("another certificate behind the same NAT address has its own failure allowance, got %d", rec.Code)
	}
}

func TestAuthThrottle_LogsAreSampledMetricsAreNot(t *testing.T) {
	lim := &fakeLimiter{allow: false}
	log := &countingLog{}
	metrics := &tallyMetrics{counts: map[string]int{}}
	provider := NewStaticTokenProvider(map[string]string{tokenA: "merchant-a"}, nil)
	throttle := &AuthThrottle{Limiter: lim, Limit: Limit{Capacity: 1, RefillPerSec: 1}}
	h := Authenticate(provider, log, metrics, throttle)(okHandler())

	for range 50 {
		h.ServeHTTP(httptest.NewRecorder(), request("wrong", "", "203.0.113.5:1"))
	}
	if metrics.counts[ports.MetricAuthThrottled] != 50 || metrics.counts[ports.MetricAuthRejected] != 50 {
		t.Errorf("every failed token is counted in auth.rejected and every 429 in auth.throttled, got rejected=%d throttled=%d",
			metrics.counts[ports.MetricAuthRejected], metrics.counts[ports.MetricAuthThrottled])
	}
	if got := log.count(ports.LogEventAuthThrottled); got != 1 {
		t.Errorf("a flood within one second must log once, not %d times", got)
	}
}

func TestRateLimit_LogsAreSampledMetricsAreNot(t *testing.T) {
	lim := &fakeLimiter{allow: false}
	log := &countingLog{}
	metrics := &tallyMetrics{counts: map[string]int{}}
	h := RateLimit(lim, log, metrics, RequestKeys(nil, testLimits))(okHandler())

	for range 50 {
		h.ServeHTTP(httptest.NewRecorder(), request("", "", "203.0.113.5:1"))
	}
	if metrics.counts[ports.MetricRateLimitRejected] != 50 || log.count(ports.LogEventRateLimitRejected) != 1 {
		t.Errorf("want 50 counted and 1 logged, got %d counted and %d logged",
			metrics.counts[ports.MetricRateLimitRejected], log.count(ports.LogEventRateLimitRejected))
	}
}

func TestLogSampler_ReportsSuppressedCount(t *testing.T) {
	s := newLogSampler(20 * time.Millisecond)
	if ok, _ := s.allow(); !ok {
		t.Fatal("the first event is logged")
	}
	for range 3 {
		if ok, _ := s.allow(); ok {
			t.Fatal("events inside the interval are suppressed")
		}
	}
	time.Sleep(30 * time.Millisecond)
	if ok, suppressed := s.allow(); !ok || suppressed != 3 {
		t.Fatalf("the next logged event carries the suppressed count, got ok=%v suppressed=%d", ok, suppressed)
	}
}

type countingLog struct {
	noopLog
	mu     sync.Mutex
	events map[string]int
}

func (c *countingLog) Warn(event string, _ map[string]any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.events == nil {
		c.events = map[string]int{}
	}
	c.events[event]++
}

func (c *countingLog) count(event string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.events[event]
}

type tallyMetrics struct {
	mu     sync.Mutex
	counts map[string]int
}

func (m *tallyMetrics) Increment(name string, _ map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[name]++
}
func (m *tallyMetrics) Histogram(string, float64, map[string]string) {}
func (m *tallyMetrics) Gauge(string, float64, map[string]string)     {}

func TestAuthThrottle_LogsIdentityAndClientIP(t *testing.T) {
	lim := &fakeLimiter{allow: false}
	log := &capturingLog{fields: map[string]map[string]any{}}
	provider := NewStaticTokenProvider(map[string]string{tokenA: "merchant-a"}, nil)
	metrics := &countingMetrics{counts: map[string]map[string]string{}}
	throttle := &AuthThrottle{Limiter: lim, ClientIP: NewClientIP(nil), Limit: Limit{Capacity: 1, RefillPerSec: 1}}
	Authenticate(provider, log, metrics, throttle)(okHandler()).ServeHTTP(httptest.NewRecorder(), request("wrong", "", "203.0.113.5:1"))

	f := log.fields[ports.LogEventAuthThrottled]
	if f["client_ip"] != "203.0.113.5" || f["identity"] != "ip:203.0.113.5" {
		t.Errorf("the throttle log must say who was throttled, got %v", f)
	}
	if tags, ok := metrics.counts[ports.MetricAuthThrottled]; !ok || tags["identity_type"] != "ip" {
		t.Errorf("a throttle must be counted by identity type (low cardinality) for alerting, got %v", metrics.counts)
	}

	Authenticate(provider, log, metrics, throttle)(okHandler()).ServeHTTP(httptest.NewRecorder(), withCert(request("wrong", "", "203.0.113.5:1"), []byte("cert")))
	if tags := metrics.counts[ports.MetricAuthThrottled]; tags["identity_type"] != "cert" {
		t.Errorf("a certificate identity is counted as cert, got %v", tags)
	}
}

func TestAuthThrottle_NilDisablesThrottlingButNotMetrics(t *testing.T) {
	provider := NewStaticTokenProvider(map[string]string{tokenA: "merchant-a"}, nil)
	metrics := &tallyMetrics{counts: map[string]int{}}
	h := Authenticate(provider, noopLog{}, metrics, nil)(okHandler())

	for range 20 {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, request("wrong", "", ""))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("without a throttle a bad token is always 401, got %d", rec.Code)
		}
	}
	if metrics.counts[ports.MetricAuthRejected] != 20 {
		t.Fatalf("with the throttle disabled (AUTH_FAILURE_LIMIT_CAPACITY=0 or no limiter) a flood must still be visible in auth.rejected, got %d",
			metrics.counts[ports.MetricAuthRejected])
	}
}
