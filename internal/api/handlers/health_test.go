package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"samarth/payment-service/internal/app/gatewaystatus"
)

type fakePinger struct {
	err   error
	calls atomic.Int32
}

func (p *fakePinger) Ping(context.Context) error {
	p.calls.Add(1)
	return p.err
}

type fakeGatewayStatuses struct{ statuses []gatewaystatus.Status }

func (f fakeGatewayStatuses) Statuses(context.Context) []gatewaystatus.Status { return f.statuses }

func serveHealth(t *testing.T, handler http.HandlerFunc) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not JSON: %v (%s)", err, rec.Body.String())
	}
	return rec.Code, body
}

func TestLiveness_DoesNotTouchDependencies(t *testing.T) {
	db, cache := &fakePinger{err: errors.New("down")}, &fakePinger{err: errors.New("down")}
	h := NewHealthHandler(db, cache, nil, noopLog{})

	code, body := serveHealth(t, h.Liveness)
	if code != http.StatusOK || body["status"] != "healthy" {
		t.Fatalf("liveness must report healthy regardless of dependencies, got %d %v", code, body)
	}
	if db.calls.Load() != 0 || cache.calls.Load() != 0 {
		t.Errorf("liveness must not ping dependencies, db=%d cache=%d", db.calls.Load(), cache.calls.Load())
	}
}

func TestComponentChecks(t *testing.T) {
	cases := []struct {
		name     string
		dbErr    error
		cacheErr error
		handler  func(*HealthHandler) http.HandlerFunc
		wantCode int
	}{
		{"database healthy", nil, nil, func(h *HealthHandler) http.HandlerFunc { return h.Database }, http.StatusOK},
		{"database down", errors.New("down"), nil, func(h *HealthHandler) http.HandlerFunc { return h.Database }, http.StatusServiceUnavailable},
		{"cache healthy", nil, nil, func(h *HealthHandler) http.HandlerFunc { return h.Cache }, http.StatusOK},
		{"cache down", nil, errors.New("down"), func(h *HealthHandler) http.HandlerFunc { return h.Cache }, http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHealthHandler(&fakePinger{err: tc.dbErr}, &fakePinger{err: tc.cacheErr}, nil, noopLog{})
			code, _ := serveHealth(t, tc.handler(h))
			if code != tc.wantCode {
				t.Errorf("got %d, want %d", code, tc.wantCode)
			}
		})
	}
}

func TestOverall(t *testing.T) {
	cases := []struct {
		name          string
		dbErr         error
		cacheErr      error
		wantCode      int
		wantStatus    string
		wantDatabase  string
		wantCacheComp string
	}{
		{"all healthy", nil, nil, http.StatusOK, "healthy", "healthy", "healthy"},
		{"cache down is degraded, not a failure", nil, errors.New("down"), http.StatusOK, "degraded", "healthy", "degraded"},
		{"database down fails readiness", errors.New("down"), nil, http.StatusServiceUnavailable, "unhealthy", "unhealthy", "healthy"},
		{"both down", errors.New("down"), errors.New("down"), http.StatusServiceUnavailable, "unhealthy", "unhealthy", "degraded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHealthHandler(&fakePinger{err: tc.dbErr}, &fakePinger{err: tc.cacheErr}, nil, noopLog{})
			h.SetReady()
			code, body := serveHealth(t, h.Overall)
			if code != tc.wantCode || body["status"] != tc.wantStatus {
				t.Fatalf("got %d %v, want %d %s", code, body["status"], tc.wantCode, tc.wantStatus)
			}
			components, _ := body["components"].(map[string]any)
			if components["app"] != "healthy" || components["database"] != tc.wantDatabase || components["cache"] != tc.wantCacheComp {
				t.Errorf("unexpected components: %v", components)
			}
		})
	}
}

type fakeCert struct {
	name     string
	notAfter time.Time
}

func (c fakeCert) Name() string        { return c.name }
func (c fakeCert) NotAfter() time.Time { return c.notAfter }

func TestOverall_NotReadyUntilListenersServe(t *testing.T) {
	h := NewHealthHandler(&fakePinger{}, &fakePinger{}, nil, noopLog{})

	code, body := serveHealth(t, h.Overall)
	components, _ := body["components"].(map[string]any)
	if code != http.StatusServiceUnavailable || components["app"] != "starting" {
		t.Fatalf("readiness must fail before the listeners are serving, got %d %v", code, body)
	}

	h.SetReady()
	if code, _ := serveHealth(t, h.Overall); code != http.StatusOK {
		t.Fatalf("readiness must pass once ready, got %d", code)
	}
}

func TestOverall_DrainingFailsReadinessButNotLiveness(t *testing.T) {
	h := NewHealthHandler(&fakePinger{}, &fakePinger{}, nil, noopLog{})
	h.SetReady()
	h.SetDraining()

	code, body := serveHealth(t, h.Overall)
	components, _ := body["components"].(map[string]any)
	if code != http.StatusServiceUnavailable || components["app"] != "draining" {
		t.Fatalf("a draining instance must leave the load balancer, got %d %v", code, body)
	}
	if code, _ := serveHealth(t, h.Liveness); code != http.StatusOK {
		t.Fatalf("draining must not fail liveness, or the orchestrator restarts a pod that is shutting down cleanly, got %d", code)
	}
}

func TestOverall_Certificates(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name       string
		notAfter   time.Time
		wantCode   int
		wantStatus string
		wantCert   string
	}{
		{"valid", now.Add(90 * 24 * time.Hour), http.StatusOK, "healthy", "healthy"},
		{"inside the renewal window", now.Add(48 * time.Hour), http.StatusOK, "healthy", "expiring"},
		{"expired while running", now.Add(-time.Minute), http.StatusServiceUnavailable, "unhealthy", "expired"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHealthHandler(&fakePinger{}, &fakePinger{}, nil, noopLog{},
				fakeCert{name: "merchant", notAfter: now.Add(90 * 24 * time.Hour)},
				fakeCert{name: "webhook", notAfter: tc.notAfter},
			)
			h.SetReady()

			code, body := serveHealth(t, h.Overall)
			components, _ := body["components"].(map[string]any)
			if code != tc.wantCode || body["status"] != tc.wantStatus {
				t.Fatalf("got %d %v, want %d %s", code, body["status"], tc.wantCode, tc.wantStatus)
			}
			if components["tls_webhook"] != tc.wantCert || components["tls_merchant"] != "healthy" {
				t.Errorf("unexpected certificate components: %v", components)
			}
		})
	}
}

func TestGateways_ReportsEveryGateway(t *testing.T) {
	cooldown := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	h := NewHealthHandler(&fakePinger{}, &fakePinger{}, fakeGatewayStatuses{statuses: []gatewaystatus.Status{
		{GatewayID: "payu", Available: true, Active: true, BreakerState: "CLOSED"},
		{GatewayID: "stripe", Available: false, Reason: "circuit_breaker_cooldown", Active: true, BreakerState: "OPEN", CooldownUntil: cooldown, ConsecutiveFailures: 5},
	}}, noopLog{})

	rec := httptest.NewRecorder()
	h.Gateways(rec, httptest.NewRequest(http.MethodGet, "/health-gateways", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway status is diagnostic and must always be 200, got %d", rec.Code)
	}

	var resp gatewaysResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Gateways) != 2 {
		t.Fatalf("expected 2 gateways, got %d", len(resp.Gateways))
	}
	stripe := resp.Gateways[1]
	if stripe.Available || stripe.Reason != "circuit_breaker_cooldown" || stripe.BreakerState != "OPEN" || stripe.ConsecutiveFailures != 5 {
		t.Errorf("unexpected stripe status: %+v", stripe)
	}
	if stripe.CooldownUntil == nil || !stripe.CooldownUntil.Equal(cooldown) {
		t.Errorf("expected cooldown_until %s, got %v", cooldown, stripe.CooldownUntil)
	}
	if payu := resp.Gateways[0]; !payu.Available || payu.CooldownUntil != nil || payu.Reason != "" {
		t.Errorf("an available gateway should have no reason or cooldown: %+v", payu)
	}
}

func TestGateways_NoReaderIsEmptyList(t *testing.T) {
	h := NewHealthHandler(&fakePinger{}, &fakePinger{}, nil, noopLog{})
	rec := httptest.NewRecorder()
	h.Gateways(rec, httptest.NewRequest(http.MethodGet, "/health-gateways", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "{\"gateways\":[]}\n" {
		t.Fatalf("expected an empty list, got %d %q", rec.Code, rec.Body.String())
	}
}
