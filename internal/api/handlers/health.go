package handlers

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"samarth/payment-service/internal/app/gatewaystatus"
	"samarth/payment-service/internal/ports"
)

const healthCheckTimeout = 3 * time.Second

const certExpiringWindow = 14 * 24 * time.Hour

const (
	statusHealthy   = "healthy"
	statusUnhealthy = "unhealthy"
	statusDegraded  = "degraded"
	statusStarting  = "starting"
	statusDraining  = "draining"
	statusExpiring  = "expiring"
	statusExpired   = "expired"
)

type Pinger interface {
	Ping(ctx context.Context) error
}

type GatewayStatusReader interface {
	Statuses(ctx context.Context) []gatewaystatus.Status
}

type CertificateChecker interface {
	Name() string
	NotAfter() time.Time
}

type HealthHandler struct {
	db       Pinger
	cache    Pinger
	gateways GatewayStatusReader
	certs    []CertificateChecker
	log      ports.Logger

	ready    atomic.Bool
	draining atomic.Bool
}

func NewHealthHandler(db, cache Pinger, gateways GatewayStatusReader, log ports.Logger, certs ...CertificateChecker) *HealthHandler {
	return &HealthHandler{db: db, cache: cache, gateways: gateways, certs: certs, log: log}
}

func (h *HealthHandler) SetReady() {
	h.ready.Store(true)
}

func (h *HealthHandler) SetDraining() {
	h.draining.Store(true)
}

type componentResponse struct {
	Status string `json:"status"`
}

type overallResponse struct {
	Status     string            `json:"status"`
	Components map[string]string `json:"components"`
}

type gatewayStatusResponse struct {
	GatewayID           string     `json:"gateway_id"`
	Available           bool       `json:"available"`
	Reason              string     `json:"reason,omitempty"`
	Active              bool       `json:"active"`
	BreakerState        string     `json:"breaker_state"`
	CooldownUntil       *time.Time `json:"cooldown_until,omitempty"`
	ConsecutiveFailures int        `json:"consecutive_failures"`
}

type gatewaysResponse struct {
	Gateways []gatewayStatusResponse `json:"gateways"`
}

func (h *HealthHandler) Liveness(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, componentResponse{Status: statusHealthy})
}

func (h *HealthHandler) Database(w http.ResponseWriter, r *http.Request) {
	h.component(w, r, "database", h.db)
}

func (h *HealthHandler) Cache(w http.ResponseWriter, r *http.Request) {
	h.component(w, r, "cache", h.cache)
}

func (h *HealthHandler) Overall(w http.ResponseWriter, r *http.Request) {
	components := map[string]string{"app": statusHealthy}
	status, code := statusHealthy, http.StatusOK

	switch {
	case h.draining.Load():
		components["app"] = statusDraining
		status, code = statusUnhealthy, http.StatusServiceUnavailable
	case !h.ready.Load():
		components["app"] = statusStarting
		status, code = statusUnhealthy, http.StatusServiceUnavailable
	}

	now := time.Now()
	for _, c := range h.certs {
		key := "tls_" + c.Name()
		remaining := c.NotAfter().Sub(now)
		switch {
		case remaining <= 0:
			components[key] = statusExpired
			if status == statusHealthy {
				status = statusDegraded
			}
		case remaining < certExpiringWindow:
			components[key] = statusExpiring
		default:
			components[key] = statusHealthy
		}
	}

	if h.check(r.Context(), "database", h.db) {
		components["database"] = statusHealthy
	} else {
		components["database"] = statusUnhealthy
		status, code = statusUnhealthy, http.StatusServiceUnavailable
	}

	if h.check(r.Context(), "cache", h.cache) {
		components["cache"] = statusHealthy
	} else {
		components["cache"] = statusDegraded
		if status == statusHealthy {
			status = statusDegraded
		}
	}

	writeJSON(w, code, overallResponse{Status: status, Components: components})
}

func (h *HealthHandler) Gateways(w http.ResponseWriter, r *http.Request) {
	resp := gatewaysResponse{Gateways: []gatewayStatusResponse{}}
	if h.gateways != nil {
		for _, s := range h.gateways.Statuses(r.Context()) {
			g := gatewayStatusResponse{
				GatewayID:           s.GatewayID,
				Available:           s.Available,
				Reason:              s.Reason,
				Active:              s.Active,
				BreakerState:        s.BreakerState,
				ConsecutiveFailures: s.ConsecutiveFailures,
			}
			if !s.CooldownUntil.IsZero() {
				cooldown := s.CooldownUntil.UTC()
				g.CooldownUntil = &cooldown
			}
			resp.Gateways = append(resp.Gateways, g)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *HealthHandler) component(w http.ResponseWriter, r *http.Request, name string, p Pinger) {
	if h.check(r.Context(), name, p) {
		writeJSON(w, http.StatusOK, componentResponse{Status: statusHealthy})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, componentResponse{Status: statusUnhealthy})
}

func (h *HealthHandler) check(ctx context.Context, name string, p Pinger) bool {
	if p == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()
	if err := p.Ping(ctx); err != nil {
		if h.log != nil {
			h.log.Warn(ports.LogEventHealthCheckFailed, map[string]any{"check": name, "error": err.Error()})
		}
		return false
	}
	return true
}
