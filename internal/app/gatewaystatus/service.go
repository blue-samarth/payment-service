package gatewaystatus

import (
	"context"
	"sort"
	"time"

	approuting "samarth/payment-service/internal/app/routing"
	"samarth/payment-service/internal/domain/gateway"
	"samarth/payment-service/internal/ports"
)

const (
	BreakerStateUnknown     = "unknown"
	ReasonConfigUnavailable = "config_unavailable"
)

type GatewayLister interface {
	IDs() []string
}

type ConfigReader interface {
	GetGatewayConfig(ctx context.Context, gatewayID string) (*ports.GatewayConfig, error)
}

type AvailabilityChecker interface {
	Availability(ctx context.Context, cfg *ports.GatewayConfig) (approuting.Availability, error)
}

type BreakerReader interface {
	Get(ctx context.Context, gatewayID string) (*gateway.CircuitBreaker, error)
}

type Status struct {
	GatewayID           string
	Available           bool
	Reason              string
	Active              bool
	BreakerState        string
	CooldownUntil       time.Time
	ConsecutiveFailures int
}

type Service struct {
	gateways     GatewayLister
	config       ConfigReader
	availability AvailabilityChecker
	breaker      BreakerReader
	log          ports.Logger
}

func NewService(gateways GatewayLister, config ConfigReader, availability AvailabilityChecker, breaker BreakerReader, log ports.Logger) *Service {
	return &Service{gateways: gateways, config: config, availability: availability, breaker: breaker, log: log}
}

func (s *Service) Statuses(ctx context.Context) []Status {
	ids := append([]string(nil), s.gateways.IDs()...)
	sort.Strings(ids)

	out := make([]Status, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.status(ctx, id))
	}
	return out
}

func (s *Service) status(ctx context.Context, gatewayID string) Status {
	st := Status{GatewayID: gatewayID, BreakerState: BreakerStateUnknown}

	cfg, err := s.config.GetGatewayConfig(ctx, gatewayID)
	if err != nil {
		s.warn(gatewayID, "gateway_config", err)
		st.Reason = ReasonConfigUnavailable
		return st
	}
	st.Active = cfg.IsActive

	avail, err := s.availability.Availability(ctx, cfg)
	st.Reason = avail.Reason
	st.Available = avail.Reason == ""
	if err != nil {
		s.warn(gatewayID, "circuit_breaker", err)
		return st
	}
	st.BreakerState = avail.BreakerState
	st.CooldownUntil = avail.CooldownUntil

	if cb, err := s.breaker.Get(ctx, gatewayID); err == nil {
		st.ConsecutiveFailures = cb.ConsecutiveFailures
	}
	return st
}

func (s *Service) warn(gatewayID, check string, err error) {
	s.log.Warn(ports.LogEventHealthCheckFailed, map[string]any{
		ports.FieldGatewayID: gatewayID,
		"check":              check,
		"error":              err.Error(),
	})
}
