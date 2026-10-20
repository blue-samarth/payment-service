package gatewaystatus_test

import . "samarth/payment-service/internal/app/gatewaystatus"

import (
	"context"
	"errors"
	"testing"
	"time"

	approuting "samarth/payment-service/internal/app/routing"
	"samarth/payment-service/internal/domain/gateway"
	"samarth/payment-service/internal/ports"
)

type ids []string

func (i ids) IDs() []string { return i }

type configs struct {
	byID map[string]*ports.GatewayConfig
	err  error
}

func (c configs) GetGatewayConfig(_ context.Context, id string) (*ports.GatewayConfig, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.byID[id], nil
}

type availability struct {
	byID map[string]approuting.Availability
	err  error
}

func (a availability) Availability(_ context.Context, cfg *ports.GatewayConfig) (approuting.Availability, error) {
	return a.byID[cfg.GatewayID], a.err
}

type breakers struct {
	failures map[string]int
	calls    int
}

func (b *breakers) Get(_ context.Context, id string) (*gateway.CircuitBreaker, error) {
	b.calls++
	return &gateway.CircuitBreaker{GatewayID: id, ConsecutiveFailures: b.failures[id]}, nil
}

type noopLogger struct{}

func (noopLogger) Info(string, map[string]any)         {}
func (noopLogger) Warn(string, map[string]any)         {}
func (noopLogger) Error(string, map[string]any, error) {}
func (noopLogger) Debug(string, map[string]any)        {}
func (noopLogger) Trace(string, map[string]any)        {}
func (l noopLogger) With(map[string]any) ports.Logger  { return l }

func activeConfigs(gatewayIDs ...string) configs {
	c := configs{byID: map[string]*ports.GatewayConfig{}}
	for _, id := range gatewayIDs {
		c.byID[id] = &ports.GatewayConfig{GatewayID: id, IsActive: true}
	}
	return c
}

func TestStatuses_SortedWithAvailabilityAndFailures(t *testing.T) {
	cooldown := time.Now().UTC().Add(time.Minute)
	b := &breakers{failures: map[string]int{"stripe": 4}}
	s := NewService(
		ids{"stripe", "payu", "razorpay"},
		activeConfigs("stripe", "payu", "razorpay"),
		availability{byID: map[string]approuting.Availability{
			"stripe":   {BreakerState: "OPEN", CooldownUntil: cooldown, Reason: "circuit_breaker_cooldown"},
			"payu":     {BreakerState: "CLOSED"},
			"razorpay": {BreakerState: "HALF_OPEN"},
		}},
		b, noopLogger{},
	)

	got := s.Statuses(context.Background())
	if len(got) != 3 || got[0].GatewayID != "payu" || got[1].GatewayID != "razorpay" || got[2].GatewayID != "stripe" {
		t.Fatalf("expected gateways sorted by id, got %+v", got)
	}
	if !got[0].Available || !got[1].Available {
		t.Errorf("gateways with no reason must be available: %+v", got[:2])
	}
	stripe := got[2]
	if stripe.Available || stripe.Reason != "circuit_breaker_cooldown" || stripe.BreakerState != "OPEN" ||
		!stripe.CooldownUntil.Equal(cooldown) || stripe.ConsecutiveFailures != 4 || !stripe.Active {
		t.Errorf("unexpected stripe status: %+v", stripe)
	}
}

func TestStatuses_ConfigUnavailable(t *testing.T) {
	b := &breakers{}
	s := NewService(ids{"stripe"}, configs{err: errors.New("db down")}, availability{}, b, noopLogger{})

	got := s.Statuses(context.Background())[0]
	if got.Available || got.Reason != ReasonConfigUnavailable || got.BreakerState != BreakerStateUnknown || got.Active {
		t.Errorf("a gateway whose config cannot be read must be reported unavailable and unknown: %+v", got)
	}
	if b.calls != 0 {
		t.Errorf("no breaker read is needed when config is unavailable, got %d", b.calls)
	}
}

func TestStatuses_BreakerStoreDownReportsUnknown(t *testing.T) {
	b := &breakers{}
	s := NewService(
		ids{"stripe"},
		activeConfigs("stripe"),
		availability{byID: map[string]approuting.Availability{"stripe": {BreakerState: "CLOSED"}}, err: errors.New("redis down")},
		b, noopLogger{},
	)

	got := s.Statuses(context.Background())[0]
	if got.BreakerState != BreakerStateUnknown {
		t.Errorf("breaker state must be unknown when the store is unreachable, got %q", got.BreakerState)
	}
	if !got.Available {
		t.Error("availability follows the config fallback, as the router does, so the gateway stays available")
	}
	if b.calls != 0 {
		t.Errorf("failure count must not be read from a store already known to be down, got %d reads", b.calls)
	}
}
