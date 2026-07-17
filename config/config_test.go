package config

import (
	"strings"
	"testing"
	"time"
)

// validatableConfig returns a Config that passes every Validate rule, so a test
// can flip a single field and isolate the rule under test.
func validatableConfig() *Config {
	c := &Config{}
	c.App.Environment = "dev"
	c.Observability.LogLevel = "info"
	c.Outbox.WALLagAlertThresholdMB = 2000
	c.Outbox.WALLagCriticalThresholdMB = 5000
	c.Outbox.PollIntervalSec = 10
	c.Outbox.ClaimTTLSec = 60
	c.RateLimit.FallbackMultiplier = 0.5
	c.Routing.FXReconciliationTolerancePct = 1.0
	c.Gateway.HTTPTimeout = 30 * time.Second
	c.Gateway.MaxAttempts = 3
	c.Jobs.IdempotencyProcessingTimeoutSec = 300
	return c
}

func TestValidate_BaselineIsValid(t *testing.T) {
	if err := Validate(validatableConfig()); err != nil {
		t.Fatalf("baseline config should validate, got: %v", err)
	}
}

func TestValidate_SNSPublisherRequiresTopic(t *testing.T) {
	c := validatableConfig()

	// Selecting the sns publisher without a domain-events topic must fail closed
	// rather than boot a relay that can't deliver anything.
	c.Outbox.Publisher = "sns"
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "SNS_PAYMENT_EVENTS_TOPIC") {
		t.Fatalf("expected missing-topic error for sns publisher, got: %v", err)
	}

	c.SNS.PaymentEventsTopic = "arn:aws:sns:us-east-1:123:payment-events"
	if err := Validate(c); err != nil {
		t.Fatalf("sns publisher with a topic should validate, got: %v", err)
	}
}

func TestValidate_RejectsUnknownPublisher(t *testing.T) {
	c := validatableConfig()
	c.Outbox.Publisher = "kafka"
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "OUTBOX_PUBLISHER") {
		t.Fatalf("expected unknown-publisher error, got: %v", err)
	}
}

func TestValidate_IdempotencyTimeoutMustExceedGatewayBudget(t *testing.T) {
	c := validatableConfig() // budget = 3 * 30s = 90s

	// At the budget boundary (90s) it must fail — the reaper could release a
	// reservation for an operation still within its gateway retry window.
	c.Jobs.IdempotencyProcessingTimeoutSec = 90
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "LEASE_REAPER_IDEMPOTENCY_TIMEOUT_SEC") {
		t.Fatalf("expected gateway-budget validation error at the boundary, got: %v", err)
	}

	// Comfortably above the budget it passes.
	c.Jobs.IdempotencyProcessingTimeoutSec = 120
	if err := Validate(c); err != nil {
		t.Fatalf("120s > 90s budget should validate, got: %v", err)
	}
}
