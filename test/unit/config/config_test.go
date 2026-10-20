package config_test

import . "samarth/payment-service/config"

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
	c.Outbox.WorkerCount = 1
	c.RateLimit.FallbackMultiplier = 0.5
	c.Routing.FXReconciliationTolerancePct = 1.0
	c.Gateway.HTTPTimeout = 30 * time.Second
	c.Gateway.MaxAttempts = 3
	c.Jobs.IdempotencyProcessingTimeoutSec = 300
	c.Redis.Addrs = []string{"localhost:6379"}
	c.Redis.RateLimitDB = 0
	c.Redis.CacheDB = 1
	return c
}

// setRequiredEnv sets every variable LoadConfig requires and clears the Redis
// variables under test, so each test controls exactly what it reads.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	required := map[string]string{
		"ENVIRONMENT":                      "dev",
		"DATABASE_PRIMARY_HOST":            "localhost",
		"DATABASE_NAME":                    "payment",
		"DATABASE_USER":                    "payment",
		"DATABASE_PASSWORD":                "payment",
		"REDIS_ADDRS":                      "localhost:6379",
		"SNS_GATEWAY_CONFIG_UPDATES_TOPIC": "arn:aws:sns:us-east-1:123:gateway-updates",
		"SNS_DEAD_LETTER_TOPIC":            "arn:aws:sns:us-east-1:123:dead-letters",
		"SNS_RECONCILIATION_JOBS_TOPIC":    "arn:aws:sns:us-east-1:123:reconciliation-jobs",
		"AWS_REGION":                       "us-east-1",
		"TLS_CERT_FILE":                    "/etc/tls/cert.pem",
		"TLS_KEY_FILE":                     "/etc/tls/key.pem",
		"TLS_CA_FILE":                      "/etc/tls/ca.pem",
		"AUDIT_LOG_BUCKET":                 "audit",
	}
	for k, v := range required {
		t.Setenv(k, v)
	}
	for _, k := range []string{"REDIS_TLS_ENABLED", "REDIS_USERNAME", "REDIS_PASSWORD", "REDIS_RATE_LIMIT_DB", "REDIS_CACHE_DB"} {
		t.Setenv(k, "")
	}
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

func TestValidate_RejectsMultipleRedisAddrs(t *testing.T) {
	c := validatableConfig()

	// More than one address makes go-redis build a cluster client, which only
	// has database 0, so the rate-limit and cache databases would collapse.
	c.Redis.Addrs = []string{"redis-1:6379", "redis-2:6379"}
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "single primary endpoint") {
		t.Fatalf("expected multiple REDIS_ADDRS to be rejected, got: %v", err)
	}
}

func TestValidate_RedisAddrMustIncludePort(t *testing.T) {
	c := validatableConfig()
	c.Redis.Addrs = []string{"redis.internal"}
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "host:port") {
		t.Fatalf("expected an address without a port to be rejected, got: %v", err)
	}
}

func TestValidate_RedisDatabasesMustDiffer(t *testing.T) {
	c := validatableConfig()
	c.Redis.CacheDB = c.Redis.RateLimitDB
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("expected identical Redis databases to be rejected, got: %v", err)
	}
}

func TestLoadConfig_RedisTLSOnByDefault(t *testing.T) {
	setRequiredEnv(t)

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !c.Redis.TLSEnabled {
		t.Error("REDIS_TLS_ENABLED must default to true so an unset variable never means plaintext")
	}
	if c.Redis.Username != "" || c.Redis.Password != "" {
		t.Errorf("credentials should be empty when unset, got username=%q", c.Redis.Username)
	}
}

func TestLoadConfig_RedisTLSAndCredentialsFromEnv(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("REDIS_TLS_ENABLED", "false")
	t.Setenv("REDIS_USERNAME", "payment-service")
	t.Setenv("REDIS_PASSWORD", "test-password")

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.Redis.TLSEnabled {
		t.Error("REDIS_TLS_ENABLED=false should disable TLS")
	}
	if c.Redis.Username != "payment-service" || c.Redis.Password != "test-password" {
		t.Errorf("credentials not read from the environment, got username=%q", c.Redis.Username)
	}
}

func TestLoadConfig_RejectsInvalidRedisTLSFlag(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("REDIS_TLS_ENABLED", "maybe")

	_, err := LoadConfig()
	if err == nil || !strings.Contains(err.Error(), "REDIS_TLS_ENABLED") {
		t.Fatalf("an unparseable REDIS_TLS_ENABLED must fail startup, not fall back to the default, got: %v", err)
	}
}
