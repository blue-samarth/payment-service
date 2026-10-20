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
	c.RateLimit.Capacity = 100
	c.RateLimit.RefillPerSec = 50
	c.Routing.FXReconciliationTolerancePct = 1.0
	c.Gateway.HTTPTimeout = 30 * time.Second
	c.Gateway.MaxAttempts = 3
	c.Gateway.CircuitBreakerFailureThreshold = 5
	c.App.ShutdownDrainDelay = 5 * time.Second
	c.App.ShutdownTimeout = 15 * time.Second
	c.Jobs.IdempotencyProcessingTimeoutSec = 300
	c.Redis.Addrs = []string{"localhost:6379"}
	c.Redis.RateLimitDB = 0
	c.Redis.CacheDB = 1
	c.App.Port = 8080
	c.App.WebhookPort = 8081
	c.App.HealthPort = 8082
	c.App.WebhookTLSMode = WebhookTLSModeApp
	c.Security.WebhookTLSCertFile = "/etc/tls/webhook.pem"
	c.Security.WebhookTLSKeyFile = "/etc/tls/webhook-key.pem"
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
		"WEBHOOK_TLS_CERT_FILE":            "/etc/tls/webhook.pem",
		"WEBHOOK_TLS_KEY_FILE":             "/etc/tls/webhook-key.pem",
		"AUDIT_LOG_BUCKET":                 "audit",
	}
	for k, v := range required {
		t.Setenv(k, v)
	}
	for _, k := range []string{
		"REDIS_TLS_ENABLED", "REDIS_USERNAME", "REDIS_PASSWORD", "REDIS_RATE_LIMIT_DB", "REDIS_CACHE_DB",
		"PORT", "WEBHOOK_PORT", "HEALTH_PORT", "WEBHOOK_TLS_MODE",
		"SERVICE_TOKENS", "OPS_TOKENS", "WEBHOOK_SECRETS", "TRUSTED_PROXY_CIDRS",
		"RATE_LIMIT_CAPACITY", "RATE_LIMIT_REFILL_PER_SEC", "CIRCUIT_BREAKER_FAILURE_THRESHOLD",
		"SHUTDOWN_DRAIN_DELAY", "SHUTDOWN_TIMEOUT",
	} {
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

func TestValidate_RedisAddrForm(t *testing.T) {
	// net.SplitHostPort accepts an empty host or port without error, so each
	// component is checked on its own.
	rejected := map[string]string{
		"no port":          "redis.internal",
		"empty port":       "redis.internal:",
		"empty host":       ":6379",
		"non-numeric port": "redis.internal:abc",
		"signed port":      "redis.internal:+6379",
		"port zero":        "redis.internal:0",
		"port too large":   "redis.internal:65536",
	}
	for name, addr := range rejected {
		t.Run(name, func(t *testing.T) {
			c := validatableConfig()
			c.Redis.Addrs = []string{addr}
			err := Validate(c)
			if err == nil || !strings.Contains(err.Error(), "host:port") {
				t.Fatalf("expected %q to be rejected, got: %v", addr, err)
			}
		})
	}

	accepted := []string{"redis.internal:6379", "10.0.0.5:6379", "[::1]:6379", "redis.internal:65535"}
	for _, addr := range accepted {
		t.Run("valid "+addr, func(t *testing.T) {
			c := validatableConfig()
			c.Redis.Addrs = []string{addr}
			if err := Validate(c); err != nil {
				t.Fatalf("expected %q to be accepted, got: %v", addr, err)
			}
		})
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

func TestValidate_ListenerPortsMustBeDistinct(t *testing.T) {
	c := validatableConfig()
	c.App.HealthPort = c.App.Port
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "must be distinct") {
		t.Fatalf("expected a shared port to be rejected, got: %v", err)
	}
}

func TestValidate_ListenerPortRange(t *testing.T) {
	for _, port := range []int{0, -1, 65536} {
		c := validatableConfig()
		c.App.WebhookPort = port
		err := Validate(c)
		if err == nil || !strings.Contains(err.Error(), "between 1 and 65535") {
			t.Errorf("WEBHOOK_PORT=%d: expected a range error, got: %v", port, err)
		}
	}
}

func TestValidate_WebhookTLSMode(t *testing.T) {
	t.Run("unknown mode", func(t *testing.T) {
		c := validatableConfig()
		c.App.WebhookTLSMode = "none"
		if err := Validate(c); err == nil || !strings.Contains(err.Error(), "WEBHOOK_TLS_MODE") {
			t.Fatalf("expected an unknown mode to be rejected, got: %v", err)
		}
	})
	t.Run("app mode requires a certificate pair", func(t *testing.T) {
		c := validatableConfig()
		c.Security.WebhookTLSKeyFile = ""
		if err := Validate(c); err == nil || !strings.Contains(err.Error(), "WEBHOOK_TLS_CERT_FILE") {
			t.Fatalf("app mode without a key file must fail, not fall back to plaintext, got: %v", err)
		}
	})
	t.Run("lb mode needs no certificate", func(t *testing.T) {
		c := validatableConfig()
		c.App.WebhookTLSMode = WebhookTLSModeLB
		c.Security.WebhookTLSCertFile, c.Security.WebhookTLSKeyFile = "", ""
		if err := Validate(c); err != nil {
			t.Fatalf("lb mode should validate without certificate files, got: %v", err)
		}
	})
}

func TestLoadConfig_ListenerDefaults(t *testing.T) {
	setRequiredEnv(t)

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.App.Port != 8080 || c.App.WebhookPort != 8081 || c.App.HealthPort != 8082 {
		t.Errorf("unexpected default ports: merchant=%d webhook=%d health=%d", c.App.Port, c.App.WebhookPort, c.App.HealthPort)
	}
	if c.App.WebhookTLSMode != WebhookTLSModeApp {
		t.Errorf("WEBHOOK_TLS_MODE must default to app so plaintext is always an explicit choice, got %q", c.App.WebhookTLSMode)
	}
}

func TestLoadConfig_AppModeWithoutWebhookCertFails(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WEBHOOK_TLS_CERT_FILE", "")

	if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "WEBHOOK_TLS_CERT_FILE") {
		t.Fatalf("a missing webhook certificate must fail startup in the default mode, got: %v", err)
	}
}

const merchantA = "0b7c3a52-6a3e-4c1f-9d2e-6f1a2b3c4d5e"
const merchantB = "5f0d9e7a-1b2c-4d3e-8f4a-9b0c1d2e3f4a"

func TestLoadConfig_ServiceTokens(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SERVICE_TOKENS", " svc-1="+merchantA+", c3ZjLTI===="+merchantB+" ")
	t.Setenv("OPS_TOKENS", "ops-1, ,ops-2")

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := c.Auth.ServiceTokens["svc-1"]; got != merchantA {
		t.Errorf("svc-1 merchant = %q, want %q", got, merchantA)
	}
	if got := c.Auth.ServiceTokens["c3ZjLTI==="]; got != merchantB {
		t.Errorf("a token containing '=' must split on the last '=', got map %v", c.Auth.ServiceTokens)
	}
	if len(c.Auth.OpsTokens) != 2 || c.Auth.OpsTokens[0] != "ops-1" || c.Auth.OpsTokens[1] != "ops-2" {
		t.Errorf("ops tokens = %v, want [ops-1 ops-2]", c.Auth.OpsTokens)
	}
}

func TestLoadConfig_RejectsMalformedServiceTokens(t *testing.T) {
	cases := map[string]struct {
		raw  string
		want string
	}{
		"empty merchant":     {"svc-1=", "entry 1 must be token=merchantID"},
		"empty token":        {"=" + merchantA, "entry 1 must be token=merchantID"},
		"no separator":       {"svc-1", "entry 1 must be token=merchantID"},
		"merchant not uuid":  {"svc-1=merchant-9", "entry 1: merchant ID must be a UUID"},
		"duplicate token":    {"svc-1=" + merchantA + ",svc-1=" + merchantB, "entry 2 repeats an earlier token"},
		"second entry wrong": {"svc-1=" + merchantA + ",svc-2=", "entry 2 must be token=merchantID"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("SERVICE_TOKENS", tc.raw)

			_, err := LoadConfig()
			if err == nil || !strings.Contains(err.Error(), "SERVICE_TOKENS "+tc.want) {
				t.Fatalf("want error containing %q, got: %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "svc-1") {
				t.Fatalf("the error must not echo token values, got: %v", err)
			}
		})
	}
}

func TestLoadConfig_WebhookSecrets(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WEBHOOK_SECRETS", "stripe=whsec_abc=def,razorpay=rzp")

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := c.Auth.WebhookSecrets["stripe"]; got != "whsec_abc=def" {
		t.Errorf("a secret containing '=' must split on the first '=', got %q", got)
	}
	if got := c.Auth.WebhookSecrets["razorpay"]; got != "rzp" {
		t.Errorf("razorpay secret = %q", got)
	}
}

func TestLoadConfig_RejectsMalformedWebhookSecrets(t *testing.T) {
	for _, raw := range []string{"stripe=", "=whsec_abc", "stripe", "stripe=a,stripe=b"} {
		t.Run(raw, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("WEBHOOK_SECRETS", raw)

			_, err := LoadConfig()
			if err == nil || !strings.Contains(err.Error(), "WEBHOOK_SECRETS entry") {
				t.Fatalf("want a WEBHOOK_SECRETS error, got: %v", err)
			}
			if strings.Contains(err.Error(), "whsec_abc") {
				t.Fatalf("the error must not echo secret values, got: %v", err)
			}
		})
	}
}

func TestLoadConfig_TrustedProxies(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("TRUSTED_PROXY_CIDRS", "10.1.0.0/16, 192.168.1.7/24,fd00::/8")

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	got := make([]string, len(c.RateLimit.TrustedProxies))
	for i, p := range c.RateLimit.TrustedProxies {
		got[i] = p.String()
	}
	if strings.Join(got, ",") != "10.1.0.0/16,192.168.1.0/24,fd00::/8" {
		t.Errorf("trusted proxies = %v", got)
	}

	t.Setenv("TRUSTED_PROXY_CIDRS", "10.1.0.0")
	if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "TRUSTED_PROXY_CIDRS entry 1") {
		t.Fatalf("a bare address must be rejected, got: %v", err)
	}
}

func TestLoadConfig_TrustedProxiesDefaultEmpty(t *testing.T) {
	setRequiredEnv(t)

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(c.RateLimit.TrustedProxies) != 0 {
		t.Errorf("no proxy may be trusted by default, got %v", c.RateLimit.TrustedProxies)
	}
}

func TestLoadConfig_RejectsInvalidNumbersInsteadOfFallingBack(t *testing.T) {
	cases := map[string]string{
		"RATE_LIMIT_CAPACITY":               "lots",
		"RATE_LIMIT_REFILL_PER_SEC":         "fast",
		"CIRCUIT_BREAKER_FAILURE_THRESHOLD": "5.5",
		"SHUTDOWN_DRAIN_DELAY":              "5",
		"SHUTDOWN_TIMEOUT":                  "soon",
	}
	for key, raw := range cases {
		t.Run(key, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(key, raw)

			if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("an unparseable %s must fail startup, got: %v", key, err)
			}
		})
	}
}

func TestValidate_RuntimeLimits(t *testing.T) {
	cases := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"zero capacity":      {func(c *Config) { c.RateLimit.Capacity = 0 }, "RATE_LIMIT_CAPACITY"},
		"zero refill":        {func(c *Config) { c.RateLimit.RefillPerSec = 0 }, "RATE_LIMIT_REFILL_PER_SEC"},
		"zero threshold":     {func(c *Config) { c.Gateway.CircuitBreakerFailureThreshold = 0 }, "CIRCUIT_BREAKER_FAILURE_THRESHOLD"},
		"negative drain":     {func(c *Config) { c.App.ShutdownDrainDelay = -time.Second }, "SHUTDOWN_DRAIN_DELAY"},
		"zero shutdown time": {func(c *Config) { c.App.ShutdownTimeout = 0 }, "SHUTDOWN_TIMEOUT"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := validatableConfig()
			tc.mutate(c)
			if err := Validate(c); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error mentioning %s, got: %v", tc.want, err)
			}
		})
	}
}

func TestLoadConfig_RuntimeDefaults(t *testing.T) {
	setRequiredEnv(t)

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.RateLimit.Capacity != 100 || c.RateLimit.RefillPerSec != 50 {
		t.Errorf("rate limit defaults = %d/%v", c.RateLimit.Capacity, c.RateLimit.RefillPerSec)
	}
	if c.Gateway.CircuitBreakerFailureThreshold != 5 {
		t.Errorf("breaker threshold default = %d", c.Gateway.CircuitBreakerFailureThreshold)
	}
	if c.App.ShutdownDrainDelay != 5*time.Second || c.App.ShutdownTimeout != 15*time.Second {
		t.Errorf("shutdown defaults = %v/%v", c.App.ShutdownDrainDelay, c.App.ShutdownTimeout)
	}
}
