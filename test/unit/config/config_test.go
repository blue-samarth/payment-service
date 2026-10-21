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
	c.Outbox.RelayMode = RelayModePolling
	c.Outbox.PollIntervalSec = 10
	c.Outbox.ClaimTTLSec = 60
	c.Outbox.WorkerCount = 1
	c.RateLimit.FallbackMultiplier = 0.5
	c.RateLimit.Capacity = 100
	c.RateLimit.RefillPerSec = 50
	c.RateLimit.UserCapacity = 20
	c.RateLimit.UserRefillPerSec = 5
	c.RateLimit.IPCapacity = 100
	c.RateLimit.IPRefillPerSec = 50
	c.RateLimit.WebhookCapacity = 300
	c.RateLimit.WebhookRefillPerSec = 100
	c.Routing.FXReconciliationTolerancePct = 1.0
	c.Gateway.HTTPTimeout = 30 * time.Second
	c.Gateway.MaxAttempts = 3
	c.Gateway.CircuitBreakerFailureThreshold = 5
	c.App.ShutdownDrainDelay = 5 * time.Second
	c.App.ShutdownTimeout = 105 * time.Second
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
		"AUTH_FAILURE_LIMIT_CAPACITY", "AUTH_FAILURE_REFILL_PER_SEC",
		"RATE_LIMIT_USER_CAPACITY", "RATE_LIMIT_USER_REFILL_PER_SEC",
		"RATE_LIMIT_IP_CAPACITY", "RATE_LIMIT_IP_REFILL_PER_SEC",
		"OUTBOX_RELAY_MODE",
		"RATE_LIMIT_WEBHOOK_CAPACITY", "RATE_LIMIT_WEBHOOK_REFILL_PER_SEC",
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

var (
	svcToken1   = "svc-1-" + strings.Repeat("a", 32)
	svcToken2   = "svc-2-" + strings.Repeat("b", 32)
	paddedToken = strings.Repeat("c", 32) + "==="
	opsToken1   = "ops-1-" + strings.Repeat("d", 32)
	opsToken2   = "ops-2-" + strings.Repeat("e", 32)
)

func TestLoadConfig_ServiceTokens(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SERVICE_TOKENS", " "+svcToken1+"="+merchantA+", "+paddedToken+"="+merchantB+" ")
	t.Setenv("OPS_TOKENS", opsToken1+", ,"+opsToken2)

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := c.Auth.ServiceTokens[svcToken1]; got != merchantA {
		t.Errorf("svc-1 merchant = %q, want %q", got, merchantA)
	}
	if got := c.Auth.ServiceTokens[paddedToken]; got != merchantB {
		t.Errorf("a token containing '=' must split on the last '=', got map %v", c.Auth.ServiceTokens)
	}
	if len(c.Auth.OpsTokens) != 2 || c.Auth.OpsTokens[0] != opsToken1 || c.Auth.OpsTokens[1] != opsToken2 {
		t.Errorf("ops tokens = %v", c.Auth.OpsTokens)
	}
}

func TestLoadConfig_ServiceTokenMerchantIsNormalised(t *testing.T) {
	for _, form := range []string{
		strings.ToUpper(merchantA),
		"{" + merchantA + "}",
		"urn:uuid:" + merchantA,
	} {
		t.Run(form, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("SERVICE_TOKENS", svcToken1+"="+form)

			c, err := LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if got := c.Auth.ServiceTokens[svcToken1]; got != merchantA {
				t.Errorf("merchant must be stored in canonical form so one merchant has one bucket, got %q", got)
			}
		})
	}
}

func TestLoadConfig_RejectsMalformedServiceTokens(t *testing.T) {
	cases := map[string]struct {
		raw  string
		want string
	}{
		"empty merchant":     {svcToken1 + "=", "entry 1 must be token=merchantID"},
		"empty token":        {"=" + merchantA, "entry 1 must be token=merchantID"},
		"no separator":       {svcToken1, "entry 1 must be token=merchantID"},
		"merchant not uuid":  {svcToken1 + "=merchant-9", "entry 1: merchant ID must be a UUID"},
		"short token":        {"svc-1=" + merchantA, "entry 1: token must be at least 32 characters"},
		"duplicate token":    {svcToken1 + "=" + merchantA + "," + svcToken1 + "=" + merchantB, "entry 2 repeats an earlier token"},
		"second entry wrong": {svcToken1 + "=" + merchantA + "," + svcToken2 + "=", "entry 2 must be token=merchantID"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("SERVICE_TOKENS", tc.raw)

			_, err := LoadConfig()
			if err == nil || !strings.Contains(err.Error(), "SERVICE_TOKENS "+tc.want) {
				t.Fatalf("want error containing %q, got: %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "svc-1") || strings.Contains(err.Error(), "svc-2") {
				t.Fatalf("the error must not echo token values, got: %v", err)
			}
		})
	}
}

func TestLoadConfig_RejectsMalformedOpsTokens(t *testing.T) {
	cases := map[string]struct {
		raw  string
		want string
	}{
		"short token":     {"ops-1", "OPS_TOKENS entry 1: token must be at least 32 characters"},
		"duplicate token": {opsToken1 + "," + opsToken1, "OPS_TOKENS entry 2 repeats an earlier token"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("OPS_TOKENS", tc.raw)

			_, err := LoadConfig()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got: %v", tc.want, err)
			}
			if strings.Contains(err.Error(), "ops-1") {
				t.Fatalf("the error must not echo token values, got: %v", err)
			}
		})
	}
}

func TestValidate_ProductionGuardrails(t *testing.T) {
	cases := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"unauthenticated": {func(c *Config) { c.App.AllowNoAuth = true }, "ALLOW_NO_AUTH=true is not allowed when ENVIRONMENT=prod"},
		"optional mTLS":   {func(c *Config) { c.App.MtlsStrictMode = false }, "MTLS_STRICT_MODE=false is not allowed when ENVIRONMENT=prod"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := validatableConfig()
			c.App.Environment = "prod"
			c.App.MtlsStrictMode = true
			tc.mutate(c)
			if err := Validate(c); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got: %v", tc.want, err)
			}

			c.App.Environment = "staging"
			if err := Validate(c); err != nil {
				t.Fatalf("outside prod the toggle stays available, got: %v", err)
			}
		})
	}

	t.Run("lb webhook TLS and plaintext Redis stay allowed in prod", func(t *testing.T) {
		c := validatableConfig()
		c.App.Environment = "prod"
		c.App.MtlsStrictMode = true
		c.App.WebhookTLSMode = WebhookTLSModeLB
		c.Redis.TLSEnabled = false
		if err := Validate(c); err != nil {
			t.Fatalf("TLS at the load balancer is a valid production setup, got: %v", err)
		}
	})
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

func TestValidate_ShutdownTimeoutCoversTheGatewayBudget(t *testing.T) {
	c := validatableConfig()
	c.App.ShutdownTimeout = 89 * time.Second
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "SHUTDOWN_TIMEOUT (1m29s) must be at least the gateway budget") {
		t.Fatalf("a shutdown budget below the gateway budget must be rejected, got: %v", err)
	}

	c.App.ShutdownTimeout = 90 * time.Second
	if err := Validate(c); err != nil {
		t.Fatalf("a shutdown budget equal to the gateway budget is enough, got: %v", err)
	}
}

func TestLoadConfig_ShutdownTimeoutDefaultFollowsTheGatewayBudget(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("GATEWAY_HTTP_TIMEOUT", "10s")
	t.Setenv("GATEWAY_MAX_ATTEMPTS", "2")

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.App.ShutdownTimeout != 35*time.Second {
		t.Errorf("default SHUTDOWN_TIMEOUT must be the gateway budget + 15s, got %s", c.App.ShutdownTimeout)
	}
}

func TestLoadConfig_AuthFailureLimit(t *testing.T) {
	setRequiredEnv(t)

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.RateLimit.AuthFailureCapacity != 10 || c.RateLimit.AuthFailureRefillPerSec != 0.1 {
		t.Errorf("defaults = %d/%v, want 10/0.1", c.RateLimit.AuthFailureCapacity, c.RateLimit.AuthFailureRefillPerSec)
	}

	t.Setenv("AUTH_FAILURE_LIMIT_CAPACITY", "0")
	t.Setenv("AUTH_FAILURE_REFILL_PER_SEC", "0")
	if _, err := LoadConfig(); err != nil {
		t.Fatalf("capacity 0 disables the throttle, so its refill rate is irrelevant, got: %v", err)
	}

	for key, raw := range map[string]string{"AUTH_FAILURE_LIMIT_CAPACITY": "ten", "AUTH_FAILURE_REFILL_PER_SEC": "fast"} {
		t.Run(key, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(key, raw)
			if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("an unparseable %s must fail startup, got: %v", key, err)
			}
		})
	}
}

func TestLoadConfig_UserAndWebhookLimits(t *testing.T) {
	setRequiredEnv(t)

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.RateLimit.UserCapacity != c.RateLimit.Capacity || c.RateLimit.UserRefillPerSec != c.RateLimit.RefillPerSec {
		t.Errorf("the user limit must default to the merchant limit so upgrading changes nothing, got %d/%v vs %d/%v",
			c.RateLimit.UserCapacity, c.RateLimit.UserRefillPerSec, c.RateLimit.Capacity, c.RateLimit.RefillPerSec)
	}

	t.Setenv("RATE_LIMIT_CAPACITY", "40")
	t.Setenv("RATE_LIMIT_REFILL_PER_SEC", "8")
	c, err = LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.RateLimit.UserCapacity != 40 || c.RateLimit.UserRefillPerSec != 8 {
		t.Errorf("the user default must follow a configured merchant limit, got %d/%v", c.RateLimit.UserCapacity, c.RateLimit.UserRefillPerSec)
	}

	t.Setenv("RATE_LIMIT_USER_CAPACITY", "10")
	t.Setenv("RATE_LIMIT_USER_REFILL_PER_SEC", "2")
	c, err = LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.RateLimit.UserCapacity != 10 || c.RateLimit.UserRefillPerSec != 2 {
		t.Errorf("an explicit user limit must be used, got %d/%v", c.RateLimit.UserCapacity, c.RateLimit.UserRefillPerSec)
	}
	if c.RateLimit.IPCapacity != c.RateLimit.Capacity || c.RateLimit.IPRefillPerSec != c.RateLimit.RefillPerSec {
		t.Errorf("the IP limit must default to the merchant limit, got %d/%v", c.RateLimit.IPCapacity, c.RateLimit.IPRefillPerSec)
	}
	if c.RateLimit.WebhookCapacity != 300 || c.RateLimit.WebhookRefillPerSec != 100 {
		t.Errorf("webhook defaults = %d/%v, want 300/100", c.RateLimit.WebhookCapacity, c.RateLimit.WebhookRefillPerSec)
	}
}

func TestValidate_UserAndWebhookLimits(t *testing.T) {
	cases := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"user capacity zero":         {func(c *Config) { c.RateLimit.UserCapacity = 0 }, "RATE_LIMIT_USER_CAPACITY must be at least 1"},
		"user refill zero":           {func(c *Config) { c.RateLimit.UserRefillPerSec = 0 }, "RATE_LIMIT_USER_REFILL_PER_SEC"},
		"webhook capacity zero":      {func(c *Config) { c.RateLimit.WebhookCapacity = 0 }, "RATE_LIMIT_WEBHOOK_CAPACITY"},
		"webhook refill zero":        {func(c *Config) { c.RateLimit.WebhookRefillPerSec = 0 }, "RATE_LIMIT_WEBHOOK_REFILL_PER_SEC"},
		"user above merchant":        {func(c *Config) { c.RateLimit.UserCapacity = 101 }, "RATE_LIMIT_USER_CAPACITY must not exceed"},
		"user refill above merchant": {func(c *Config) { c.RateLimit.UserRefillPerSec = 51 }, "RATE_LIMIT_USER_REFILL_PER_SEC must not exceed"},
		"ip capacity zero":           {func(c *Config) { c.RateLimit.IPCapacity = 0 }, "RATE_LIMIT_IP_CAPACITY"},
		"ip refill zero":             {func(c *Config) { c.RateLimit.IPRefillPerSec = 0 }, "RATE_LIMIT_IP_REFILL_PER_SEC"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := validatableConfig()
			tc.mutate(c)
			if err := Validate(c); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got: %v", tc.want, err)
			}
		})
	}
}

func TestValidate_AuthFailureLimit(t *testing.T) {
	c := validatableConfig()
	c.RateLimit.AuthFailureCapacity = -1
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "AUTH_FAILURE_LIMIT_CAPACITY") {
		t.Fatalf("a negative capacity must be rejected, got: %v", err)
	}

	c = validatableConfig()
	c.RateLimit.AuthFailureCapacity = 10
	c.RateLimit.AuthFailureRefillPerSec = 0
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "AUTH_FAILURE_REFILL_PER_SEC") {
		t.Fatalf("an enabled throttle needs a positive refill, got: %v", err)
	}
}

func TestLoadConfig_RelayModeIsPollingOnly(t *testing.T) {
	setRequiredEnv(t)

	c, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if c.Outbox.RelayMode != RelayModePolling {
		t.Errorf("the relay only polls, so the default must say so, got %q", c.Outbox.RelayMode)
	}

	t.Setenv("OUTBOX_RELAY_MODE", "polling")
	if _, err := LoadConfig(); err != nil {
		t.Fatalf("polling is accepted explicitly, got: %v", err)
	}

	t.Setenv("OUTBOX_RELAY_MODE", "cdc")
	if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "OUTBOX_RELAY_MODE=cdc is not implemented") {
		t.Fatalf("asking for CDC must fail loudly instead of silently polling, got: %v", err)
	}

	t.Setenv("OUTBOX_RELAY_MODE", "kafka")
	if _, err := LoadConfig(); err == nil || !strings.Contains(err.Error(), "OUTBOX_RELAY_MODE must be 'polling'") {
		t.Fatalf("an unknown mode must be rejected, got: %v", err)
	}
}

func TestLoadConfig_LeftoverCDCVariablesAreIgnored(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("OUTBOX_RELAY_WAL_LAG_ALERT_THRESHOLD_MB", "9000")
	t.Setenv("OUTBOX_RELAY_WAL_LAG_CRITICAL_THRESHOLD_MB", "100")
	t.Setenv("OUTBOX_RELAY_SLOT_SIZE_ALERT_MB", "not-a-number")

	if _, err := LoadConfig(); err != nil {
		t.Fatalf("settings for a relay mode that does not exist must not be read or validated, got: %v", err)
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
	if c.App.ShutdownDrainDelay != 5*time.Second || c.App.ShutdownTimeout != 105*time.Second {
		t.Errorf("shutdown defaults = %v/%v", c.App.ShutdownDrainDelay, c.App.ShutdownTimeout)
	}
}
