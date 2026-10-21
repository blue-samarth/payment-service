package config

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

func requireEnv(key string, errs *[]string) string {
	v := os.Getenv(key)
	if v == "" {
		*errs = append(*errs, key+" is required")
	}
	return v
}
func getEnvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
func getEnvInt(key string, def int, errs *[]string) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		*errs = append(*errs, key+" must be a valid integer")
		return def
	}
	return n
}
func getEnvInt64(key string, def int64, errs *[]string) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		*errs = append(*errs, key+" must be a valid 64-bit integer")
		return def
	}
	return n
}
func getEnvFloat64(key string, def float64, errs *[]string) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		*errs = append(*errs, key+" must be a valid number")
		return def
	}
	return f
}
func getEnvBool(key string, def bool, errs *[]string) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		*errs = append(*errs, key+" must be a valid boolean (true/false)")
		return def
	}
	return b
}
func getEnvDuration(key string, def time.Duration, errs *[]string) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		*errs = append(*errs, key+" must be a valid duration (e.g. 30s, 5m, 1h)")
		return def
	}
	return d
}

const (
	WebhookTLSModeApp = "app"
	WebhookTLSModeLB  = "lb"
)

const MinTokenLength = 32

const ShutdownTimeoutMargin = 15 * time.Second

type AppConfig struct {
	Environment    string
	ServiceVersion string
	Port           int
	WebhookPort    int
	HealthPort     int
	WebhookTLSMode string
	MtlsStrictMode bool
	AllowNoAuth    bool

	ShutdownDrainDelay time.Duration
	ShutdownTimeout    time.Duration
}
type AuthConfig struct {
	ServiceTokens  map[string]string
	OpsTokens      []string
	WebhookSecrets map[string]string
}
type StartupConfig struct {
	ConnectMaxAttempts    int
	ConnectAttemptTimeout time.Duration
	ConnectBackoff        time.Duration
}
type DatabaseConfig struct {
	PrimaryHost       string
	ReplicaHost       string
	Port              int
	Name              string
	User              string
	Password          string
	SSLMode           string
	SearchPath        string
	MaxOpenConns      int
	MaxIdleConns      int
	ConnMaxLifetime   time.Duration
	ConnMaxIdleTime   time.Duration
	HealthCheckPeriod time.Duration
	MigrationLockKey  int64
}
type RedisConfig struct {
	Addrs               []string
	RateLimitDB         int
	CacheDB             int
	TLSEnabled          bool
	Username            string
	Password            string
	DialTimeout         time.Duration
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	HealthCheckInterval time.Duration
}
type SNSConfig struct {
	GatewayConfigUpdatesTopic string
	DeadLetterTopic           string
	ReconciliationJobsTopic   string
	PaymentEventsTopic        string
	PublishMaxRetries         int
}
type AWSConfig struct {
	Region                 string
	SecretsManagerCacheTTL time.Duration
}
type OutboxConfig struct {
	RelayMode                   string
	ShardCount                  int
	WALLagAlertThresholdMB      int64
	WALLagCriticalThresholdMB   int64
	SlotSizeAlertMB             int64
	ConsumerHeartbeatTimeoutSec int
	ReconnectIntervalSec        int
	PollIntervalSec             int
	MaxAttempts                 int
	BatchSize                   int
	ClaimTTLSec                 int
	Publisher                   string
	SNSAggregateVersionAttr     bool
	WorkerIndex                 int
	WorkerCount                 int
}
type RateLimitConfig struct {
	Capacity                int64
	RefillPerSec            float64
	AuthFailureCapacity     int64
	AuthFailureRefillPerSec float64
	TrustedProxies          []netip.Prefix
	FallbackMultiplier      float64
	LocalMaxBuckets         int
	HealthCheckInterval     time.Duration
}
type RoutingConfig struct {
	SnapshotTTLSeconds           int
	FeeCacheTTLSec               int
	FXReconciliationTolerancePct float64
}
type ReconciliationConfig struct {
	RunIntervalHours          int
	ReportFetchTimeoutSeconds int
	AutoResolutionEnabled     bool
}
type ObservabilityConfig struct {
	Backend                    string
	LogLevel                   string
	OTLPEndpoint               string
	OTLPProtocol               string
	TailSamplingP99ThresholdMs int
}
type SecurityConfig struct {
	TLSCertFile             string
	TLSKeyFile              string
	TLSCAFile               string
	WebhookTLSCertFile      string
	WebhookTLSKeyFile       string
	CertRefreshIntervalSec  int
	TokenCacheTTLSec        int
	DEKCacheTTLSec          int
	DEKRotationBatchSize    int
	DEKRotationBatchSleepMs int
}
type GatewayConfig struct {
	HTTPTimeout                    time.Duration
	MaxAttempts                    int
	CircuitBreakerFailureThreshold int
}

func (g GatewayConfig) Budget() time.Duration {
	return time.Duration(g.MaxAttempts) * g.HTTPTimeout
}

type JobsConfig struct {
	LockRetryIntervalSec            int
	LockTimeoutMinutes              int
	LeaseExpiryIntervalSec          int
	FeeSnapshotIntervalHours        int
	IdempotencyProcessingTimeoutSec int
	RunTimeoutMinutes               int
	PartitionWeeksAhead             int
	PartitionRetentionWeeks         int
	PartitionDropAfterDays          int
}
type DataRetentionConfig struct {
	BatchSize      int
	BatchSleepMs   int
	AuditLogBucket string
}
type Config struct {
	App            AppConfig
	Auth           AuthConfig
	Startup        StartupConfig
	Database       DatabaseConfig
	Redis          RedisConfig
	SNS            SNSConfig
	AWS            AWSConfig
	Outbox         OutboxConfig
	RateLimit      RateLimitConfig
	Routing        RoutingConfig
	Gateway        GatewayConfig
	Reconciliation ReconciliationConfig
	Observability  ObservabilityConfig
	Security       SecurityConfig
	Jobs           JobsConfig
	DataRetention  DataRetentionConfig
}

// LoadConfig reads configuration from environment variables, applies defaults, and validates required fields.
func LoadConfig() (*Config, error) {
	c := &Config{}
	var errs []string

	c.App.Environment = requireEnv("ENVIRONMENT", &errs)
	c.App.ServiceVersion = getEnvDefault("SERVICE_VERSION", "unknown")
	c.App.Port = getEnvInt("PORT", 8080, &errs)
	c.App.WebhookPort = getEnvInt("WEBHOOK_PORT", 8081, &errs)
	c.App.HealthPort = getEnvInt("HEALTH_PORT", 8082, &errs)
	c.App.WebhookTLSMode = getEnvDefault("WEBHOOK_TLS_MODE", WebhookTLSModeApp)
	c.App.MtlsStrictMode = getEnvBool("MTLS_STRICT_MODE", true, &errs)
	c.App.AllowNoAuth = getEnvBool("ALLOW_NO_AUTH", false, &errs)
	c.App.ShutdownDrainDelay = getEnvDuration("SHUTDOWN_DRAIN_DELAY", 5*time.Second, &errs)

	c.Auth.ServiceTokens = parseServiceTokens(os.Getenv("SERVICE_TOKENS"), &errs)
	c.Auth.OpsTokens = parseOpsTokens(os.Getenv("OPS_TOKENS"), &errs)
	c.Auth.WebhookSecrets = parseWebhookSecrets(os.Getenv("WEBHOOK_SECRETS"), &errs)

	c.Startup.ConnectMaxAttempts = getEnvInt("STARTUP_CONNECT_MAX_ATTEMPTS", 5, &errs)
	c.Startup.ConnectAttemptTimeout = getEnvDuration("STARTUP_CONNECT_ATTEMPT_TIMEOUT", 15*time.Second, &errs)
	c.Startup.ConnectBackoff = getEnvDuration("STARTUP_CONNECT_BACKOFF", 2*time.Second, &errs)

	c.Database.PrimaryHost = requireEnv("DATABASE_PRIMARY_HOST", &errs)
	c.Database.ReplicaHost = getEnvDefault("DATABASE_REPLICA_HOST", "")
	c.Database.Port = getEnvInt("DATABASE_PORT", 5432, &errs)
	c.Database.Name = requireEnv("DATABASE_NAME", &errs)
	c.Database.User = requireEnv("DATABASE_USER", &errs)
	c.Database.Password = requireEnv("DATABASE_PASSWORD", &errs)
	c.Database.SSLMode = getEnvDefault("DATABASE_SSL_MODE", "verify-full")
	c.Database.MaxOpenConns = getEnvInt("DATABASE_MAX_OPEN_CONNS", 25, &errs)
	c.Database.MaxIdleConns = getEnvInt("DATABASE_MAX_IDLE_CONNS", 5, &errs)
	c.Database.ConnMaxLifetime = getEnvDuration("DATABASE_CONN_MAX_LIFETIME", 30*time.Minute, &errs)
	c.Database.ConnMaxIdleTime = getEnvDuration("DATABASE_CONN_MAX_IDLE_TIME", 5*time.Minute, &errs)
	c.Database.HealthCheckPeriod = getEnvDuration("DATABASE_HEALTH_CHECK_PERIOD", 30*time.Second, &errs)
	c.Database.MigrationLockKey = getEnvInt64("DATABASE_MIGRATION_LOCK_KEY", 1234567890, &errs)

	redisAddrs := getEnvDefault("REDIS_ADDRS", "")
	if redisAddrs == "" {
		errs = append(errs, "REDIS_ADDRS is required")
	} else {
		parts := strings.Split(redisAddrs, ",")
		addrs := make([]string, 0, len(parts))
		for _, p := range parts {
			if p = strings.TrimSpace(p); p != "" {
				addrs = append(addrs, p)
			}
		}
		if len(addrs) == 0 {
			errs = append(errs, "REDIS_ADDRS is required")
		} else {
			c.Redis.Addrs = addrs
		}
	}
	c.Redis.RateLimitDB = getEnvInt("REDIS_RATE_LIMIT_DB", 0, &errs)
	c.Redis.CacheDB = getEnvInt("REDIS_CACHE_DB", 1, &errs)
	c.Redis.TLSEnabled = getEnvBool("REDIS_TLS_ENABLED", true, &errs)
	c.Redis.Username = getEnvDefault("REDIS_USERNAME", "")
	c.Redis.Password = getEnvDefault("REDIS_PASSWORD", "")
	c.Redis.DialTimeout = getEnvDuration("REDIS_DIAL_TIMEOUT", 5*time.Second, &errs)
	c.Redis.ReadTimeout = getEnvDuration("REDIS_READ_TIMEOUT", 3*time.Second, &errs)
	c.Redis.WriteTimeout = getEnvDuration("REDIS_WRITE_TIMEOUT", 3*time.Second, &errs)
	healthCheckMs := getEnvInt("RATE_LIMIT_REDIS_HEALTH_CHECK_INTERVAL_MS", 500, &errs)
	c.Redis.HealthCheckInterval = time.Duration(healthCheckMs) * time.Millisecond

	c.SNS.GatewayConfigUpdatesTopic = requireEnv("SNS_GATEWAY_CONFIG_UPDATES_TOPIC", &errs)
	c.SNS.DeadLetterTopic = requireEnv("SNS_DEAD_LETTER_TOPIC", &errs)
	c.SNS.ReconciliationJobsTopic = requireEnv("SNS_RECONCILIATION_JOBS_TOPIC", &errs)
	c.SNS.PaymentEventsTopic = getEnvDefault("SNS_PAYMENT_EVENTS_TOPIC", "")
	c.SNS.PublishMaxRetries = getEnvInt("SNS_PUBLISH_MAX_RETRIES", 3, &errs)

	c.AWS.Region = requireEnv("AWS_REGION", &errs)
	c.AWS.SecretsManagerCacheTTL = getEnvDuration("SECRETS_MANAGER_CACHE_TTL", 1*time.Hour, &errs)

	c.Outbox.RelayMode = getEnvDefault("OUTBOX_RELAY_MODE", "cdc")
	if c.Outbox.RelayMode != "cdc" && c.Outbox.RelayMode != "polling" {
		errs = append(errs, "OUTBOX_RELAY_MODE must be 'cdc' or 'polling'")
	}
	c.Outbox.ShardCount = 64 // Fixed by DB schema; not overridable.
	c.Outbox.WALLagAlertThresholdMB = getEnvInt64("OUTBOX_RELAY_WAL_LAG_ALERT_THRESHOLD_MB", 2000, &errs)
	c.Outbox.WALLagCriticalThresholdMB = getEnvInt64("OUTBOX_RELAY_WAL_LAG_CRITICAL_THRESHOLD_MB", 5000, &errs)
	c.Outbox.SlotSizeAlertMB = getEnvInt64("OUTBOX_RELAY_SLOT_SIZE_ALERT_MB", 8000, &errs)
	c.Outbox.ConsumerHeartbeatTimeoutSec = getEnvInt("OUTBOX_RELAY_CONSUMER_HEARTBEAT_TIMEOUT_SEC", 60, &errs)
	c.Outbox.ReconnectIntervalSec = getEnvInt("OUTBOX_RELAY_RECONNECT_INTERVAL_SEC", 60, &errs)
	c.Outbox.PollIntervalSec = getEnvInt("OUTBOX_RELAY_POLL_INTERVAL_SEC", 10, &errs)
	c.Outbox.MaxAttempts = getEnvInt("OUTBOX_RELAY_MAX_ATTEMPTS", 5, &errs)
	c.Outbox.BatchSize = getEnvInt("OUTBOX_RELAY_BATCH_SIZE", 50, &errs)
	c.Outbox.ClaimTTLSec = getEnvInt("OUTBOX_RELAY_CLAIM_TTL_SEC", 60, &errs)
	c.Outbox.Publisher = getEnvDefault("OUTBOX_PUBLISHER", "log")
	c.Outbox.SNSAggregateVersionAttr = getEnvBool("OUTBOX_SNS_AGGREGATE_VERSION_ATTRIBUTE", false, &errs)
	c.Outbox.WorkerIndex = getEnvInt("RELAY_WORKER_INDEX", 0, &errs)
	c.Outbox.WorkerCount = getEnvInt("RELAY_WORKER_COUNT", 1, &errs)

	c.RateLimit.FallbackMultiplier = getEnvFloat64("RATE_LIMIT_FALLBACK_MULTIPLIER", 0.5, &errs)
	c.RateLimit.LocalMaxBuckets = getEnvInt("RATE_LIMIT_LOCAL_MAX_BUCKETS", 10000, &errs)
	c.RateLimit.HealthCheckInterval = time.Duration(healthCheckMs) * time.Millisecond
	c.RateLimit.Capacity = getEnvInt64("RATE_LIMIT_CAPACITY", 100, &errs)
	c.RateLimit.RefillPerSec = getEnvFloat64("RATE_LIMIT_REFILL_PER_SEC", 50, &errs)
	c.RateLimit.AuthFailureCapacity = getEnvInt64("AUTH_FAILURE_LIMIT_CAPACITY", 10, &errs)
	c.RateLimit.AuthFailureRefillPerSec = getEnvFloat64("AUTH_FAILURE_REFILL_PER_SEC", 0.1, &errs)
	c.RateLimit.TrustedProxies = parseTrustedProxies(os.Getenv("TRUSTED_PROXY_CIDRS"), &errs)

	c.Routing.SnapshotTTLSeconds = getEnvInt("ROUTING_SNAPSHOT_TTL_SECONDS", 30, &errs)
	c.Routing.FeeCacheTTLSec = getEnvInt("GATEWAY_FEE_CACHE_TTL_SEC", 3600, &errs)
	c.Routing.FXReconciliationTolerancePct = getEnvFloat64("FX_RECONCILIATION_TOLERANCE_PCT", 1.0, &errs)

	c.Gateway.HTTPTimeout = getEnvDuration("GATEWAY_HTTP_TIMEOUT", 30*time.Second, &errs)
	c.Gateway.MaxAttempts = getEnvInt("GATEWAY_MAX_ATTEMPTS", 3, &errs)
	c.Gateway.CircuitBreakerFailureThreshold = getEnvInt("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5, &errs)
	c.App.ShutdownTimeout = getEnvDuration("SHUTDOWN_TIMEOUT", c.Gateway.Budget()+ShutdownTimeoutMargin, &errs)

	c.Reconciliation.RunIntervalHours = getEnvInt("RECONCILIATION_RUN_INTERVAL_HOURS", 6, &errs)
	c.Reconciliation.ReportFetchTimeoutSeconds = getEnvInt("RECONCILIATION_REPORT_FETCH_TIMEOUT_SECONDS", 120, &errs)
	c.Reconciliation.AutoResolutionEnabled = getEnvBool("RECONCILIATION_AUTO_RESOLUTION_ENABLED", true, &errs)

	c.Observability.Backend = getEnvDefault("OBSERVABILITY_BACKEND", "stdout")
	c.Observability.LogLevel = getEnvDefault("LOG_LEVEL", "info")
	c.Observability.OTLPEndpoint = getEnvDefault("OTLP_ENDPOINT", "")
	c.Observability.OTLPProtocol = getEnvDefault("OTLP_PROTOCOL", "http/protobuf")
	c.Observability.TailSamplingP99ThresholdMs = getEnvInt("TAIL_SAMPLING_P99_THRESHOLD_MS", 500, &errs)

	c.Security.TLSCertFile = requireEnv("TLS_CERT_FILE", &errs)
	c.Security.TLSKeyFile = requireEnv("TLS_KEY_FILE", &errs)
	c.Security.TLSCAFile = requireEnv("TLS_CA_FILE", &errs)
	c.Security.WebhookTLSCertFile = getEnvDefault("WEBHOOK_TLS_CERT_FILE", "")
	c.Security.WebhookTLSKeyFile = getEnvDefault("WEBHOOK_TLS_KEY_FILE", "")
	c.Security.CertRefreshIntervalSec = getEnvInt("TLS_CERT_REFRESH_INTERVAL_SECONDS", 3600, &errs)
	c.Security.TokenCacheTTLSec = getEnvInt("TOKEN_CACHE_TTL_SECONDS", 60, &errs)
	c.Security.DEKCacheTTLSec = getEnvInt("DEK_CACHE_TTL_SECONDS", 3600, &errs)
	c.Security.DEKRotationBatchSize = getEnvInt("DEK_ROTATION_BATCH_SIZE", 100, &errs)
	c.Security.DEKRotationBatchSleepMs = getEnvInt("DEK_ROTATION_BATCH_SLEEP_MS", 500, &errs)

	c.Jobs.LockRetryIntervalSec = getEnvInt("JOB_LOCK_RETRY_INTERVAL_SECONDS", 30, &errs)
	c.Jobs.LockTimeoutMinutes = getEnvInt("JOB_LOCK_TIMEOUT_MINUTES", 10, &errs)
	c.Jobs.LeaseExpiryIntervalSec = getEnvInt("LEASE_EXPIRY_INTERVAL_SECONDS", 60, &errs)
	c.Jobs.FeeSnapshotIntervalHours = getEnvInt("FEE_SNAPSHOT_INTERVAL_HOURS", 1, &errs)
	c.Jobs.IdempotencyProcessingTimeoutSec = getEnvInt("LEASE_REAPER_IDEMPOTENCY_TIMEOUT_SEC", 300, &errs)
	c.Jobs.RunTimeoutMinutes = getEnvInt("JOB_RUN_TIMEOUT_MINUTES", 10, &errs)
	c.Jobs.PartitionWeeksAhead = getEnvInt("PARTITION_WEEKS_AHEAD", 2, &errs)
	c.Jobs.PartitionRetentionWeeks = getEnvInt("PARTITION_RETENTION_WEEKS", 2, &errs)
	c.Jobs.PartitionDropAfterDays = getEnvInt("PARTITION_DROP_AFTER_DAYS", 14, &errs)

	c.DataRetention.BatchSize = getEnvInt("DATA_RETENTION_BATCH_SIZE", 1000, &errs)
	c.DataRetention.BatchSleepMs = getEnvInt("DATA_RETENTION_BATCH_SLEEP_MS", 100, &errs)
	c.DataRetention.AuditLogBucket = requireEnv("AUDIT_LOG_BUCKET", &errs)

	if len(errs) > 0 {
		return nil, fmt.Errorf("config: missing or invalid values:\n  - %s", strings.Join(errs, "\n  - "))
	}

	if err := Validate(c); err != nil {
		return nil, err
	}

	return c, nil
}

func Validate(c *Config) error {
	var errs []string

	validEnvs := map[string]bool{"prod": true, "staging": true, "dev": true}
	if !validEnvs[c.App.Environment] {
		errs = append(errs, "ENVIRONMENT must be one of: prod, staging, dev")
	}
	if c.App.Environment == "prod" {
		if c.App.AllowNoAuth {
			errs = append(errs, "ALLOW_NO_AUTH=true is not allowed when ENVIRONMENT=prod")
		}
		if !c.App.MtlsStrictMode {
			errs = append(errs, "MTLS_STRICT_MODE=false is not allowed when ENVIRONMENT=prod")
		}
	}

	validLogLevels := map[string]bool{
		"error": true, "warn": true, "info": true, "debug": true, "trace": true,
	}
	if !validLogLevels[c.Observability.LogLevel] {
		errs = append(errs, "LOG_LEVEL must be one of: error, warn, info, debug, trace")
	}

	validOTLPProtocols := map[string]bool{"http/protobuf": true, "grpc": true}
	if c.Observability.OTLPEndpoint != "" && !validOTLPProtocols[c.Observability.OTLPProtocol] {
		errs = append(errs, `OTLP_PROTOCOL must be "http/protobuf" or "grpc"`)
	}

	ports := map[string]int{"PORT": c.App.Port, "WEBHOOK_PORT": c.App.WebhookPort, "HEALTH_PORT": c.App.HealthPort}
	seen := make(map[int]bool, len(ports))
	for _, p := range ports {
		if p < 1 || p > 65535 {
			errs = append(errs, "PORT, WEBHOOK_PORT and HEALTH_PORT must each be between 1 and 65535")
			break
		}
		seen[p] = true
	}
	if len(seen) != len(ports) {
		errs = append(errs, "PORT, WEBHOOK_PORT and HEALTH_PORT must be distinct")
	}

	switch c.App.WebhookTLSMode {
	case WebhookTLSModeApp:
		if c.Security.WebhookTLSCertFile == "" || c.Security.WebhookTLSKeyFile == "" {
			errs = append(errs, "WEBHOOK_TLS_CERT_FILE and WEBHOOK_TLS_KEY_FILE are required when WEBHOOK_TLS_MODE=app")
		}
	case WebhookTLSModeLB:
	default:
		errs = append(errs, "WEBHOOK_TLS_MODE must be 'app' or 'lb'")
	}

	if len(c.Redis.Addrs) > 1 {
		errs = append(errs, "REDIS_ADDRS must be a single primary endpoint: multiple addresses switch the client to cluster mode, which supports only database 0 and cannot keep REDIS_RATE_LIMIT_DB and REDIS_CACHE_DB apart")
	}
	for _, addr := range c.Redis.Addrs {
		if !validHostPort(addr) {
			errs = append(errs, "REDIS_ADDRS must be in host:port form, with a non-empty host and a port between 1 and 65535")
			break
		}
	}
	if c.Redis.RateLimitDB == c.Redis.CacheDB {
		errs = append(errs, "REDIS_RATE_LIMIT_DB and REDIS_CACHE_DB must differ")
	}

	if c.Outbox.WALLagAlertThresholdMB >= c.Outbox.WALLagCriticalThresholdMB {
		errs = append(errs, "OUTBOX_RELAY_WAL_LAG_ALERT_THRESHOLD_MB must be < OUTBOX_RELAY_WAL_LAG_CRITICAL_THRESHOLD_MB")
	}

	if c.Outbox.ClaimTTLSec <= c.Outbox.PollIntervalSec {
		errs = append(errs, "OUTBOX_RELAY_CLAIM_TTL_SEC must be > OUTBOX_RELAY_POLL_INTERVAL_SEC (and must exceed max publish duration)")
	}

	switch c.Outbox.Publisher {
	case "", "log", "sns":
	default:
		errs = append(errs, "OUTBOX_PUBLISHER must be 'log' or 'sns'")
	}
	if c.Outbox.Publisher == "sns" && c.SNS.PaymentEventsTopic == "" {
		errs = append(errs, "SNS_PAYMENT_EVENTS_TOPIC is required when OUTBOX_PUBLISHER=sns")
	}

	if c.Outbox.WorkerCount < 1 {
		errs = append(errs, "RELAY_WORKER_COUNT must be >= 1")
	}
	if c.Outbox.WorkerIndex < 0 || c.Outbox.WorkerIndex >= c.Outbox.WorkerCount {
		errs = append(errs, "RELAY_WORKER_INDEX must be in [0, RELAY_WORKER_COUNT)")
	}

	if c.Gateway.MaxAttempts > 0 && c.Gateway.HTTPTimeout > 0 {
		gatewayBudget := c.Gateway.Budget()
		idemTimeout := time.Duration(c.Jobs.IdempotencyProcessingTimeoutSec) * time.Second
		if idemTimeout <= gatewayBudget {
			errs = append(errs, fmt.Sprintf(
				"LEASE_REAPER_IDEMPOTENCY_TIMEOUT_SEC (%s) must exceed the gateway budget GATEWAY_HTTP_TIMEOUT*GATEWAY_MAX_ATTEMPTS (%s), or the reaper can release a still-in-flight reservation and cause double-execution",
				idemTimeout, gatewayBudget,
			))
		}
	}

	if c.RateLimit.FallbackMultiplier <= 0 || c.RateLimit.FallbackMultiplier > 1 {
		errs = append(errs, "RATE_LIMIT_FALLBACK_MULTIPLIER must be in (0, 1]")
	}
	if c.RateLimit.Capacity < 1 {
		errs = append(errs, "RATE_LIMIT_CAPACITY must be at least 1")
	}
	if c.RateLimit.RefillPerSec <= 0 {
		errs = append(errs, "RATE_LIMIT_REFILL_PER_SEC must be greater than 0")
	}
	if c.RateLimit.AuthFailureCapacity < 0 {
		errs = append(errs, "AUTH_FAILURE_LIMIT_CAPACITY must not be negative (0 disables auth-failure throttling)")
	}
	if c.RateLimit.AuthFailureCapacity > 0 && c.RateLimit.AuthFailureRefillPerSec <= 0 {
		errs = append(errs, "AUTH_FAILURE_REFILL_PER_SEC must be greater than 0")
	}
	if c.Gateway.CircuitBreakerFailureThreshold < 1 {
		errs = append(errs, "CIRCUIT_BREAKER_FAILURE_THRESHOLD must be at least 1")
	}
	if c.App.ShutdownDrainDelay < 0 {
		errs = append(errs, "SHUTDOWN_DRAIN_DELAY must not be negative")
	}
	if c.App.ShutdownTimeout <= 0 {
		errs = append(errs, "SHUTDOWN_TIMEOUT must be greater than 0")
	}
	if budget := c.Gateway.Budget(); budget > 0 && c.App.ShutdownTimeout < budget {
		errs = append(errs, fmt.Sprintf(
			"SHUTDOWN_TIMEOUT (%s) must be at least the gateway budget GATEWAY_HTTP_TIMEOUT*GATEWAY_MAX_ATTEMPTS (%s), or a deploy can cut off a charge in flight",
			c.App.ShutdownTimeout, budget,
		))
	}

	if c.Routing.FXReconciliationTolerancePct < 0 || c.Routing.FXReconciliationTolerancePct > 100 {
		errs = append(errs, "FX_RECONCILIATION_TOLERANCE_PCT must be between 0 and 100")
	}

	if len(errs) > 0 {
		return errors.New("config validation failed:\n  - " + strings.Join(errs, "\n  - "))
	}
	return nil
}

func validHostPort(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	n, err := strconv.ParseUint(port, 10, 16)
	return err == nil && n > 0
}

func splitList(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func parseServiceTokens(raw string, errs *[]string) map[string]string {
	out := map[string]string{}
	for i, entry := range splitList(raw) {
		sep := strings.LastIndex(entry, "=")
		if sep <= 0 || sep == len(entry)-1 {
			*errs = append(*errs, fmt.Sprintf("SERVICE_TOKENS entry %d must be token=merchantID", i+1))
			continue
		}
		token := entry[:sep]
		merchant, err := uuid.Parse(entry[sep+1:])
		if err != nil {
			*errs = append(*errs, fmt.Sprintf("SERVICE_TOKENS entry %d: merchant ID must be a UUID", i+1))
			continue
		}
		if len(token) < MinTokenLength {
			*errs = append(*errs, fmt.Sprintf("SERVICE_TOKENS entry %d: token must be at least %d characters", i+1, MinTokenLength))
			continue
		}
		if _, dup := out[token]; dup {
			*errs = append(*errs, fmt.Sprintf("SERVICE_TOKENS entry %d repeats an earlier token", i+1))
			continue
		}
		out[token] = merchant.String()
	}
	return out
}

func parseOpsTokens(raw string, errs *[]string) []string {
	var out []string
	seen := map[string]bool{}
	for i, token := range splitList(raw) {
		if len(token) < MinTokenLength {
			*errs = append(*errs, fmt.Sprintf("OPS_TOKENS entry %d: token must be at least %d characters", i+1, MinTokenLength))
			continue
		}
		if seen[token] {
			*errs = append(*errs, fmt.Sprintf("OPS_TOKENS entry %d repeats an earlier token", i+1))
			continue
		}
		seen[token] = true
		out = append(out, token)
	}
	return out
}

func parseWebhookSecrets(raw string, errs *[]string) map[string]string {
	out := map[string]string{}
	for i, entry := range splitList(raw) {
		sep := strings.Index(entry, "=")
		if sep <= 0 || sep == len(entry)-1 {
			*errs = append(*errs, fmt.Sprintf("WEBHOOK_SECRETS entry %d must be gatewayID=secret", i+1))
			continue
		}
		gatewayID := entry[:sep]
		if _, dup := out[gatewayID]; dup {
			*errs = append(*errs, fmt.Sprintf("WEBHOOK_SECRETS entry %d repeats gateway %q", i+1, gatewayID))
			continue
		}
		out[gatewayID] = entry[sep+1:]
	}
	return out
}

func parseTrustedProxies(raw string, errs *[]string) []netip.Prefix {
	var out []netip.Prefix
	for i, entry := range splitList(raw) {
		p, err := netip.ParsePrefix(entry)
		if err != nil {
			*errs = append(*errs, fmt.Sprintf("TRUSTED_PROXY_CIDRS entry %d must be a CIDR such as 10.1.0.0/16", i+1))
			continue
		}
		out = append(out, p.Masked())
	}
	return out
}

func quoteDSNValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}

func (d DatabaseConfig) DSN() string {
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		quoteDSNValue(d.PrimaryHost), d.Port, quoteDSNValue(d.Name),
		quoteDSNValue(d.User), quoteDSNValue(d.Password), quoteDSNValue(d.SSLMode),
	)
	if d.SearchPath != "" {
		dsn += " search_path=" + quoteDSNValue(d.SearchPath)
	}
	return dsn
}
func (d DatabaseConfig) ReplicaDSN() string {
	if d.ReplicaHost == "" {
		return d.DSN()
	}
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		quoteDSNValue(d.ReplicaHost), d.Port, quoteDSNValue(d.Name),
		quoteDSNValue(d.User), quoteDSNValue(d.Password), quoteDSNValue(d.SSLMode),
	)
	if d.SearchPath != "" {
		dsn += " search_path=" + quoteDSNValue(d.SearchPath)
	}
	return dsn
}
