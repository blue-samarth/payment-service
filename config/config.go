package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
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
func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
func getEnvInt64(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}
func getEnvFloat64(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}
func getEnvBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}
func getEnvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// Config structs with fields for all configuration values, grouped by category.
type AppConfig struct {
	Environment    string
	ServiceVersion string
	Port           int
	MtlsStrictMode bool
}
type DatabaseConfig struct {
	PrimaryHost       string
	ReplicaHost       string
	Port              int
	Name              string
	User              string
	Password          string
	SSLMode           string
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
	DialTimeout         time.Duration
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	HealthCheckInterval time.Duration
}
type SNSConfig struct {
	GatewayConfigUpdatesTopic string
	DeadLetterTopic           string
	ReconciliationJobsTopic   string
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
}
type RateLimitConfig struct {
	FallbackMultiplier  float64
	LocalMaxBuckets     int
	HealthCheckInterval time.Duration
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
	CertRefreshIntervalSec  int
	TokenCacheTTLSec        int
	DEKCacheTTLSec          int
	DEKRotationBatchSize    int
	DEKRotationBatchSleepMs int
}
type JobsConfig struct {
	LockRetryIntervalSec     int
	LockTimeoutMinutes       int
	LeaseExpiryIntervalSec   int
	FeeSnapshotIntervalHours int
}
type DataRetentionConfig struct {
	BatchSize      int
	BatchSleepMs   int
	AuditLogBucket string
}
type Config struct {
	App            AppConfig
	Database       DatabaseConfig
	Redis          RedisConfig
	SNS            SNSConfig
	AWS            AWSConfig
	Outbox         OutboxConfig
	RateLimit      RateLimitConfig
	Routing        RoutingConfig
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
	c.App.Port = getEnvInt("PORT", 8080)
	c.App.MtlsStrictMode = getEnvBool("MTLS_STRICT_MODE", true)

	c.Database.PrimaryHost = requireEnv("DATABASE_PRIMARY_HOST", &errs)
	c.Database.ReplicaHost = getEnvDefault("DATABASE_REPLICA_HOST", "")
	c.Database.Port = getEnvInt("DATABASE_PORT", 5432)
	c.Database.Name = requireEnv("DATABASE_NAME", &errs)
	c.Database.User = requireEnv("DATABASE_USER", &errs)
	c.Database.Password = requireEnv("DATABASE_PASSWORD", &errs)
	c.Database.SSLMode = getEnvDefault("DATABASE_SSL_MODE", "verify-full")
	c.Database.MaxOpenConns = getEnvInt("DATABASE_MAX_OPEN_CONNS", 25)
	c.Database.MaxIdleConns = getEnvInt("DATABASE_MAX_IDLE_CONNS", 5)
	c.Database.ConnMaxLifetime = getEnvDuration("DATABASE_CONN_MAX_LIFETIME", 30*time.Minute)
	c.Database.ConnMaxIdleTime = getEnvDuration("DATABASE_CONN_MAX_IDLE_TIME", 5*time.Minute)
	c.Database.HealthCheckPeriod = getEnvDuration("DATABASE_HEALTH_CHECK_PERIOD", 30*time.Second)
	c.Database.MigrationLockKey = getEnvInt64("DATABASE_MIGRATION_LOCK_KEY", 1234567890)

	redisAddrs := getEnvDefault("REDIS_ADDRS", "")
	if redisAddrs == "" {
		errs = append(errs, "REDIS_ADDRS is required")
	} else {
		c.Redis.Addrs = strings.Split(redisAddrs, ",")
	}
	c.Redis.RateLimitDB = getEnvInt("REDIS_RATE_LIMIT_DB", 0)
	c.Redis.CacheDB = getEnvInt("REDIS_CACHE_DB", 1)
	c.Redis.DialTimeout = getEnvDuration("REDIS_DIAL_TIMEOUT", 5*time.Second)
	c.Redis.ReadTimeout = getEnvDuration("REDIS_READ_TIMEOUT", 3*time.Second)
	c.Redis.WriteTimeout = getEnvDuration("REDIS_WRITE_TIMEOUT", 3*time.Second)
	healthCheckMs := getEnvInt("RATE_LIMIT_REDIS_HEALTH_CHECK_INTERVAL_MS", 500)
	c.Redis.HealthCheckInterval = time.Duration(healthCheckMs) * time.Millisecond

	c.SNS.GatewayConfigUpdatesTopic = requireEnv("SNS_GATEWAY_CONFIG_UPDATES_TOPIC", &errs)
	c.SNS.DeadLetterTopic = requireEnv("SNS_DEAD_LETTER_TOPIC", &errs)
	c.SNS.ReconciliationJobsTopic = requireEnv("SNS_RECONCILIATION_JOBS_TOPIC", &errs)
	c.SNS.PublishMaxRetries = getEnvInt("SNS_PUBLISH_MAX_RETRIES", 3)

	c.AWS.Region = requireEnv("AWS_REGION", &errs)
	c.AWS.SecretsManagerCacheTTL = getEnvDuration("SECRETS_MANAGER_CACHE_TTL", 1*time.Hour)

	c.Outbox.RelayMode = getEnvDefault("OUTBOX_RELAY_MODE", "cdc")
	if c.Outbox.RelayMode != "cdc" && c.Outbox.RelayMode != "polling" {
		errs = append(errs, "OUTBOX_RELAY_MODE must be 'cdc' or 'polling'")
	}
	c.Outbox.ShardCount = 64 // Fixed by DB schema; not overridable.
	c.Outbox.WALLagAlertThresholdMB = getEnvInt64("OUTBOX_RELAY_WAL_LAG_ALERT_THRESHOLD_MB", 2000)
	c.Outbox.WALLagCriticalThresholdMB = getEnvInt64("OUTBOX_RELAY_WAL_LAG_CRITICAL_THRESHOLD_MB", 5000)
	c.Outbox.SlotSizeAlertMB = getEnvInt64("OUTBOX_RELAY_SLOT_SIZE_ALERT_MB", 8000)
	c.Outbox.ConsumerHeartbeatTimeoutSec = getEnvInt("OUTBOX_RELAY_CONSUMER_HEARTBEAT_TIMEOUT_SEC", 60)
	c.Outbox.ReconnectIntervalSec = getEnvInt("OUTBOX_RELAY_RECONNECT_INTERVAL_SEC", 60)
	c.Outbox.PollIntervalSec = getEnvInt("OUTBOX_RELAY_POLL_INTERVAL_SEC", 10)
	c.Outbox.MaxAttempts = getEnvInt("OUTBOX_RELAY_MAX_ATTEMPTS", 5)
	c.Outbox.BatchSize = getEnvInt("OUTBOX_RELAY_BATCH_SIZE", 50)

	c.RateLimit.FallbackMultiplier = getEnvFloat64("RATE_LIMIT_FALLBACK_MULTIPLIER", 0.5)
	c.RateLimit.LocalMaxBuckets = getEnvInt("RATE_LIMIT_LOCAL_MAX_BUCKETS", 10000)
	c.RateLimit.HealthCheckInterval = time.Duration(healthCheckMs) * time.Millisecond

	c.Routing.SnapshotTTLSeconds = getEnvInt("ROUTING_SNAPSHOT_TTL_SECONDS", 30)
	c.Routing.FeeCacheTTLSec = getEnvInt("GATEWAY_FEE_CACHE_TTL_SEC", 3600)
	c.Routing.FXReconciliationTolerancePct = getEnvFloat64("FX_RECONCILIATION_TOLERANCE_PCT", 1.0)

	c.Reconciliation.RunIntervalHours = getEnvInt("RECONCILIATION_RUN_INTERVAL_HOURS", 6)
	c.Reconciliation.ReportFetchTimeoutSeconds = getEnvInt("RECONCILIATION_REPORT_FETCH_TIMEOUT_SECONDS", 120)
	c.Reconciliation.AutoResolutionEnabled = getEnvBool("RECONCILIATION_AUTO_RESOLUTION_ENABLED", true)

	c.Observability.Backend = getEnvDefault("OBSERVABILITY_BACKEND", "stdout")
	c.Observability.LogLevel = getEnvDefault("LOG_LEVEL", "info")
	c.Observability.OTLPEndpoint = getEnvDefault("OTLP_ENDPOINT", "")
	c.Observability.OTLPProtocol = getEnvDefault("OTLP_PROTOCOL", "http/protobuf")
	c.Observability.TailSamplingP99ThresholdMs = getEnvInt("TAIL_SAMPLING_P99_THRESHOLD_MS", 500)

	c.Security.TLSCertFile = requireEnv("TLS_CERT_FILE", &errs)
	c.Security.TLSKeyFile = requireEnv("TLS_KEY_FILE", &errs)
	c.Security.TLSCAFile = requireEnv("TLS_CA_FILE", &errs)
	c.Security.CertRefreshIntervalSec = getEnvInt("TLS_CERT_REFRESH_INTERVAL_SECONDS", 3600)
	c.Security.TokenCacheTTLSec = getEnvInt("TOKEN_CACHE_TTL_SECONDS", 60)
	c.Security.DEKCacheTTLSec = getEnvInt("DEK_CACHE_TTL_SECONDS", 3600)
	c.Security.DEKRotationBatchSize = getEnvInt("DEK_ROTATION_BATCH_SIZE", 100)
	c.Security.DEKRotationBatchSleepMs = getEnvInt("DEK_ROTATION_BATCH_SLEEP_MS", 500)

	c.Jobs.LockRetryIntervalSec = getEnvInt("JOB_LOCK_RETRY_INTERVAL_SECONDS", 30)
	c.Jobs.LockTimeoutMinutes = getEnvInt("JOB_LOCK_TIMEOUT_MINUTES", 10)
	c.Jobs.LeaseExpiryIntervalSec = getEnvInt("LEASE_EXPIRY_INTERVAL_SECONDS", 60)
	c.Jobs.FeeSnapshotIntervalHours = getEnvInt("FEE_SNAPSHOT_INTERVAL_HOURS", 1)

	c.DataRetention.BatchSize = getEnvInt("DATA_RETENTION_BATCH_SIZE", 1000)
	c.DataRetention.BatchSleepMs = getEnvInt("DATA_RETENTION_BATCH_SLEEP_MS", 100)
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

	if c.Outbox.WALLagAlertThresholdMB >= c.Outbox.WALLagCriticalThresholdMB {
		errs = append(errs, "OUTBOX_RELAY_WAL_LAG_ALERT_THRESHOLD_MB must be < OUTBOX_RELAY_WAL_LAG_CRITICAL_THRESHOLD_MB")
	}

	if c.RateLimit.FallbackMultiplier <= 0 || c.RateLimit.FallbackMultiplier > 1 {
		errs = append(errs, "RATE_LIMIT_FALLBACK_MULTIPLIER must be in (0, 1]")
	}

	if c.Routing.FXReconciliationTolerancePct < 0 || c.Routing.FXReconciliationTolerancePct > 100 {
		errs = append(errs, "FX_RECONCILIATION_TOLERANCE_PCT must be between 0 and 100")
	}

	if len(errs) > 0 {
		return errors.New("config validation failed:\n  - " + strings.Join(errs, "\n  - "))
	}
	return nil
}

func (d DatabaseConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		d.PrimaryHost, d.Port, d.Name, d.User, d.Password, d.SSLMode,
	)
}
func (d DatabaseConfig) ReplicaDSN() string {
	if d.ReplicaHost == "" {
		return d.DSN()
	}
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		d.ReplicaHost, d.Port, d.Name, d.User, d.Password, d.SSLMode,
	)
}
