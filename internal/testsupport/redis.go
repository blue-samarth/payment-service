package testsupport

import (
	"context"
	"sync"
	"testing"
	"time"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/redis"
)

var (
	redisOnce   sync.Once
	sharedRedis *redis.Client
	redisErr    error
)

func RequireRedis(t *testing.T) *redis.Client {
	t.Helper()
	redisOnce.Do(func() { sharedRedis, redisErr = setupRedis() })
	if redisErr != nil {
		t.Skipf("integration redis unavailable: %v\n  start it with: docker compose -f deploy/docker/docker-compose.test.yml up -d", redisErr)
	}
	return sharedRedis
}

func FlushRedis(t *testing.T, c *redis.Client) {
	t.Helper()
	ctx := context.Background()
	if err := c.RateLimit.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("testsupport: flush rate-limit db: %v", err)
	}
	if err := c.Cache.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("testsupport: flush cache db: %v", err)
	}
}

func setupRedis() (*redis.Client, error) {
	c, err := redis.New(redisConfigFromEnv())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func redisConfigFromEnv() config.RedisConfig {
	return config.RedisConfig{
		Addrs:               []string{getEnv("PAYMENT_TEST_REDIS_ADDR", "localhost:6379")},
		RateLimitDB:         0,
		CacheDB:             1,
		DialTimeout:         2 * time.Second,
		ReadTimeout:         2 * time.Second,
		WriteTimeout:        2 * time.Second,
		HealthCheckInterval: time.Second,
	}
}
