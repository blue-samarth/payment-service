//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/redis"
	"samarth/payment-service/internal/testsupport"
)

func TestNew_TLSClientDoesNotDowngradeToPlaintext(t *testing.T) {
	testsupport.RequireRedis(t)

	cfg := config.RedisConfig{
		Addrs:        []string{testsupport.RedisAddr()},
		RateLimitDB:  0,
		CacheDB:      1,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	}

	plain, err := redis.New(cfg)
	if err != nil {
		t.Fatalf("New (plaintext): %v", err)
	}
	defer plain.Close()

	cfg.TLSEnabled = true
	secure, err := redis.New(cfg)
	if err != nil {
		t.Fatalf("New (TLS): %v", err)
	}
	defer secure.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := plain.Ping(ctx); err != nil {
		t.Fatalf("control: the plaintext client must reach the compose Redis, got %v", err)
	}
	if err := secure.Ping(ctx); err == nil {
		t.Fatal("the TLS client reached a plaintext-only Redis, so it downgraded instead of refusing")
	}
}
