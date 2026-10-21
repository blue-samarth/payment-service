package redis_test

import . "samarth/payment-service/internal/adapters/redis"

import (
	"crypto/tls"
	"testing"

	goredis "github.com/redis/go-redis/v9"

	"samarth/payment-service/config"
)

func singleNodeOptions(t *testing.T, c goredis.UniversalClient) *goredis.Options {
	t.Helper()
	simple, ok := c.(*goredis.Client)
	if !ok {
		t.Fatalf("expected a single-node client for one address, got %T", c)
	}
	return simple.Options()
}

func newClient(t *testing.T, cfg config.RedisConfig) *Client {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestNew_TLSEnabledConfiguresBothClients(t *testing.T) {
	c := newClient(t, config.RedisConfig{
		Addrs:       []string{"redis.internal:6379"},
		RateLimitDB: 0,
		StateDB:     1,
		TLSEnabled:  true,
		Username:    "payment-service",
		Password:    "test-password",
	})

	clients := map[string]struct {
		client goredis.UniversalClient
		db     int
	}{
		"rate_limit": {c.RateLimit, 0},
		"state":      {c.State, 1},
	}
	for name, tc := range clients {
		opts := singleNodeOptions(t, tc.client)
		if opts.TLSConfig == nil {
			t.Fatalf("%s: TLS enabled but the client has no TLS config", name)
		}
		if opts.TLSConfig.MinVersion != tls.VersionTLS12 {
			t.Errorf("%s: TLS minimum version = %x, want TLS 1.2", name, opts.TLSConfig.MinVersion)
		}
		if opts.TLSConfig.InsecureSkipVerify {
			t.Errorf("%s: certificate verification must not be skipped", name)
		}
		if opts.Username != "payment-service" || opts.Password != "test-password" {
			t.Errorf("%s: credentials not passed to the client", name)
		}
		if opts.DB != tc.db {
			t.Errorf("%s: DB = %d, want %d", name, opts.DB, tc.db)
		}
	}
}

func TestNew_ClientsDoNotShareTLSConfig(t *testing.T) {
	c := newClient(t, config.RedisConfig{Addrs: []string{"redis.internal:6379"}, StateDB: 1, TLSEnabled: true})

	rl := singleNodeOptions(t, c.RateLimit).TLSConfig
	st := singleNodeOptions(t, c.State).TLSConfig
	if rl == st {
		t.Error("each client should own its TLS config so one cannot mutate the other's")
	}
}

func TestNew_TLSDisabledIsPlaintext(t *testing.T) {
	c := newClient(t, config.RedisConfig{Addrs: []string{"localhost:6379"}, StateDB: 1})

	for name, client := range map[string]goredis.UniversalClient{"rate_limit": c.RateLimit, "state": c.State} {
		if singleNodeOptions(t, client).TLSConfig != nil {
			t.Errorf("%s: TLS disabled but the client has a TLS config", name)
		}
	}
}
