package redis

import (
	"context"
	"crypto/tls"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"samarth/payment-service/config"
)

type Client struct {
	RateLimit goredis.UniversalClient
	State     goredis.UniversalClient
}

func New(cfg config.RedisConfig) (*Client, error) {
	base := &goredis.UniversalOptions{
		Addrs:        cfg.Addrs,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
	if cfg.TLSEnabled {
		base.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	rl := *base
	rl.DB = cfg.RateLimitDB
	rl.TLSConfig = base.TLSConfig.Clone()
	st := *base
	st.DB = cfg.StateDB
	st.TLSConfig = base.TLSConfig.Clone()

	return &Client{
		RateLimit: goredis.NewUniversalClient(&rl),
		State:     goredis.NewUniversalClient(&st),
	}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.RateLimit.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: rate_limit ping: %w", err)
	}
	if err := c.State.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: state ping: %w", err)
	}
	return nil
}

func (c *Client) Close() error {
	e1 := c.RateLimit.Close()
	e2 := c.State.Close()
	if e1 != nil {
		return e1
	}
	return e2
}
