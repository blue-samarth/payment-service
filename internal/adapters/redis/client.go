package redis

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"samarth/payment-service/config"
)

type Client struct {
	RateLimit goredis.UniversalClient
	Cache     goredis.UniversalClient
}

func New(cfg config.RedisConfig) (*Client, error) {
	base := &goredis.UniversalOptions{
		Addrs:        cfg.Addrs,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	rl := *base
	rl.DB = cfg.RateLimitDB
	ca := *base
	ca.DB = cfg.CacheDB

	return &Client{
		RateLimit: goredis.NewUniversalClient(&rl),
		Cache:     goredis.NewUniversalClient(&ca),
	}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.RateLimit.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: rate_limit ping: %w", err)
	}
	if err := c.Cache.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: cache ping: %w", err)
	}
	return nil
}

func (c *Client) Close() error {
	e1 := c.RateLimit.Close()
	e2 := c.Cache.Close()
	if e1 != nil {
		return e1
	}
	return e2
}
