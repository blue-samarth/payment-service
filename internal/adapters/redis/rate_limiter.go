package redis

import (
	"container/list"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/ports"
)

type RateLimiter struct {
	client            goredis.UniversalClient
	cfg               config.RateLimitConfig
	available         bool
	mu                sync.RWMutex
	localBuckets      sync.Map // map[string]*lruBucketEntry for lock-free reads
	lruOrder          *list.List
	lruMu             sync.Mutex
	lruCount          int
	logger            ports.Logger
	metrics           ports.MetricRecorder
	fallbackStartedAt time.Time
}

type lruBucketEntry struct {
	bucket  *localBucket
	lruNode *list.Element
}

type localBucket struct {
	mu         sync.Mutex
	tokens     float64
	capacity   float64
	lastRefill time.Time
	refillRate float64
}

type RateLimitResult struct {
	Allowed    bool
	RetryAfter time.Duration
}

func NewRateLimiter(c *Client, cfg config.RateLimitConfig, logger ports.Logger, metrics ports.MetricRecorder) *RateLimiter {
	r := &RateLimiter{
		client:    c.RateLimit,
		cfg:       cfg,
		available: true,
		lruOrder:  list.New(),
		logger:    logger,
		metrics:   metrics,
	}

	_ = tokenBucketScript.Load(context.Background(), c.RateLimit).Err()

	// Start background health check with configurable interval
	go func() {
		ticker := time.NewTicker(cfg.HealthCheckInterval)
		defer ticker.Stop()

		for range ticker.C {
			if err := c.RateLimit.Ping(context.Background()).Err(); err == nil {
				_ = tokenBucketScript.Load(context.Background(), c.RateLimit).Err()
				r.MarkAvailable()
			} else {
				r.markUnavailable()
			}
		}
	}()

	return r
}

func bucketKeys(namespace, entityID string) (string, string) {
	h := sha256.Sum256([]byte(entityID))
	primary := fmt.Sprintf("rate_limit:rl:%s:%x", namespace, h[:8])
	legacy := fmt.Sprintf("rl:%s:%x", namespace, h[:4])
	return primary, legacy
}

func (r *RateLimiter) Allow(ctx context.Context, userID, merchantID, ip string, capacity int64, ratePerSec float64) RateLimitResult {
	r.mu.RLock()
	avail := r.available
	r.mu.RUnlock()

	if avail {
		return r.allowRedis(ctx, userID, merchantID, ip, capacity, ratePerSec)
	}
	return r.allowLocal(userID, merchantID, ip, capacity, ratePerSec)
}

func (r *RateLimiter) allowRedis(ctx context.Context, userID, merchantID, ip string, capacity int64, ratePerSec float64) RateLimitResult {
	nowMs := time.Now().UnixMilli()
	userPrimary, userLegacy := bucketKeys("user", userID)
	merchantPrimary, merchantLegacy := bucketKeys("merchant", merchantID)
	ipPrimary, ipLegacy := bucketKeys("ip", ip)
	keys := [][2]string{
		{userPrimary, userLegacy},
		{merchantPrimary, merchantLegacy},
		{ipPrimary, ipLegacy},
	}

	pipe := r.client.Pipeline()

	cmds := make([]*goredis.Cmd, 0, len(keys))
	for _, keyPair := range keys {
		cmd := tokenBucketScript.Run(ctx, pipe, []string{keyPair[0], keyPair[1]},
			capacity, ratePerSec, nowMs, 1,
		)
		cmds = append(cmds, cmd)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		r.markUnavailable()
		return r.allowLocal(userID, merchantID, ip, capacity, ratePerSec)
	}

	var maxWait int64
	for _, cmd := range cmds {
		res, err := cmd.Int64Slice()
		if err != nil {
			r.markUnavailable()
			return r.allowLocal(userID, merchantID, ip, capacity, ratePerSec)
		}
		if res[0] == 0 {
			if res[1] > maxWait {
				maxWait = res[1]
			}
		}
	}

	if maxWait > 0 {
		return RateLimitResult{Allowed: false, RetryAfter: time.Duration(maxWait) * time.Millisecond}
	}
	return RateLimitResult{Allowed: true}
}

func (r *RateLimiter) allowLocal(userID, merchantID, ip string, capacity int64, ratePerSec float64) RateLimitResult {
	localCap := float64(capacity) * r.cfg.FallbackMultiplier

	for _, id := range []string{userID, merchantID, ip} {
		b := r.getOrCreateBucket(id, localCap, ratePerSec)
		b.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(b.lastRefill).Seconds()
		b.tokens = min(b.capacity, b.tokens+elapsed*b.refillRate)
		b.lastRefill = now
		if b.tokens < 1 {
			wait := time.Duration((1-b.tokens)/b.refillRate*1000) * time.Millisecond
			b.mu.Unlock()
			return RateLimitResult{Allowed: false, RetryAfter: wait}
		}
		b.tokens--
		b.mu.Unlock()
	}
	return RateLimitResult{Allowed: true}
}

func (r *RateLimiter) getOrCreateBucket(id string, capacity, rate float64) *localBucket {
	// Try to load from sync.Map (lock-free read)
	if _, ok := r.localBuckets.Load(id); ok {
		r.lruMu.Lock()
		if cur, ok := r.localBuckets.Load(id); ok {
			entry := cur.(*lruBucketEntry)
			r.lruOrder.MoveToFront(entry.lruNode)
			r.lruMu.Unlock()
			return entry.bucket
		}
		r.lruMu.Unlock()
	}

	// Bucket doesn't exist, create new one
	r.lruMu.Lock()
	defer r.lruMu.Unlock()

	// Double-check after acquiring lock
	if val, ok := r.localBuckets.Load(id); ok {
		entry := val.(*lruBucketEntry)
		r.lruOrder.MoveToFront(entry.lruNode)
		return entry.bucket
	}

	// Evict oldest if at capacity
	if r.lruCount >= r.cfg.LocalMaxBuckets {
		r.evictOldestLocked()
	}

	b := &localBucket{
		tokens:     capacity,
		capacity:   capacity,
		lastRefill: time.Now(),
		refillRate: rate,
	}

	// Add to front of LRU list (most recently used)
	node := r.lruOrder.PushFront(id)
	entry := &lruBucketEntry{
		bucket:  b,
		lruNode: node,
	}

	r.localBuckets.Store(id, entry)
	r.lruCount++
	return b
}

func (r *RateLimiter) evictOldestLocked() {
	// Remove least recently used (back of list)
	if oldest := r.lruOrder.Back(); oldest != nil {
		id := oldest.Value.(string)
		r.localBuckets.Delete(id)
		r.lruOrder.Remove(oldest)
		r.lruCount--
	}
}

func (r *RateLimiter) markUnavailable() {
	r.mu.Lock()
	if !r.available {
		r.mu.Unlock()
		return
	}
	r.available = false
	r.fallbackStartedAt = time.Now().UTC()
	r.mu.Unlock()

	r.recordFallbackActivated()
}

func (r *RateLimiter) MarkAvailable() {
	var startedAt time.Time
	r.mu.Lock()
	if r.available {
		r.mu.Unlock()
		return
	}
	r.available = true
	startedAt = r.fallbackStartedAt
	r.fallbackStartedAt = time.Time{}
	r.mu.Unlock()

	r.recordFallbackRestored(startedAt)
}

func (r *RateLimiter) recordFallbackActivated() {
	if r.logger != nil {
		r.logger.Warn(ports.LogEventRateLimitFallback, map[string]any{
			"redis_available": false,
		})
	}
	if r.metrics != nil {
		r.metrics.Gauge(ports.MetricRateLimitRedisAvailable, 0, nil)
		r.metrics.Increment(ports.MetricRateLimitFallbackActivationsTotal, nil)
	}
}

func (r *RateLimiter) recordFallbackRestored(startedAt time.Time) {
	durationSec := 0.0
	if !startedAt.IsZero() {
		durationSec = time.Since(startedAt).Seconds()
	}

	if r.logger != nil {
		fields := map[string]any{
			"redis_available": true,
		}
		if durationSec > 0 {
			fields["fallback_duration_sec"] = durationSec
		}
		r.logger.Info(ports.LogEventRateLimitRestored, fields)
	}
	if r.metrics != nil {
		r.metrics.Gauge(ports.MetricRateLimitRedisAvailable, 1, nil)
		if durationSec > 0 {
			r.metrics.Histogram(ports.MetricRateLimitFallbackDurationSeconds, durationSec, nil)
		}
	}
}

func (r *RateLimiter) IsAvailable() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.available
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
