package redis

import (
	"container/list"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/ports"
)

const (
	defaultHealthCheckInterval   = 5 * time.Second
	defaultRedisFailureThreshold = 3
)

type RateLimiter struct {
	client              goredis.UniversalClient
	cfg                 config.RateLimitConfig
	available           bool
	mu                  sync.RWMutex
	localBuckets        sync.Map // map[string]*lruBucketEntry for lock-free reads
	lruOrder            *list.List
	lruMu               sync.Mutex
	lruCount            int
	logger              ports.Logger
	metrics             ports.MetricRecorder
	fallbackStartedAt   time.Time
	failureThreshold    int
	consecutiveFailures int32
	stopHealth          chan struct{}
	stopOnce            sync.Once
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
		client:           c.RateLimit,
		cfg:              cfg,
		available:        true,
		lruOrder:         list.New(),
		logger:           logger,
		metrics:          metrics,
		failureThreshold: defaultRedisFailureThreshold,
		stopHealth:       make(chan struct{}),
	}

	_ = tokenBucketScript.Load(context.Background(), c.RateLimit).Err()

	interval := cfg.HealthCheckInterval
	if interval <= 0 {
		interval = defaultHealthCheckInterval
	}
	go r.runHealthCheck(interval)

	return r
}

func (r *RateLimiter) Close() {
	r.stopOnce.Do(func() { close(r.stopHealth) })
}

func (r *RateLimiter) runHealthCheck(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopHealth:
			return
		case <-ticker.C:
			if err := r.client.Ping(context.Background()).Err(); err == nil {
				_ = tokenBucketScript.Load(context.Background(), r.client).Err()
				r.MarkAvailable()
			} else {
				r.markUnavailable()
			}
		}
	}
}

func bucketKey(namespace, entityID string) string {
	h := sha256.Sum256([]byte(entityID))
	return fmt.Sprintf("rate_limit:rl:%s:%x", namespace, h[:8])
}

const minRefillRate = 1e-9

func (r *RateLimiter) Allow(ctx context.Context, userID, merchantID, ip string, capacity int64, ratePerSec float64) RateLimitResult {
	if ratePerSec <= 0 {
		ratePerSec = minRefillRate
	}

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

	keys := []string{
		bucketKey("user", userID),
		bucketKey("merchant", merchantID),
		bucketKey("ip", ip),
	}

	res, err := tokenBucketScript.Run(ctx, r.client, keys, capacity, ratePerSec, nowMs, 1).Int64Slice()
	if err != nil || len(res) != 2 {
		r.recordRedisFailure()
		return r.allowLocal(userID, merchantID, ip, capacity, ratePerSec)
	}
	r.recordRedisSuccess()

	if res[0] == 0 {
		return RateLimitResult{Allowed: false, RetryAfter: time.Duration(res[1]) * time.Millisecond}
	}
	return RateLimitResult{Allowed: true}
}

func (r *RateLimiter) recordRedisFailure() {
	if int(atomic.AddInt32(&r.consecutiveFailures, 1)) >= r.failureThreshold {
		r.markUnavailable()
	}
}

func (r *RateLimiter) recordRedisSuccess() {
	atomic.StoreInt32(&r.consecutiveFailures, 0)
}

func (r *RateLimiter) allowLocal(userID, merchantID, ip string, capacity int64, ratePerSec float64) RateLimitResult {
	localCap := float64(capacity) * r.cfg.FallbackMultiplier

	ids := []string{userID, merchantID, ip}
	buckets := make([]*localBucket, len(ids))
	for i, id := range ids {
		buckets[i] = r.getOrCreateBucket(id, localCap, ratePerSec)
	}

	allowed := true
	var maxWait time.Duration
	now := time.Now()
	for _, b := range buckets {
		b.mu.Lock()
		b.capacity = localCap
		b.refillRate = ratePerSec
		elapsed := now.Sub(b.lastRefill).Seconds()
		b.tokens = min(b.capacity, b.tokens+elapsed*b.refillRate)
		b.lastRefill = now
		if b.tokens < 1 {
			allowed = false
			if b.refillRate > 0 {
				if wait := time.Duration((1-b.tokens)/b.refillRate*1000) * time.Millisecond; wait > maxWait {
					maxWait = wait
				}
			}
		}
		b.mu.Unlock()
	}

	if !allowed {
		return RateLimitResult{Allowed: false, RetryAfter: maxWait}
	}

	for _, b := range buckets {
		b.mu.Lock()
		if b.tokens >= 1 {
			b.tokens--
		}
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

	atomic.StoreInt32(&r.consecutiveFailures, 0)
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
