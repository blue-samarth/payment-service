package middleware

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"samarth/payment-service/internal/ports"
)

type RateDecision struct {
	Allowed    bool
	RetryAfter time.Duration
}

type Limiter interface {
	Allow(ctx context.Context, userID, merchantID, ip string, capacity int64, ratePerSec float64) RateDecision
}

type RateLimitConfig struct {
	Capacity     int64
	RefillPerSec float64
}

type RateLimitKeys func(r *http.Request) (userID, merchantID, ip string)

func RequestKeys(clientIP *ClientIP) RateLimitKeys {
	return func(r *http.Request) (string, string, string) {
		return r.Header.Get("X-User-ID"), merchantBucketID(r.Context()), clientIP.From(r)
	}
}

func WebhookKeys(clientIP *ClientIP) RateLimitKeys {
	return func(r *http.Request) (string, string, string) {
		ip := clientIP.From(r)
		return "webhook:" + ip, "webhook:" + ip, ip
	}
}

func RateLimit(limiter Limiter, cfg RateLimitConfig, log ports.Logger, keys RateLimitKeys) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userID, merchantID, ip := keys(r)

			decision := limiter.Allow(r.Context(), userID, merchantID, ip, cfg.Capacity, cfg.RefillPerSec)
			if !decision.Allowed {
				retrySec := int(decision.RetryAfter.Seconds())
				if retrySec < 1 {
					retrySec = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(retrySec))
				log.Warn(ports.LogEventRateLimitRejected, map[string]any{
					"ip":   ip,
					"path": r.URL.Path,
				})
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"too many requests"}}`))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func merchantBucketID(ctx context.Context) string {
	if m := MerchantIDFromContext(ctx); m != "" {
		return m
	}
	if p, ok := PrincipalFromContext(ctx); ok {
		return "principal:" + string(p.Role)
	}
	return ""
}
