package middleware

import (
	"context"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"samarth/payment-service/internal/ports"
)

const (
	NamespaceUser        = "user"
	NamespaceMerchant    = "merchant"
	NamespaceIP          = "ip"
	NamespaceWebhookIP   = "webhook_ip"
	NamespaceAuthFailure = "auth_failure"
)

const unauthenticatedScope = "unauthenticated"

type Bucket struct {
	Namespace string
	ID        string
}

type RateDecision struct {
	Allowed    bool
	RetryAfter time.Duration
}

type Limiter interface {
	Take(ctx context.Context, buckets []Bucket, capacity int64, ratePerSec float64, cost int64) RateDecision
}

type RateLimitConfig struct {
	Capacity     int64
	RefillPerSec float64
}

const ipv6BucketPrefix = 64

type RateLimitKeys func(r *http.Request) []Bucket

func RequestKeys(clientIP *ClientIP) RateLimitKeys {
	return func(r *http.Request) []Bucket {
		var buckets []Bucket
		merchant := merchantBucketID(r.Context())
		if merchant != "" {
			buckets = append(buckets, Bucket{Namespace: NamespaceMerchant, ID: merchant})
		}
		if user := strings.TrimSpace(r.Header.Get("X-User-ID")); user != "" {
			scope := merchant
			if scope == "" {
				scope = unauthenticatedScope
			}
			buckets = append(buckets, Bucket{Namespace: NamespaceUser, ID: scope + "\x00" + user})
		}
		if ip := bucketIP(clientIP.From(r)); ip != "" {
			buckets = append(buckets, Bucket{Namespace: NamespaceIP, ID: ip})
		}
		return buckets
	}
}

func WebhookKeys(clientIP *ClientIP) RateLimitKeys {
	return func(r *http.Request) []Bucket {
		ip := bucketIP(clientIP.From(r))
		if ip == "" {
			return nil
		}
		return []Bucket{{Namespace: NamespaceWebhookIP, ID: ip}}
	}
}

func bucketIP(ip string) string {
	addr, err := netip.ParseAddr(ip)
	if err != nil || !addr.Is6() || addr.Is4In6() {
		return ip
	}
	return netip.PrefixFrom(addr, ipv6BucketPrefix).Masked().String()
}

func RateLimit(limiter Limiter, cfg RateLimitConfig, log ports.Logger, keys RateLimitKeys) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			buckets := keys(r)

			decision := limiter.Take(r.Context(), buckets, cfg.Capacity, cfg.RefillPerSec, 1)
			if !decision.Allowed {
				log.Warn(ports.LogEventRateLimitRejected, map[string]any{
					"remote_addr": r.RemoteAddr,
					"path":        r.URL.Path,
					"buckets":     namespaces(buckets),
				})
				writeRateLimited(w, decision)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func writeRateLimited(w http.ResponseWriter, decision RateDecision) {
	retrySec := int(decision.RetryAfter.Seconds())
	if retrySec < 1 {
		retrySec = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(retrySec))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"too many requests"}}`))
}

func namespaces(buckets []Bucket) []string {
	out := make([]string, len(buckets))
	for i, b := range buckets {
		out[i] = b.Namespace
	}
	return out
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
