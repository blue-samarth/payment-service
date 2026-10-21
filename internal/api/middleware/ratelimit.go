package middleware

import (
	"context"
	"errors"
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

const (
	unauthenticatedScope = "unauthenticated"
	unknownClient        = "unknown"
	MaxUserIDLength      = 128
)

var errUserIDTooLong = errors.New("X-User-ID must be at most 128 characters")

type Limit struct {
	Capacity     int64
	RefillPerSec float64
}

func (l Limit) canBindBelow(outer Limit) bool {
	return l.Capacity < outer.Capacity || l.RefillPerSec < outer.RefillPerSec
}

type Limits struct {
	Merchant Limit
	User     Limit
	IP       Limit
}

type Bucket struct {
	Namespace string
	ID        string
	Limit
}

type RateDecision struct {
	Allowed    bool
	RetryAfter time.Duration
}

type Limiter interface {
	Take(ctx context.Context, buckets []Bucket, cost int64) RateDecision
}

const ipv6BucketPrefix = 64

type RateLimitKeys func(r *http.Request) ([]Bucket, error)

func RequestKeys(clientIP *ClientIP, limits Limits) RateLimitKeys {
	return func(r *http.Request) ([]Bucket, error) {
		var buckets []Bucket
		merchant := merchantBucketID(r.Context())
		if merchant != "" {
			buckets = append(buckets, Bucket{Namespace: NamespaceMerchant, ID: merchant, Limit: limits.Merchant})
		}
		if user := strings.TrimSpace(r.Header.Get("X-User-ID")); user != "" {
			if len(user) > MaxUserIDLength {
				return nil, errUserIDTooLong
			}
			if merchant == "" || limits.User.canBindBelow(limits.Merchant) {
				scope := merchant
				if scope == "" {
					scope = unauthenticatedScope
				}
				buckets = append(buckets, Bucket{Namespace: NamespaceUser, ID: scope + "\x00" + user, Limit: limits.User})
			}
		}
		buckets = append(buckets, Bucket{Namespace: NamespaceIP, ID: clientBucketIP(clientIP, r), Limit: limits.IP})
		return buckets, nil
	}
}

func WebhookKeys(clientIP *ClientIP, limit Limit) RateLimitKeys {
	return func(r *http.Request) ([]Bucket, error) {
		return []Bucket{{Namespace: NamespaceWebhookIP, ID: clientBucketIP(clientIP, r), Limit: limit}}, nil
	}
}

func clientBucketIP(clientIP *ClientIP, r *http.Request) string {
	if ip := bucketIP(clientIP.From(r)); ip != "" {
		return ip
	}
	return unknownClient
}

func bucketIP(ip string) string {
	addr, err := netip.ParseAddr(ip)
	if err != nil || !addr.Is6() || addr.Is4In6() {
		return ip
	}
	return netip.PrefixFrom(addr, ipv6BucketPrefix).Masked().String()
}

func RateLimit(limiter Limiter, log ports.Logger, metrics ports.MetricRecorder, keys RateLimitKeys) func(http.Handler) http.Handler {
	rejectedLog := newLogSampler(logSampleInterval)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			buckets, err := keys(r)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":{"code":"invalid_user_id","message":"` + err.Error() + `"}}`))
				return
			}

			decision := limiter.Take(r.Context(), buckets, 1)
			if !decision.Allowed {
				if metrics != nil {
					metrics.Increment(ports.MetricRateLimitRejected, map[string]string{"listener": listenerOf(buckets)})
				}
				if emit, suppressed := rejectedLog.allow(); emit {
					log.Warn(ports.LogEventRateLimitRejected, map[string]any{
						"client_ip":   bucketClientIP(buckets),
						"remote_addr": r.RemoteAddr,
						"path":        r.URL.Path,
						"buckets":     namespaces(buckets),
						"suppressed":  suppressed,
					})
				}
				writeRateLimited(w, decision)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func listenerOf(buckets []Bucket) string {
	for _, b := range buckets {
		if b.Namespace == NamespaceWebhookIP {
			return "webhook"
		}
	}
	return "merchant"
}

func bucketClientIP(buckets []Bucket) string {
	for _, b := range buckets {
		if b.Namespace == NamespaceIP || b.Namespace == NamespaceWebhookIP {
			return b.ID
		}
	}
	return ""
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
