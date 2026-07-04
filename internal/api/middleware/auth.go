package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"

	"samarth/payment-service/internal/ports"
)

type Role string

const (
	RoleService Role = "service"
	RoleOps     Role = "ops"
)

type Principal struct {
	Role Role
}

type TokenProvider interface {
	ValidHashes(ctx context.Context, role Role) (map[string]struct{}, error)
}

const principalKey contextKey = "principal"
const merchantIDKey contextKey = "merchant_id"

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey).(Principal)
	return p, ok
}

func MerchantIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(merchantIDKey).(string); ok {
		return v
	}
	return ""
}

func Authenticate(provider TokenProvider, log ports.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" || strings.HasPrefix(r.URL.Path, "/webhooks/") {
				next.ServeHTTP(w, r)
				return
			}

			role, ok := authenticate(r, provider)
			if !ok {
				log.Warn("auth.rejected", map[string]any{"path": r.URL.Path})
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":{"code":"unauthorized","message":"missing or invalid service token"}}`))
				return
			}

			ctx := context.WithValue(r.Context(), principalKey, Principal{Role: role})
			if m := r.Header.Get("X-Merchant-ID"); m != "" {
				ctx = context.WithValue(ctx, merchantIDKey, m)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func authenticate(r *http.Request, provider TokenProvider) (Role, bool) {
	if tok := r.Header.Get("X-Ops-Token"); tok != "" {
		if validToken(r.Context(), provider, RoleOps, tok) {
			return RoleOps, true
		}
		return "", false
	}
	if tok := r.Header.Get("X-Service-Token"); tok != "" {
		if validToken(r.Context(), provider, RoleService, tok) {
			return RoleService, true
		}
		return "", false
	}
	return "", false
}

func validToken(ctx context.Context, provider TokenProvider, role Role, token string) bool {
	hashes, err := provider.ValidHashes(ctx, role)
	if err != nil {
		return false
	}
	sum := sha256.Sum256([]byte(token))
	_, ok := hashes[hex.EncodeToString(sum[:])]
	return ok
}

type StaticTokenProvider struct {
	service map[string]struct{}
	ops     map[string]struct{}
}

func NewStaticTokenProvider(serviceTokens, opsTokens []string) *StaticTokenProvider {
	return &StaticTokenProvider{
		service: hashSet(serviceTokens),
		ops:     hashSet(opsTokens),
	}
}

func (p *StaticTokenProvider) ValidHashes(_ context.Context, role Role) (map[string]struct{}, error) {
	switch role {
	case RoleOps:
		return p.ops, nil
	case RoleService:
		return p.service, nil
	default:
		return nil, nil
	}
}

func (p *StaticTokenProvider) HasAny() bool {
	return len(p.service) > 0 || len(p.ops) > 0
}

func hashSet(tokens []string) map[string]struct{} {
	set := make(map[string]struct{}, len(tokens))
	for _, t := range tokens {
		if t == "" {
			continue
		}
		sum := sha256.Sum256([]byte(t))
		set[hex.EncodeToString(sum[:])] = struct{}{}
	}
	return set
}
