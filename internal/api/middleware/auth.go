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
	Role       Role
	MerchantID string
}
type TokenProvider interface {
	Resolve(ctx context.Context, role Role, token string) (Principal, bool, error)
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

			principal, ok := authenticate(r, provider)
			if !ok {
				log.Warn("auth.rejected", map[string]any{"path": r.URL.Path})
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":{"code":"unauthorized","message":"missing or invalid service token"}}`))
				return
			}

			ctx := context.WithValue(r.Context(), principalKey, principal)
			if principal.MerchantID != "" {
				ctx = context.WithValue(ctx, merchantIDKey, principal.MerchantID)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func authenticate(r *http.Request, provider TokenProvider) (Principal, bool) {
	if tok := r.Header.Get("X-Ops-Token"); tok != "" {
		return resolveToken(r.Context(), provider, RoleOps, tok)
	}
	if tok := r.Header.Get("X-Service-Token"); tok != "" {
		return resolveToken(r.Context(), provider, RoleService, tok)
	}
	return Principal{}, false
}

func resolveToken(ctx context.Context, provider TokenProvider, role Role, token string) (Principal, bool) {
	principal, ok, err := provider.Resolve(ctx, role, token)
	if err != nil || !ok {
		return Principal{}, false
	}
	return principal, true
}

type StaticTokenProvider struct {
	service map[string]Principal
	ops     map[string]Principal
}

func NewStaticTokenProvider(serviceTokens map[string]string, opsTokens []string) *StaticTokenProvider {
	return &StaticTokenProvider{
		service: servicePrincipals(serviceTokens),
		ops:     opsPrincipals(opsTokens),
	}
}

func (p *StaticTokenProvider) Resolve(_ context.Context, role Role, token string) (Principal, bool, error) {
	var set map[string]Principal
	switch role {
	case RoleOps:
		set = p.ops
	case RoleService:
		set = p.service
	default:
		return Principal{}, false, nil
	}
	sum := sha256.Sum256([]byte(token))
	principal, ok := set[hex.EncodeToString(sum[:])]
	return principal, ok, nil
}

func (p *StaticTokenProvider) HasAny() bool { return len(p.service) > 0 || len(p.ops) > 0 }

func servicePrincipals(tokens map[string]string) map[string]Principal {
	set := make(map[string]Principal, len(tokens))
	for token, merchantID := range tokens {
		if token == "" {
			continue
		}
		sum := sha256.Sum256([]byte(token))
		set[hex.EncodeToString(sum[:])] = Principal{Role: RoleService, MerchantID: merchantID}
	}
	return set
}

func opsPrincipals(tokens []string) map[string]Principal {
	set := make(map[string]Principal, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		sum := sha256.Sum256([]byte(token))
		set[hex.EncodeToString(sum[:])] = Principal{Role: RoleOps}
	}
	return set
}
