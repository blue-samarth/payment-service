package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func authedHandler(captured *Principal, merchant *string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p, ok := PrincipalFromContext(r.Context()); ok {
			*captured = p
		}
		*merchant = MerchantIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
}

func TestAuth_ValidServiceToken(t *testing.T) {
	provider := NewStaticTokenProvider([]string{"svc-secret"}, nil)
	var p Principal
	var merchant string
	h := Authenticate(provider, noopLog{})(authedHandler(&p, &merchant))

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Service-Token", "svc-secret")
	req.Header.Set("X-Merchant-ID", "merchant-9")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if p.Role != RoleService {
		t.Errorf("expected service role, got %q", p.Role)
	}
	if merchant != "merchant-9" {
		t.Errorf("expected merchant injected into context, got %q", merchant)
	}
}

func TestAuth_ValidOpsToken(t *testing.T) {
	provider := NewStaticTokenProvider(nil, []string{"ops-secret"})
	var p Principal
	var merchant string
	h := Authenticate(provider, noopLog{})(authedHandler(&p, &merchant))

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Ops-Token", "ops-secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || p.Role != RoleOps {
		t.Fatalf("expected 200 ops, got %d role=%q", rec.Code, p.Role)
	}
}

func TestAuth_InvalidToken401(t *testing.T) {
	provider := NewStaticTokenProvider([]string{"svc-secret"}, nil)
	h := Authenticate(provider, noopLog{})(okHandler())

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Service-Token", "wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for invalid token, got %d", rec.Code)
	}
}

func TestAuth_MissingToken401(t *testing.T) {
	provider := NewStaticTokenProvider([]string{"svc-secret"}, nil)
	h := Authenticate(provider, noopLog{})(okHandler())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/payments", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing token, got %d", rec.Code)
	}
}

func TestAuth_HealthExempt(t *testing.T) {
	provider := NewStaticTokenProvider([]string{"svc-secret"}, nil)
	h := Authenticate(provider, noopLog{})(okHandler())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected /health to be exempt from auth, got %d", rec.Code)
	}
}

func TestAuth_OpsTokenNotValidAsService(t *testing.T) {
	provider := NewStaticTokenProvider([]string{"svc-secret"}, []string{"ops-secret"})
	h := Authenticate(provider, noopLog{})(okHandler())

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Service-Token", "ops-secret") // ops token in the service header
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ops token must not authenticate as a service token, got %d", rec.Code)
	}
}

func TestIdempotency_MerchantScopedKeys(t *testing.T) {
	store := newMemIdempotency()
	var calls int
	h := Idempotency(store, noopLog{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusCreated)
	}))

	// Same idempotency key + body, but different merchants in context must not collide.
	for _, m := range []string{"merchant-a", "merchant-b"} {
		req := httptest.NewRequest(http.MethodPost, "/payments", nil)
		req.Header.Set("Idempotency-Key", "shared-key")
		ctx := context.WithValue(req.Context(), merchantIDKey, m)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req.WithContext(ctx))
	}

	if calls != 2 {
		t.Errorf("different merchants sharing an idempotency key must each execute, ran %d", calls)
	}
}
