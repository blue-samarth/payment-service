package middleware

import (
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
	provider := NewStaticTokenProvider(map[string]string{"svc-secret": "merchant-9"}, nil)
	var p Principal
	var merchant string
	h := Authenticate(provider, noopLog{})(authedHandler(&p, &merchant))

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Service-Token", "svc-secret")
	req.Header.Set("X-Merchant-ID", "attacker-merchant") // spoofed header must be ignored
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if p.Role != RoleService {
		t.Errorf("expected service role, got %q", p.Role)
	}
	if merchant != "merchant-9" {
		t.Errorf("merchant must come from the token binding, not X-Merchant-ID, got %q", merchant)
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
	provider := NewStaticTokenProvider(map[string]string{"svc-secret": "merchant-1"}, nil)
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
	provider := NewStaticTokenProvider(map[string]string{"svc-secret": "merchant-1"}, nil)
	h := Authenticate(provider, noopLog{})(okHandler())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/payments", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing token, got %d", rec.Code)
	}
}

func TestAuth_HealthExempt(t *testing.T) {
	provider := NewStaticTokenProvider(map[string]string{"svc-secret": "merchant-1"}, nil)
	h := Authenticate(provider, noopLog{})(okHandler())

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected /health to be exempt from auth, got %d", rec.Code)
	}
}

func TestAuth_OpsTokenNotValidAsService(t *testing.T) {
	provider := NewStaticTokenProvider(map[string]string{"svc-secret": "merchant-1"}, []string{"ops-secret"})
	h := Authenticate(provider, noopLog{})(okHandler())

	req := httptest.NewRequest(http.MethodPost, "/payments", nil)
	req.Header.Set("X-Service-Token", "ops-secret") // ops token in the service header
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ops token must not authenticate as a service token, got %d", rec.Code)
	}
}

