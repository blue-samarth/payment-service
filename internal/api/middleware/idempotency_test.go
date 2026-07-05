package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type memCache struct {
	mu      sync.Mutex
	entries map[string][]byte
	getErr  error
	putErr  error
}

func newMemCache() *memCache { return &memCache{entries: map[string][]byte{}} }

func (m *memCache) Get(_ context.Context, key string) (bool, []byte, error) {
	if m.getErr != nil {
		return false, nil, m.getErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	body, ok := m.entries[key]
	return ok, body, nil
}

func (m *memCache) Put(_ context.Context, key string, response []byte) error {
	if m.putErr != nil {
		return m.putErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries[key] = response
	return nil
}

func countingHandler(calls *int64, status int, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(calls, 1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func doCached(h http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestResponseCache_NoKeyPassesThrough(t *testing.T) {
	var calls int64
	h := ResponseCache(newMemCache(), noopLog{})(countingHandler(&calls, http.StatusCreated, `{"ok":true}`))
	rec := doCached(h, "", `{"a":1}`)
	if rec.Code != http.StatusCreated || calls != 1 {
		t.Fatalf("expected passthrough (201, 1 call), got %d / %d", rec.Code, calls)
	}
}

func TestResponseCache_ReplaysCachedHit(t *testing.T) {
	var calls int64
	h := ResponseCache(newMemCache(), noopLog{})(countingHandler(&calls, http.StatusCreated, `{"id":"abc"}`))

	first := doCached(h, "key-1", `{"a":1}`)
	second := doCached(h, "key-1", `{"a":1}`)

	if calls != 1 {
		t.Errorf("second request should be served from cache, handler ran %d times", calls)
	}
	if first.Body.String() != second.Body.String() || second.Code != http.StatusCreated {
		t.Errorf("replay mismatch: %q/%d vs %q/%d", first.Body.String(), first.Code, second.Body.String(), second.Code)
	}
	if second.Header().Get("Idempotent-Replayed") != "true" {
		t.Error("expected Idempotent-Replayed header on the cached response")
	}
}

func TestResponseCache_Only2xxStored(t *testing.T) {
	var calls int64
	h := ResponseCache(newMemCache(), noopLog{})(countingHandler(&calls, http.StatusInternalServerError, `boom`))
	doCached(h, "key-2", `{"a":1}`)
	doCached(h, "key-2", `{"a":1}`)
	if calls != 2 {
		t.Errorf("a 5xx must not be cached; both requests should run, ran %d", calls)
	}
}

func TestResponseCache_4xxNotStored(t *testing.T) {
	var calls int64
	h := ResponseCache(newMemCache(), noopLog{})(countingHandler(&calls, http.StatusConflict, `nope`))
	doCached(h, "key-3", `{"a":1}`)
	doCached(h, "key-3", `{"a":1}`)
	if calls != 2 {
		t.Errorf("only 2xx is cached; a 4xx should re-run, ran %d", calls)
	}
}

func TestResponseCache_SwallowsBackendErrors(t *testing.T) {
	var calls int64
	cache := newMemCache()
	cache.getErr = errors.New("cache down")
	cache.putErr = errors.New("cache down")
	h := ResponseCache(cache, noopLog{})(countingHandler(&calls, http.StatusCreated, `{"ok":true}`))

	rec := doCached(h, "key-4", `{"a":1}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("a cache-backend outage must fall through to the handler, got %d", rec.Code)
	}
	if calls != 1 {
		t.Errorf("handler should have run despite cache errors, ran %d", calls)
	}
}

func TestResponseCache_MerchantScopedKeys(t *testing.T) {
	var calls int64
	h := ResponseCache(newMemCache(), noopLog{})(countingHandler(&calls, http.StatusCreated, `{"ok":true}`))

	for _, m := range []string{"merchant-a", "merchant-b"} {
		req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{"a":1}`))
		req.Header.Set("Idempotency-Key", "shared-key")
		ctx := context.WithValue(req.Context(), merchantIDKey, m)
		h.ServeHTTP(httptest.NewRecorder(), req.WithContext(ctx))
	}
	if calls != 2 {
		t.Errorf("the same key under different merchants must not collide in the cache, ran %d", calls)
	}
}
