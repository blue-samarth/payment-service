package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type memIdempotency struct {
	mu      sync.Mutex
	records map[string]*memRecord
}

type memRecord struct {
	requestHash string
	status      string
	response    []byte
}

func newMemIdempotency() *memIdempotency {
	return &memIdempotency{records: map[string]*memRecord{}}
}

func (m *memIdempotency) Reserve(_ context.Context, composite, requestHash string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.records[composite]; ok {
		return false, nil
	}
	m.records[composite] = &memRecord{requestHash: requestHash, status: "PROCESSING"}
	return true, nil
}

func (m *memIdempotency) Lookup(_ context.Context, composite string) (bool, string, string, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.records[composite]
	if !ok {
		return false, "", "", nil, nil
	}
	return true, r.requestHash, r.status, r.response, nil
}

func (m *memIdempotency) Complete(_ context.Context, composite string, response []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.records[composite]; ok {
		r.status = "COMPLETED"
		r.response = response
	}
	return nil
}

func (m *memIdempotency) Release(_ context.Context, composite string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, composite)
	return nil
}

func countingHandler(calls *int64, status int, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(calls, 1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func doIdem(h http.Handler, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestIdempotency_NoKeyPassesThrough(t *testing.T) {
	var calls int64
	h := Idempotency(newMemIdempotency(), noopLog{})(countingHandler(&calls, http.StatusCreated, `{"ok":true}`))
	rec := doIdem(h, "", `{"a":1}`)
	if rec.Code != http.StatusCreated || calls != 1 {
		t.Fatalf("expected passthrough (201, 1 call), got %d / %d", rec.Code, calls)
	}
}

func TestIdempotency_ReplaysCachedResponse(t *testing.T) {
	var calls int64
	store := newMemIdempotency()
	h := Idempotency(store, noopLog{})(countingHandler(&calls, http.StatusCreated, `{"id":"abc"}`))

	first := doIdem(h, "key-1", `{"a":1}`)
	second := doIdem(h, "key-1", `{"a":1}`)

	if calls != 1 {
		t.Errorf("handler should run once, ran %d times", calls)
	}
	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Errorf("both should be 201, got %d / %d", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() {
		t.Errorf("replayed body mismatch: %q vs %q", first.Body.String(), second.Body.String())
	}
	if second.Header().Get("Idempotent-Replayed") != "true" {
		t.Error("expected Idempotent-Replayed header on the cached response")
	}
}

func TestIdempotency_SameKeyDifferentBody409(t *testing.T) {
	var calls int64
	store := newMemIdempotency()
	h := Idempotency(store, noopLog{})(countingHandler(&calls, http.StatusCreated, `{"id":"abc"}`))

	doIdem(h, "key-2", `{"a":1}`)
	conflict := doIdem(h, "key-2", `{"a":2}`)

	if conflict.Code != http.StatusConflict {
		t.Fatalf("expected 409 for reused key with different body, got %d", conflict.Code)
	}
	if calls != 1 {
		t.Errorf("handler should not run for the conflicting request, ran %d", calls)
	}
}

func TestIdempotency_5xxNotCached(t *testing.T) {
	var calls int64
	store := newMemIdempotency()
	h := Idempotency(store, noopLog{})(countingHandler(&calls, http.StatusInternalServerError, `boom`))

	doIdem(h, "key-3", `{"a":1}`)
	doIdem(h, "key-3", `{"a":1}`)

	if calls != 2 {
		t.Errorf("a 5xx should release the key so retries re-run the handler, ran %d", calls)
	}
}
