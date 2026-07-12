package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakePinger struct{ err error }

func (p fakePinger) Ping(ctx context.Context) error { return p.err }

func TestHealth_OK(t *testing.T) {
	h := NewHealthHandler(
		Check{Name: "database", Pinger: fakePinger{}},
		Check{Name: "redis", Pinger: fakePinger{}},
	)
	rec := httptest.NewRecorder()
	h.Health(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestHealth_DBDown(t *testing.T) {
	h := NewHealthHandler(Check{Name: "database", Pinger: fakePinger{err: errors.New("down")}})
	rec := httptest.NewRecorder()
	h.Health(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestHealth_RedisDown(t *testing.T) {
	h := NewHealthHandler(
		Check{Name: "database", Pinger: fakePinger{}},
		Check{Name: "redis", Pinger: fakePinger{err: errors.New("down")}},
	)
	rec := httptest.NewRecorder()
	h.Health(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}
