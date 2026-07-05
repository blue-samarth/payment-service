package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"samarth/payment-service/internal/ports"
)

type ResponseCacheStore interface {
	Get(ctx context.Context, key string) (found bool, response []byte, err error)
	Put(ctx context.Context, key string, response []byte) error
}

type cachedResponse struct {
	StatusCode int    `json:"status_code"`
	Body       []byte `json:"body"`
}

func ResponseCache(cache ResponseCacheStore, log ports.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("Idempotency-Key")
			if key == "" || !isMutating(r.Method) {
				next.ServeHTTP(w, r)
				return
			}

			composite := cacheKey(r, key)

			if found, body, err := cache.Get(r.Context(), composite); err == nil && found {
				replayCached(w, body, log)
				return
			}

			rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			if rec.status >= 200 && rec.status < 300 {
				if envelope, err := json.Marshal(cachedResponse{StatusCode: rec.status, Body: rec.body.Bytes()}); err == nil {
					_ = cache.Put(r.Context(), composite, envelope)
				}
			}
		})
	}
}

func cacheKey(r *http.Request, key string) string {
	merchant := MerchantIDFromContext(r.Context())
	return hashString(merchant + ":" + r.Method + " " + r.URL.Path + ":" + key)
}

type responseRecorder struct {
	http.ResponseWriter
	status      int
	body        bytes.Buffer
	wroteHeader bool
}

func (rec *responseRecorder) WriteHeader(code int) {
	if rec.wroteHeader {
		return
	}
	rec.status = code
	rec.wroteHeader = true
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *responseRecorder) Write(b []byte) (int, error) {
	if !rec.wroteHeader {
		rec.WriteHeader(http.StatusOK)
	}
	rec.body.Write(b)
	return rec.ResponseWriter.Write(b)
}

func replayCached(w http.ResponseWriter, raw []byte, log ports.Logger) {
	var cached cachedResponse
	if err := json.Unmarshal(raw, &cached); err != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Idempotent-Replayed", "true")
	w.WriteHeader(cached.StatusCode)
	_, _ = w.Write(cached.Body)
}

func isMutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
