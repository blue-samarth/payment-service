package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"

	"samarth/payment-service/internal/ports"
)

type IdempotencyStore interface {
	Reserve(ctx context.Context, compositeHash, requestHash string) (bool, error)
	Lookup(ctx context.Context, compositeHash string) (found bool, requestHash, status string, response []byte, err error)
	Complete(ctx context.Context, compositeHash string, response []byte) error
	Release(ctx context.Context, compositeHash string) error
}

type cachedResponse struct {
	StatusCode int    `json:"status_code"`
	Body       []byte `json:"body"`
}

func Idempotency(store IdempotencyStore, log ports.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("Idempotency-Key")
			if key == "" || !isMutating(r.Method) {
				next.ServeHTTP(w, r)
				return
			}

			body, err := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if err != nil {
				writeJSONError(w, http.StatusBadRequest, "invalid_request_body", "could not read request body")
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))

			merchant := MerchantIDFromContext(r.Context())
			composite := hashString(merchant + ":" + r.Method + " " + r.URL.Path + ":" + key)
			requestHash := hashBytes(body)

			claimed, err := store.Reserve(r.Context(), composite, requestHash)
			if err != nil {
				writeJSONError(w, http.StatusInternalServerError, "idempotency_error", "could not process idempotency key")
				return
			}

			if !claimed {
				found, prevHash, status, resp, err := store.Lookup(r.Context(), composite)
				if err != nil || !found {
					writeJSONError(w, http.StatusInternalServerError, "idempotency_error", "could not resolve idempotency key")
					return
				}
				if prevHash != requestHash {
					writeJSONError(w, http.StatusConflict, "idempotency_key_reused", "idempotency key reused with a different request body")
					return
				}
				if status != "COMPLETED" {
					writeJSONError(w, http.StatusConflict, "idempotency_in_progress", "a request with this idempotency key is already in progress")
					return
				}
				replayCached(w, resp, log)
				return
			}

			rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			if rec.status >= 500 {
				_ = store.Release(r.Context(), composite)
				return
			}

			envelope, err := json.Marshal(cachedResponse{StatusCode: rec.status, Body: rec.body.Bytes()})
			if err != nil {
				return
			}
			if err := store.Complete(r.Context(), composite, envelope); err != nil {
				log.Error("idempotency.complete_failed", map[string]any{
					ports.FieldErrorCode:     "idempotency_complete_failed",
					ports.FieldTraceID:       RequestIDFromContext(r.Context()),
					ports.FieldTransactionID: "",
				}, err)
			}
		})
	}
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
		writeJSONError(w, http.StatusInternalServerError, "idempotency_error", "corrupted cached response")
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

func hashString(s string) string { return hashBytes([]byte(s)) }

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"error":{"code":"` + code + `","message":"` + message + `"}}`))
}
