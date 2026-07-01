package observability

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"samarth/payment-service/internal/ports"
)

func captureLogger(buf *bytes.Buffer) *SlogLogger {
	h := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: levelTrace})
	return NewSlogLoggerFromHandler(h)
}

func lastEntry(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &entry); err != nil {
		t.Fatalf("failed to parse log line %q: %v", lines[len(lines)-1], err)
	}
	return entry
}

func TestSlogLogger_Info(t *testing.T) {
	var buf bytes.Buffer
	log := captureLogger(&buf)
	log.Info("transaction.created", map[string]any{"transaction_id": "tx-1"})

	entry := lastEntry(t, &buf)
	if entry["msg"] != "transaction.created" {
		t.Errorf("expected msg transaction.created, got %v", entry["msg"])
	}
	if entry["transaction_id"] != "tx-1" {
		t.Errorf("expected field transaction_id=tx-1, got %v", entry["transaction_id"])
	}
	if entry["level"] != "INFO" {
		t.Errorf("expected level INFO, got %v", entry["level"])
	}
}

func TestSlogLogger_ErrorContractSatisfied(t *testing.T) {
	var buf bytes.Buffer
	log := captureLogger(&buf)
	log.Error("payment.failed", map[string]any{
		ports.FieldErrorCode:     "x",
		ports.FieldTraceID:       "t",
		ports.FieldTransactionID: "tx",
	}, errors.New("boom"))

	entry := lastEntry(t, &buf)
	if _, ok := entry["log_validation_error"]; ok {
		t.Error("did not expect log_validation_error when all required fields present")
	}
	if entry["error"] != "boom" {
		t.Errorf("expected error field, got %v", entry["error"])
	}
}

func TestSlogLogger_ErrorContractViolated(t *testing.T) {
	var buf bytes.Buffer
	log := captureLogger(&buf)
	log.Error("payment.failed", map[string]any{ports.FieldErrorCode: "x"}, errors.New("boom"))

	entry := lastEntry(t, &buf)
	v, ok := entry["log_validation_error"].(string)
	if !ok {
		t.Fatal("expected log_validation_error field when required fields missing")
	}
	if !strings.Contains(v, ports.FieldTraceID) || !strings.Contains(v, ports.FieldTransactionID) {
		t.Errorf("expected missing fields named in validation error, got %q", v)
	}
}

func TestSlogLogger_ErrorNilFields(t *testing.T) {
	var buf bytes.Buffer
	log := captureLogger(&buf)
	log.Error("payment.failed", nil, errors.New("boom"))

	entry := lastEntry(t, &buf)
	if _, ok := entry["log_validation_error"]; !ok {
		t.Error("expected validation error with nil fields")
	}
}

func TestSlogLogger_WithMergesFields(t *testing.T) {
	var buf bytes.Buffer
	log := captureLogger(&buf)
	log.With(map[string]any{"merchant_id": "m-1"}).Info("event", map[string]any{"k": "v"})

	entry := lastEntry(t, &buf)
	if entry["merchant_id"] != "m-1" {
		t.Errorf("expected base field from With, got %v", entry["merchant_id"])
	}
	if entry["k"] != "v" {
		t.Errorf("expected call field, got %v", entry["k"])
	}
}

func TestSlogLogger_LevelsBelowThresholdSuppressed(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	log := NewSlogLoggerFromHandler(h)

	log.Debug("debug.event", nil)
	log.Trace("trace.event", nil)
	if buf.Len() != 0 {
		t.Errorf("expected debug/trace suppressed at INFO level, got %q", buf.String())
	}

	log.Warn("warn.event", nil)
	if buf.Len() == 0 {
		t.Error("expected warn to be emitted at INFO level")
	}
}
