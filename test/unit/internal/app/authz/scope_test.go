package authz_test

import . "samarth/payment-service/internal/app/authz"

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/ports"
)

type recordingLog struct {
	events []string
	fields []map[string]any
}

func (l *recordingLog) Info(string, map[string]any) {}
func (l *recordingLog) Warn(event string, fields map[string]any) {
	l.events = append(l.events, event)
	l.fields = append(l.fields, fields)
}
func (l *recordingLog) Error(string, map[string]any, error) {}
func (l *recordingLog) Debug(string, map[string]any)        {}
func (l *recordingLog) Trace(string, map[string]any)        {}
func (l *recordingLog) With(map[string]any) ports.Logger    { return l }

type recordingMetrics struct{ increments map[string]int }

func (m *recordingMetrics) Increment(name string, tags map[string]string) {
	m.increments[name+"/"+tags["operation"]]++
}
func (m *recordingMetrics) Histogram(string, float64, map[string]string) {}
func (m *recordingMetrics) Gauge(string, float64, map[string]string)     {}

func TestScope_ZeroValueDeniesEverything(t *testing.T) {
	var zero Scope
	for _, owner := range []uuid.UUID{uuid.Nil, uuid.New()} {
		if zero.Allows(owner) {
			t.Fatalf("the zero Scope must deny, but allowed owner %s", owner)
		}
	}
}

func TestScope_MerchantAndAll(t *testing.T) {
	own := uuid.New()
	if !Merchant(own).Allows(own) || Merchant(own).Allows(uuid.New()) {
		t.Error("a merchant scope must allow exactly its own merchant")
	}
	if Merchant(uuid.Nil).Allows(uuid.Nil) {
		t.Error("a scope built from the nil UUID must not match a nil owner")
	}
	if !AllMerchants().Allows(uuid.New()) {
		t.Error("AllMerchants must allow any owner")
	}
}

func TestCheck_DenialIsLoggedCountedAndReadsAsNotFound(t *testing.T) {
	log := &recordingLog{}
	metrics := &recordingMetrics{increments: map[string]int{}}
	caller, owner, txn := uuid.New(), uuid.New(), uuid.New()

	err := Check(log, metrics, "cancel_payment", Merchant(caller), owner, txn)
	if !errors.Is(err, ErrDenied) || !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("expected ErrDenied wrapping ports.ErrNotFound, got %v", err)
	}
	if len(log.events) != 1 || log.events[0] != ports.LogEventAuthzDenied {
		t.Fatalf("expected one %s log, got %v", ports.LogEventAuthzDenied, log.events)
	}
	f := log.fields[0]
	if f["scope"] != caller.String() || f["owner_merchant_id"] != owner.String() || f[ports.FieldTransactionID] != txn.String() || f["operation"] != "cancel_payment" {
		t.Errorf("log fields missing the caller, owner, transaction or operation: %v", f)
	}
	if metrics.increments[ports.MetricAuthzDenied+"/cancel_payment"] != 1 {
		t.Errorf("expected one %s increment tagged with the operation, got %v", ports.MetricAuthzDenied, metrics.increments)
	}

	if err := Check(log, metrics, "cancel_payment", Merchant(owner), owner, txn); err != nil || len(log.events) != 1 {
		t.Fatalf("an allowed check must return nil and log nothing, got %v and %d logs", err, len(log.events))
	}
}
