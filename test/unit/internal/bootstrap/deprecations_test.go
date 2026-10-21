package bootstrap_test

import . "samarth/payment-service/internal/bootstrap"

import (
	"testing"

	"samarth/payment-service/internal/ports"
)

type warnLog struct {
	events []string
	fields []map[string]any
}

func (l *warnLog) Warn(event string, f map[string]any) {
	l.events = append(l.events, event)
	l.fields = append(l.fields, f)
}
func (*warnLog) Info(string, map[string]any)         {}
func (*warnLog) Error(string, map[string]any, error) {}
func (*warnLog) Debug(string, map[string]any)        {}
func (*warnLog) Trace(string, map[string]any)        {}
func (l *warnLog) With(map[string]any) ports.Logger  { return l }

func TestLogDeprecations_OneWarningEach(t *testing.T) {
	log := &warnLog{}
	LogDeprecations(log, []string{"REDIS_CACHE_DB is deprecated", "another"})

	if len(log.events) != 2 || log.events[0] != ports.LogEventConfigDeprecated || log.fields[0]["warning"] != "REDIS_CACHE_DB is deprecated" {
		t.Fatalf("each deprecation must be logged once as config.deprecated, got %v %v", log.events, log.fields)
	}

	quiet := &warnLog{}
	LogDeprecations(quiet, nil)
	if len(quiet.events) != 0 {
		t.Fatal("no deprecations, no warnings")
	}
}
