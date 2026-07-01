package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"samarth/payment-service/internal/ports"
)

const levelTrace = slog.Level(-8)

type SlogLogger struct {
	base *slog.Logger
}

func NewSlogLogger(level slog.Level) *SlogLogger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return &SlogLogger{base: slog.New(h)}
}

func fieldsToAttrs(fields map[string]any) []any {
	attrs := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		attrs = append(attrs, k, v)
	}
	return attrs
}

func NewSlogLoggerFromHandler(h slog.Handler) *SlogLogger { return &SlogLogger{base: slog.New(h)} }
func (l *SlogLogger) Info(event string, fields map[string]any) { l.base.Info(event, fieldsToAttrs(fields)...) }
func (l *SlogLogger) Warn(event string, fields map[string]any) { l.base.Warn(event, fieldsToAttrs(fields)...) }
func (l *SlogLogger) Debug(event string, fields map[string]any) { l.base.Debug(event, fieldsToAttrs(fields)...) }
func (l *SlogLogger) Trace(event string, fields map[string]any) { l.base.Log(context.Background(), levelTrace, event, fieldsToAttrs(fields)...) }
func (l *SlogLogger) With(fields map[string]any) ports.Logger { return &SlogLogger{base: l.base.With(fieldsToAttrs(fields)...)} }

func (l *SlogLogger) Error(event string, fields map[string]any, err error) {
	if fields == nil {
		fields = map[string]any{}
	}

	var missing []string
	for _, required := range []string{ports.FieldErrorCode, ports.FieldTraceID, ports.FieldTransactionID} {
		if _, ok := fields[required]; !ok {
			missing = append(missing, required)
		}
	}

	attrs := fieldsToAttrs(fields)
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	if len(missing) > 0 {
		attrs = append(attrs, "log_validation_error", "missing required fields: "+strings.Join(missing, ","))
	}

	l.base.Error(event, attrs...)
}

var _ ports.Logger = (*SlogLogger)(nil)
