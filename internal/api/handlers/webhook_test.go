package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	appwebhook "samarth/payment-service/internal/app/webhook"
	"samarth/payment-service/internal/ports"
)

type noopLog struct{}

func (noopLog) Info(string, map[string]any)         {}
func (noopLog) Warn(string, map[string]any)         {}
func (noopLog) Error(string, map[string]any, error) {}
func (noopLog) Debug(string, map[string]any)        {}
func (noopLog) Trace(string, map[string]any)        {}
func (l noopLog) With(map[string]any) ports.Logger  { return l }

type fakeProcessor struct {
	calls   int
	outcome appwebhook.Outcome
}

func (f *fakeProcessor) Process(context.Context, string, appwebhook.Event, []byte) (appwebhook.Outcome, error) {
	f.calls++
	return f.outcome, nil
}

type fakeParser struct {
	event *ports.GatewayWebhookEvent
	err   error
}

func (f fakeParser) ParseWebhook([]byte, map[string]string, string) (*ports.GatewayWebhookEvent, error) {
	return f.event, f.err
}

type fakeResolver struct {
	parser ports.GatewayWebhookParser
	ok     bool
}

func (f fakeResolver) WebhookParser(string) (ports.GatewayWebhookParser, bool) {
	return f.parser, f.ok
}

type fakeSecrets struct{ secret string }

func (f fakeSecrets) WebhookSecret(context.Context, string) (string, error) { return f.secret, nil }

type fakePolicy struct{ window, skew int }

func (p fakePolicy) WebhookPolicy(context.Context, string) (int, int, error) {
	return p.window, p.skew, nil
}

func okEvent() *ports.GatewayWebhookEvent {
	return &ports.GatewayWebhookEvent{EventID: "evt_1", GatewayReferenceID: "ref_1", Status: ports.GatewayPaymentStatusSucceeded}
}

func newHandler(proc *fakeProcessor, parser ports.GatewayWebhookParser, secret string, policy WebhookPolicyProvider) *WebhookHandler {
	return NewWebhookHandler(proc, fakeResolver{parser: parser, ok: parser != nil}, fakeSecrets{secret: secret}, policy, noopLog{})
}

func postWebhook(h *WebhookHandler, gateway, ts string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/"+gateway, strings.NewReader(`raw`))
	req.SetPathValue("gateway_id", gateway)
	if ts != "" {
		req.Header.Set("X-Webhook-Timestamp", ts)
	}
	rec := httptest.NewRecorder()
	h.Handle(rec, req)
	return rec
}

func TestWebhook_ValidEventProcesses(t *testing.T) {
	proc := &fakeProcessor{outcome: appwebhook.Outcome{Resolved: true}}
	h := newHandler(proc, fakeParser{event: okEvent()}, "shh", nil)
	rec := postWebhook(h, "stripe", "")
	if rec.Code != http.StatusOK || proc.calls != 1 {
		t.Fatalf("expected 200 + 1 call, got %d / %d (%s)", rec.Code, proc.calls, rec.Body.String())
	}
}

func TestWebhook_InvalidSignature401(t *testing.T) {
	proc := &fakeProcessor{}
	h := newHandler(proc, fakeParser{err: ports.ErrWebhookSignature}, "shh", nil)
	rec := postWebhook(h, "stripe", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad signature, got %d", rec.Code)
	}
	if proc.calls != 0 {
		t.Error("processor must not run on signature failure")
	}
}

func TestWebhook_ParseError400(t *testing.T) {
	h := newHandler(&fakeProcessor{}, fakeParser{err: ports.ErrWebhookParse}, "shh", nil)
	rec := postWebhook(h, "stripe", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for parse failure, got %d", rec.Code)
	}
}

func TestWebhook_UnknownGateway404(t *testing.T) {
	// resolver returns ok=false (no parser registered)
	h := NewWebhookHandler(&fakeProcessor{}, fakeResolver{ok: false}, fakeSecrets{secret: "shh"}, nil, noopLog{})
	rec := postWebhook(h, "mystery", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unregistered gateway, got %d", rec.Code)
	}
}

func TestWebhook_NoSecret401(t *testing.T) {
	h := newHandler(&fakeProcessor{}, fakeParser{event: okEvent()}, "", nil)
	rec := postWebhook(h, "stripe", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when no secret configured, got %d", rec.Code)
	}
}

func TestWebhook_FreshTimestampAccepted(t *testing.T) {
	proc := &fakeProcessor{outcome: appwebhook.Outcome{Resolved: true}}
	h := newHandler(proc, fakeParser{event: okEvent()}, "shh", fakePolicy{window: 300, skew: 30})
	rec := postWebhook(h, "stripe", strconv.FormatInt(time.Now().Unix(), 10))
	if rec.Code != http.StatusOK {
		t.Fatalf("fresh timestamp should pass, got %d", rec.Code)
	}
}

func TestWebhook_StaleTimestampRejected(t *testing.T) {
	proc := &fakeProcessor{}
	h := newHandler(proc, fakeParser{event: okEvent()}, "shh", fakePolicy{window: 300, skew: 30})
	rec := postWebhook(h, "stripe", strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10))
	if rec.Code != http.StatusBadRequest || proc.calls != 0 {
		t.Fatalf("stale timestamp should be rejected without processing, got %d / %d calls", rec.Code, proc.calls)
	}
}

func TestWebhook_InvalidTimestampRejected(t *testing.T) {
	h := newHandler(&fakeProcessor{}, fakeParser{event: okEvent()}, "shh", fakePolicy{window: 300, skew: 30})
	rec := postWebhook(h, "stripe", "not-a-number")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("non-numeric timestamp should be rejected, got %d", rec.Code)
	}
}

func TestWebhook_NoTimestampHeaderStillAccepted(t *testing.T) {
	proc := &fakeProcessor{outcome: appwebhook.Outcome{Resolved: true}}
	h := newHandler(proc, fakeParser{event: okEvent()}, "shh", fakePolicy{window: 300, skew: 30})
	rec := postWebhook(h, "stripe", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("absent timestamp should not block, got %d", rec.Code)
	}
}
