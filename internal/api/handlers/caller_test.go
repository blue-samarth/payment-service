package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/api/middleware"
	appcancel "samarth/payment-service/internal/app/cancel"
	"samarth/payment-service/internal/domain/transaction"
)

type fixedProvider struct{ principal middleware.Principal }

func (p fixedProvider) Resolve(context.Context, middleware.Role, string) (middleware.Principal, bool, error) {
	return p.principal, true, nil
}

type recordingCancelService struct {
	calls int
	got   appcancel.CancelInput
}

func (s *recordingCancelService) Cancel(_ context.Context, in appcancel.CancelInput) (appcancel.Result, error) {
	s.calls++
	s.got = in
	return appcancel.Result{Outcome: appcancel.OutcomeRequested, Status: transaction.StatusProcessing}, nil
}

func cancelAs(principal middleware.Principal, svc *recordingCancelService, body string) *httptest.ResponseRecorder {
	h := middleware.Authenticate(fixedProvider{principal}, noopLog{}, nil, nil)(http.HandlerFunc(NewCancelHandler(svc).Cancel))
	id := uuid.NewString()
	req := httptest.NewRequest(http.MethodPost, "/payments/"+id+"/cancel", strings.NewReader(body))
	req.SetPathValue("id", id)
	req.Header.Set("X-Service-Token", "any")
	req.Header.Set("Idempotency-Key", "k")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCaller_ServicePrincipalWithoutMerchantFailsClosed(t *testing.T) {
	for name, merchant := range map[string]string{"empty": "", "garbage": "not-a-uuid", "nil": uuid.Nil.String()} {
		svc := &recordingCancelService{}
		rec := cancelAs(middleware.Principal{Role: middleware.RoleService, MerchantID: merchant}, svc, `{}`)
		if rec.Code != http.StatusForbidden || svc.calls != 0 {
			t.Errorf("%s: got %d with %d service calls, want 403 and none", name, rec.Code, svc.calls)
		}
	}
}

func TestCaller_CancelIsScopedToTheTokensMerchant(t *testing.T) {
	merchant := uuid.New()
	svc := &recordingCancelService{}
	if rec := cancelAs(middleware.Principal{Role: middleware.RoleService, MerchantID: merchant.String()}, svc, `{}`); rec.Code != http.StatusOK {
		t.Fatalf("got %d", rec.Code)
	}
	if !svc.got.Scope.Allows(merchant) || svc.got.Scope.Allows(uuid.New()) || svc.got.By != transaction.ActorMerchant || svc.got.Via != transaction.CancelViaAPI {
		t.Fatalf("got %+v", svc.got)
	}

	svc = &recordingCancelService{}
	if rec := cancelAs(middleware.Principal{Role: middleware.RoleOps}, svc, `{"via":"ops-tool"}`); rec.Code != http.StatusOK {
		t.Fatalf("got %d", rec.Code)
	}
	if !svc.got.Scope.Allows(uuid.New()) || svc.got.By != transaction.ActorOps {
		t.Fatalf("ops must be unscoped and recorded as ops, got %+v", svc.got)
	}
}

func TestCaller_NoPrincipalFailsClosedBeforeTheBodyIsRead(t *testing.T) {
	svc := &recordingCancelService{}
	for _, body := range []string{`{}`, `{"via":`} {
		id := uuid.NewString()
		req := httptest.NewRequest(http.MethodPost, "/payments/"+id+"/cancel", strings.NewReader(body))
		req.SetPathValue("id", id)
		req.Header.Set("Idempotency-Key", "k")
		rec := httptest.NewRecorder()
		NewCancelHandler(svc).Cancel(rec, req)
		if rec.Code != http.StatusForbidden || svc.calls != 0 {
			t.Fatalf("body %q: a handler reached without any auth middleware must deny with 403, got %d with %d calls", body, rec.Code, svc.calls)
		}
	}

	for name, call := range map[string]func(http.ResponseWriter, *http.Request){
		"create": NewPaymentHandler(&fakeService{}).Create,
		"refund": NewRefundHandler(&fakeRefundService{}).Create,
	} {
		req := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(`{not json`))
		req.SetPathValue("id", uuid.NewString())
		rec := httptest.NewRecorder()
		call(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("%s: no principal with a malformed body must be 403, got %d", name, rec.Code)
		}
	}
}

func TestDecodeJSON_BodiesAreBounded(t *testing.T) {
	svc := &recordingCancelService{}
	big := `{"via":"` + strings.Repeat("x", maxCancelBodyBytes) + `"}`
	if rec := cancelAs(middleware.Principal{Role: middleware.RoleOps}, svc, big); rec.Code != http.StatusRequestEntityTooLarge || svc.calls != 0 {
		t.Fatalf("an oversized cancel body must be 413 without a service call, got %d with %d calls", rec.Code, svc.calls)
	}

	req := anonymous(httptest.NewRequest(http.MethodPost, "/payments/x/refunds", strings.NewReader(`{"amount":1,"reason":"`+strings.Repeat("r", maxRefundBodyBytes)+`"}`)))
	req.SetPathValue("id", uuid.NewString())
	rec := httptest.NewRecorder()
	NewRefundHandler(&fakeRefundService{}).Create(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("an oversized refund body must be 413, got %d", rec.Code)
	}
}

func TestCaller_AnonymousPrincipalIsUnscopedMerchantActor(t *testing.T) {
	svc := &recordingCancelService{}
	h := middleware.AllowAnonymous()(http.HandlerFunc(NewCancelHandler(svc).Cancel))
	id := uuid.NewString()
	req := httptest.NewRequest(http.MethodPost, "/payments/"+id+"/cancel", strings.NewReader(`{}`))
	req.SetPathValue("id", id)
	req.Header.Set("Idempotency-Key", "k")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !svc.got.Scope.Allows(uuid.New()) || svc.got.By != transaction.ActorMerchant {
		t.Fatalf("ALLOW_NO_AUTH must be explicit and unscoped, got %d %+v", rec.Code, svc.got)
	}
}

func TestCaller_CancelRejectsMalformedBody(t *testing.T) {
	svc := &recordingCancelService{}
	if rec := cancelAs(middleware.Principal{Role: middleware.RoleOps}, svc, `{"via":`); rec.Code != http.StatusBadRequest || svc.calls != 0 {
		t.Fatalf("got %d with %d calls", rec.Code, svc.calls)
	}
}

func TestCaller_MerchantFor(t *testing.T) {
	own, other := uuid.New(), uuid.New()
	scoped := caller{merchant: own, actor: transaction.ActorMerchant}
	unscoped := caller{actor: transaction.ActorOps}

	cases := []struct {
		name      string
		c         caller
		requested string
		want      uuid.UUID
		err       error
	}{
		{"scoped, omitted", scoped, "", own, nil},
		{"scoped, same", scoped, own.String(), own, nil},
		{"scoped, other is left to the service's scope check", scoped, other.String(), other, nil},
		{"scoped, garbage", scoped, "x", uuid.Nil, errMerchantInvalid},
		{"unscoped, omitted", unscoped, "", uuid.Nil, errMerchantRequired},
		{"unscoped, any", unscoped, other.String(), other, nil},
	}
	for _, tc := range cases {
		got, err := tc.c.merchantFor(tc.requested)
		if got != tc.want || !errors.Is(err, tc.err) {
			t.Errorf("%s: got %s, %v; want %s, %v", tc.name, got, err, tc.want, tc.err)
		}
	}
}

func TestCaller_ActorAndVia(t *testing.T) {
	merchant := caller{merchant: uuid.New(), actor: transaction.ActorMerchant}
	ops := caller{actor: transaction.ActorOps}

	for _, tc := range []struct {
		c         caller
		requested string
		ok        bool
	}{
		{merchant, "", true},
		{merchant, "merchant", true},
		{merchant, "ops", false},
		{merchant, "api", false},
		{ops, "ops", true},
		{ops, "merchant", false},
	} {
		if _, err := tc.c.actorFor("actor", tc.requested); (err == nil) != tc.ok {
			t.Errorf("%s asking for actor %q: err=%v, want ok=%v", tc.c.actor, tc.requested, err, tc.ok)
		}
	}

	for _, tc := range []struct {
		c         caller
		requested string
		ok        bool
	}{
		{merchant, "", true},
		{merchant, "api", true},
		{merchant, "dashboard", true},
		{merchant, "ops-tool", false},
		{merchant, "carrier-pigeon", false},
		{ops, "ops-tool", true},
		{ops, "carrier-pigeon", false},
	} {
		if _, err := tc.c.viaFor(tc.requested); (err == nil) != tc.ok {
			t.Errorf("%s asking for via %q: err=%v, want ok=%v", tc.c.actor, tc.requested, err, tc.ok)
		}
	}
}
