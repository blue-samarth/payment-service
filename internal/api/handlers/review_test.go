package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/api/middleware"
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type fakeReviewResolver struct {
	err   error
	calls int
	got   payment.ResolveInput
}

func (f *fakeReviewResolver) ResolveReview(_ context.Context, id uuid.UUID, in payment.ResolveInput) (*transaction.Transaction, error) {
	f.calls++
	f.got = in
	if f.err != nil {
		return nil, f.err
	}
	t, _ := transaction.New(uuid.New(), 1000, "INR", transaction.PaymentMethodCard, "razorpay", uuid.New(), "", "", nil, 30)
	t.ID = id
	t.Status = in.Status
	t.GatewayReferenceID = in.GatewayReferenceID
	return t, nil
}

var (
	opsToken     = "ops-" + strings.Repeat("o", 32)
	serviceToken = "svc-" + strings.Repeat("s", 32)
)

func resolveRequest(t *testing.T, resolver *fakeReviewResolver, header, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	provider := middleware.NewStaticTokenProvider(map[string]string{serviceToken: uuid.NewString()}, []string{opsToken})
	h := middleware.Authenticate(provider, noopLog{}, nil, nil)(http.HandlerFunc(NewReviewHandler(resolver, noopLog{}).Resolve))

	id := uuid.New()
	req := httptest.NewRequest(http.MethodPost, "/payments/"+id.String()+"/resolve", strings.NewReader(body))
	req.SetPathValue("id", id.String())
	if header != "" {
		req.Header.Set(header, token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestResolve_OpsTokenSettlesTheReview(t *testing.T) {
	resolver := &fakeReviewResolver{}
	rec := resolveRequest(t, resolver, "X-Ops-Token", opsToken, `{"status":"SUCCEEDED","gateway_reference_id":"pay_1","note":"captured in dashboard"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body["status"] != "SUCCEEDED" || body["gateway_reference_id"] != "pay_1" {
		t.Errorf("unexpected body %v", body)
	}
	if resolver.got.Note != "captured in dashboard" {
		t.Errorf("note not passed through: %+v", resolver.got)
	}
}

func TestResolve_RequiresAnOpsToken(t *testing.T) {
	resolver := &fakeReviewResolver{}
	if rec := resolveRequest(t, resolver, "X-Service-Token", serviceToken, `{"status":"FAILED"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("a merchant service token must not resolve reviews, got %d", rec.Code)
	}
	if rec := resolveRequest(t, resolver, "", "", `{"status":"FAILED"}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token must be rejected by auth, got %d", rec.Code)
	}
	if resolver.calls != 0 {
		t.Error("the service must not be called without an ops principal")
	}
}

func TestResolve_WithoutAuthMiddlewareIsForbidden(t *testing.T) {
	resolver := &fakeReviewResolver{}
	id := uuid.New()
	req := httptest.NewRequest(http.MethodPost, "/payments/"+id.String()+"/resolve", strings.NewReader(`{"status":"FAILED"}`))
	req.SetPathValue("id", id.String())
	rec := httptest.NewRecorder()
	NewReviewHandler(resolver, noopLog{}).Resolve(rec, req)

	if rec.Code != http.StatusForbidden || resolver.calls != 0 {
		t.Fatalf("with ALLOW_NO_AUTH there is no principal, so resolve must fail closed, got %d calls=%d", rec.Code, resolver.calls)
	}
}

func TestResolve_ErrorMapping(t *testing.T) {
	cases := map[string]struct {
		err  error
		body string
		want int
	}{
		"invalid resolution": {fmt.Errorf("%w: x", payment.ErrInvalidResolution), `{"status":"PENDING"}`, http.StatusBadRequest},
		"not under review":   {payment.ErrNotUnderReview, `{"status":"FAILED"}`, http.StatusConflict},
		"missing":            {fmt.Errorf("payment: load transaction: %w", ports.ErrNotFound), `{"status":"FAILED"}`, http.StatusNotFound},
		"internal":           {fmt.Errorf("db down"), `{"status":"FAILED"}`, http.StatusInternalServerError},
		"bad json":           {nil, `{`, http.StatusBadRequest},
		"note too long":      {nil, `{"status":"FAILED","note":"` + strings.Repeat("n", maxReviewNoteLength+1) + `"}`, http.StatusBadRequest},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := resolveRequest(t, &fakeReviewResolver{err: tc.err}, "X-Ops-Token", opsToken, tc.body)
			if rec.Code != tc.want {
				t.Fatalf("got %d %s, want %d", rec.Code, rec.Body.String(), tc.want)
			}
		})
	}
}

func TestPaymentResponse_FlagsReviewOnlyWhileProcessing(t *testing.T) {
	txn, _ := transaction.New(uuid.New(), 1000, "INR", transaction.PaymentMethodCard, "razorpay", uuid.New(), "", "", nil, 30)
	marked := time.Now()
	txn.ReviewRequiredAt = &marked

	txn.Status = transaction.StatusProcessing
	if !toPaymentResponse(txn).ReviewRequired {
		t.Error("a PROCESSING payment under review must say so")
	}
	txn.Status = transaction.StatusSucceeded
	if toPaymentResponse(txn).ReviewRequired {
		t.Error("a resolved payment is no longer under review")
	}
}
