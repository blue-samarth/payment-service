package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/api/middleware"
	"samarth/payment-service/internal/app/authz"
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type fakeService struct {
	created   *transaction.Transaction
	verdict   idempotency.Verdict
	processed *transaction.Transaction
	fetched   *transaction.Transaction
	createErr error
	procErr   error
	getErr    error
}

func (f *fakeService) CreatePayment(ctx context.Context, in payment.CreatePaymentInput) (payment.CreateResult, error) {
	if f.createErr != nil {
		return payment.CreateResult{}, f.createErr
	}
	return payment.CreateResult{Verdict: f.verdict, Transaction: f.created}, nil
}
func (f *fakeService) ProcessPayment(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error) {
	return f.processed, f.procErr
}
func (f *fakeService) GetPayment(ctx context.Context, id uuid.UUID, scope authz.Scope) (*transaction.Transaction, error) {
	return f.fetched, f.getErr
}

func anonymous(req *http.Request) *http.Request {
	return req.WithContext(middleware.WithPrincipal(req.Context(), middleware.Principal{Role: middleware.RoleAnonymous}))
}

func sampleTxn(status transaction.Status) *transaction.Transaction {
	t, _ := transaction.New(uuid.New(), 150000, "INR", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@e.com", "order", nil, 30)
	t.Status = status
	return t
}

func validBody() string {
	return `{"merchant_id":"` + uuid.NewString() + `","amount":150000,"currency":"INR","payment_method":"card","merchant_tier":"standard","is_domestic":true}`
}

func doRequest(h *PaymentHandler, method, target, body string) *httptest.ResponseRecorder {
	req := anonymous(httptest.NewRequest(method, target, strings.NewReader(body)))
	rec := httptest.NewRecorder()
	switch {
	case method == http.MethodPost:
		req.Header.Set("Idempotency-Key", "test-key")
		h.Create(rec, req)
	default:
		h.Get(rec, req)
	}
	return rec
}

func TestCreate_Success(t *testing.T) {
	created := sampleTxn(transaction.StatusPending)
	processed := sampleTxn(transaction.StatusSucceeded)
	processed.ID = created.ID
	processed.GatewayReferenceID = "pi_1"

	h := NewPaymentHandler(&fakeService{created: created, processed: processed})
	rec := doRequest(h, http.MethodPost, "/payments", validBody())

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", rec.Code, rec.Body.String())
	}
	var resp paymentResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != string(transaction.StatusSucceeded) {
		t.Errorf("expected SUCCEEDED, got %s", resp.Status)
	}
	if resp.GatewayReferenceID != "pi_1" {
		t.Errorf("expected gateway reference in response, got %q", resp.GatewayReferenceID)
	}
}

func TestCreate_NoGatewayReturns422(t *testing.T) {
	h := NewPaymentHandler(&fakeService{createErr: payment.ErrNoGateway})
	rec := doRequest(h, http.MethodPost, "/payments", validBody())
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
}

func TestCreate_MissingIdempotencyKey400(t *testing.T) {
	h := NewPaymentHandler(&fakeService{created: sampleTxn(transaction.StatusPending)})
	req := anonymous(httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(validBody())))
	rec := httptest.NewRecorder()
	h.Create(rec, req) // no Idempotency-Key header
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without Idempotency-Key, got %d", rec.Code)
	}
}

func TestCreate_ReplayedReturns200(t *testing.T) {
	replayed := sampleTxn(transaction.StatusSucceeded)
	h := NewPaymentHandler(&fakeService{created: replayed, verdict: idempotency.Replayed})
	rec := doRequest(h, http.MethodPost, "/payments", validBody())
	if rec.Code != http.StatusOK {
		t.Fatalf("a replayed idempotent create should be 200, got %d", rec.Code)
	}
}

func TestCreate_InProgressReturns409(t *testing.T) {
	h := NewPaymentHandler(&fakeService{verdict: idempotency.InProgress})
	rec := doRequest(h, http.MethodPost, "/payments", validBody())
	if rec.Code != http.StatusConflict {
		t.Fatalf("an in-progress idempotent create should be 409, got %d", rec.Code)
	}
}

func TestCreate_KeyReusedReturns409(t *testing.T) {
	h := NewPaymentHandler(&fakeService{verdict: idempotency.KeyReused})
	rec := doRequest(h, http.MethodPost, "/payments", validBody())
	if rec.Code != http.StatusConflict {
		t.Fatalf("a reused idempotency key should be 409, got %d", rec.Code)
	}
}

func TestCreate_InvalidJSON(t *testing.T) {
	h := NewPaymentHandler(&fakeService{})
	rec := doRequest(h, http.MethodPost, "/payments", "{not json")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestCreate_ValidationErrors(t *testing.T) {
	h := NewPaymentHandler(&fakeService{})
	cases := []string{
		`{"merchant_id":"not-a-uuid","amount":100,"currency":"INR","payment_method":"card"}`,
		`{"merchant_id":"` + uuid.NewString() + `","amount":0,"currency":"INR","payment_method":"card"}`,
		`{"merchant_id":"` + uuid.NewString() + `","amount":100,"payment_method":"card"}`,
	}
	for _, body := range cases {
		rec := doRequest(h, http.MethodPost, "/payments", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for %q, got %d", body, rec.Code)
		}
	}
}

func TestCreate_ProcessFailureReturns202(t *testing.T) {
	created := sampleTxn(transaction.StatusPending)
	h := NewPaymentHandler(&fakeService{created: created, procErr: errors.New("gateway down")})
	rec := doRequest(h, http.MethodPost, "/payments", validBody())
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 when processing fails after create, got %d", rec.Code)
	}
}

func TestGet_Success(t *testing.T) {
	txn := sampleTxn(transaction.StatusSucceeded)
	h := NewPaymentHandler(&fakeService{fetched: txn})
	req := anonymous(httptest.NewRequest(http.MethodGet, "/payments/"+txn.ID.String(), nil))
	req.SetPathValue("id", txn.ID.String())
	rec := httptest.NewRecorder()
	h.Get(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestGet_InvalidID(t *testing.T) {
	h := NewPaymentHandler(&fakeService{})
	req := anonymous(httptest.NewRequest(http.MethodGet, "/payments/bad", nil))
	req.SetPathValue("id", "bad")
	rec := httptest.NewRecorder()
	h.Get(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGet_ErrorMapping(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want int
	}{
		"missing":       {fmt.Errorf("transaction %w", ports.ErrNotFound), http.StatusNotFound},
		"out of scope":  {authz.ErrDenied, http.StatusNotFound},
		"store failure": {errors.New("connection refused"), http.StatusInternalServerError},
	} {
		h := NewPaymentHandler(&fakeService{getErr: tc.err})
		id := uuid.NewString()
		req := anonymous(httptest.NewRequest(http.MethodGet, "/payments/"+id, nil))
		req.SetPathValue("id", id)
		rec := httptest.NewRecorder()
		h.Get(rec, req)
		if rec.Code != tc.want {
			t.Errorf("%s: expected %d, got %d", name, tc.want, rec.Code)
		}
	}
}

func TestCreate_OutOfScopeMerchantIs403(t *testing.T) {
	h := NewPaymentHandler(&fakeService{createErr: authz.ErrDenied})
	rec := doRequest(h, http.MethodPost, "/payments", validBody())
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "merchant_mismatch") {
		t.Fatalf("expected 403 merchant_mismatch, got %d %s", rec.Code, rec.Body.String())
	}
}
