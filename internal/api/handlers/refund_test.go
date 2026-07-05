package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"samarth/payment-service/internal/app/idempotency"
	apprefund "samarth/payment-service/internal/app/refund"
	domainrefund "samarth/payment-service/internal/domain/refund"
)

type fakeRefundService struct {
	initiated *domainrefund.Refund
	verdict   idempotency.Verdict
	processed *domainrefund.Refund
	initErr   error
	procErr   error
}

func (f *fakeRefundService) InitiateRefund(context.Context, apprefund.InitiateInput) (apprefund.InitiateResult, error) {
	if f.initErr != nil {
		return apprefund.InitiateResult{}, f.initErr
	}
	return apprefund.InitiateResult{Verdict: f.verdict, Refund: f.initiated}, nil
}
func (f *fakeRefundService) ProcessRefund(context.Context, uuid.UUID) (*domainrefund.Refund, error) {
	return f.processed, f.procErr
}

func sampleRefund(status domainrefund.Status) *domainrefund.Refund {
	rf, _ := domainrefund.New(uuid.New(), 40000, 100000, 0, "r", "by")
	rf.Status = status
	return rf
}

func postRefund(h *RefundHandler, id, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/payments/"+id+"/refunds", strings.NewReader(body))
	req.SetPathValue("id", id)
	req.Header.Set("Idempotency-Key", "refund-key")
	rec := httptest.NewRecorder()
	h.Create(rec, req)
	return rec
}

func TestRefundCreate_MissingIdempotencyKey400(t *testing.T) {
	h := NewRefundHandler(&fakeRefundService{initiated: sampleRefund(domainrefund.StatusInitiated)})
	req := httptest.NewRequest(http.MethodPost, "/payments/"+uuid.NewString()+"/refunds", strings.NewReader(`{"amount":40000,"reason":"r"}`))
	req.SetPathValue("id", uuid.NewString())
	rec := httptest.NewRecorder()
	h.Create(rec, req) // no Idempotency-Key
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 without Idempotency-Key, got %d", rec.Code)
	}
}

func TestRefundCreate_ReplayedReturns200(t *testing.T) {
	h := NewRefundHandler(&fakeRefundService{initiated: sampleRefund(domainrefund.StatusRefunded), verdict: idempotency.Replayed})
	rec := postRefund(h, uuid.NewString(), `{"amount":40000,"reason":"r"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("a replayed refund should be 200, got %d", rec.Code)
	}
}

func TestRefundCreate_InProgressReturns409(t *testing.T) {
	h := NewRefundHandler(&fakeRefundService{verdict: idempotency.InProgress})
	rec := postRefund(h, uuid.NewString(), `{"amount":40000,"reason":"r"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("an in-progress refund should be 409, got %d", rec.Code)
	}
}

func TestRefundCreate_Success(t *testing.T) {
	processed := sampleRefund(domainrefund.StatusRefunded)
	processed.GatewayRefundID = "re_1"
	h := NewRefundHandler(&fakeRefundService{initiated: sampleRefund(domainrefund.StatusInitiated), processed: processed})

	rec := postRefund(h, uuid.NewString(), `{"amount":40000,"reason":"customer_request"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", rec.Code, rec.Body.String())
	}
}

func TestRefundCreate_OverRefund422(t *testing.T) {
	h := NewRefundHandler(&fakeRefundService{initErr: domainrefund.ErrOverRefund{OriginalAmount: 100000, AlreadyRefunded: 60000, Requested: 60000}})
	rec := postRefund(h, uuid.NewString(), `{"amount":60000,"reason":"r"}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for over-refund, got %d", rec.Code)
	}
}

func TestRefundCreate_NotRefundable422(t *testing.T) {
	h := NewRefundHandler(&fakeRefundService{initErr: apprefund.ErrNotRefundable})
	rec := postRefund(h, uuid.NewString(), `{"amount":1000,"reason":"r"}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for not-refundable, got %d", rec.Code)
	}
}

func TestRefundCreate_Validation(t *testing.T) {
	h := NewRefundHandler(&fakeRefundService{})
	cases := []struct {
		id, body string
	}{
		{"not-a-uuid", `{"amount":100,"reason":"r"}`},
		{uuid.NewString(), `{"amount":0,"reason":"r"}`},
		{uuid.NewString(), `{"amount":100}`},
	}
	for _, c := range cases {
		if rec := postRefund(h, c.id, c.body); rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for %q, got %d", c.body, rec.Code)
		}
	}
}

func TestRefundCreate_ProcessFailureReturns202(t *testing.T) {
	h := NewRefundHandler(&fakeRefundService{initiated: sampleRefund(domainrefund.StatusInitiated), procErr: errors.New("gateway down")})
	rec := postRefund(h, uuid.NewString(), `{"amount":40000,"reason":"r"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 when processing fails after initiate, got %d", rec.Code)
	}
}
