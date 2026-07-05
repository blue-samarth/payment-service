package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	appcancel "samarth/payment-service/internal/app/cancel"
	"samarth/payment-service/internal/domain/transaction"
)

type fakeCancelService struct {
	result appcancel.Result
}

func (f *fakeCancelService) Cancel(context.Context, appcancel.CancelInput) (appcancel.Result, error) {
	return f.result, nil
}

func postCancel(h *CancelHandler, id, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/payments/"+id+"/cancel", strings.NewReader(body))
	req.SetPathValue("id", id)
	req.Header.Set("Idempotency-Key", "cancel-key")
	rec := httptest.NewRecorder()
	h.Cancel(rec, req)
	return rec
}

func TestCancel_Requested(t *testing.T) {
	h := NewCancelHandler(&fakeCancelService{result: appcancel.Result{Outcome: appcancel.OutcomeRequested, Status: transaction.StatusProcessing}})
	rec := postCancel(h, uuid.NewString(), `{"actor":"merchant","via":"api"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "CANCEL_REQUESTED") {
		t.Errorf("expected CANCEL_REQUESTED in body, got %s", rec.Body.String())
	}
}

func TestCancel_EmptyBodyDefaults(t *testing.T) {
	h := NewCancelHandler(&fakeCancelService{result: appcancel.Result{Outcome: appcancel.OutcomeRequested, Status: transaction.StatusPending}})
	rec := postCancel(h, uuid.NewString(), "")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with empty body (defaults applied), got %d", rec.Code)
	}
}

func TestCancel_InvalidID(t *testing.T) {
	h := NewCancelHandler(&fakeCancelService{})
	rec := postCancel(h, "bad", `{}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
