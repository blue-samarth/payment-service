package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	appcancel "samarth/payment-service/internal/app/cancel"
	"samarth/payment-service/internal/domain/transaction"
)

type CancelService interface {
	Cancel(ctx context.Context, in appcancel.CancelInput) (appcancel.Result, error)
}

type CancelHandler struct {
	svc CancelService
}

func NewCancelHandler(svc CancelService) *CancelHandler {
	return &CancelHandler{svc: svc}
}

type cancelRequest struct {
	Actor string `json:"actor"`
	Via   string `json:"via"`
}

type cancelResponse struct {
	TransactionID string `json:"transaction_id"`
	Outcome       string `json:"outcome"`
	Status        string `json:"status"`
}

func (h *CancelHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	transactionID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "payment id must be a valid UUID")
		return
	}

	var req cancelRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	actor := transaction.Actor(req.Actor)
	if actor == "" {
		actor = transaction.ActorMerchant
	}
	via := transaction.CancelVia(req.Via)
	if via == "" {
		via = transaction.CancelViaAPI
	}

	res, err := h.svc.Cancel(r.Context(), appcancel.CancelInput{
		TransactionID: transactionID,
		By:            actor,
		Via:           via,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cancel_failed", "could not process cancellation")
		return
	}

	writeJSON(w, http.StatusOK, cancelResponse{
		TransactionID: transactionID.String(),
		Outcome:       string(res.Outcome),
		Status:        string(res.Status),
	})
}
