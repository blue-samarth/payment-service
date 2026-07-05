package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	appcancel "samarth/payment-service/internal/app/cancel"
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/domain/transaction"
)

type CancelService interface {
	Cancel(ctx context.Context, in appcancel.CancelInput) (appcancel.Result, error)
}
type CancelHandler struct{ svc CancelService }

func NewCancelHandler(svc CancelService) *CancelHandler { return &CancelHandler{svc: svc} }

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

	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		return
	}

	res, err := h.svc.Cancel(r.Context(), appcancel.CancelInput{
		TransactionID:  transactionID,
		By:             actor,
		Via:            via,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		if errors.Is(err, idempotency.ErrKeyRequired) {
			writeError(w, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
			return
		}
		writeError(w, http.StatusInternalServerError, "cancel_failed", "could not process cancellation")
		return
	}

	switch res.Verdict {
	case idempotency.InProgress:
		writeError(w, http.StatusConflict, "idempotency_in_progress", "a request with this idempotency key is already in progress")
	case idempotency.KeyReused:
		writeError(w, http.StatusConflict, "idempotency_key_reused", "idempotency key reused with a different request body")
	default:
		writeJSON(w, http.StatusOK, cancelResponse{
			TransactionID: transactionID.String(),
			Outcome:       string(res.Outcome),
			Status:        string(res.Status),
		})
	}
}
