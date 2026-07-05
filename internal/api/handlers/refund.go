package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"samarth/payment-service/internal/app/idempotency"
	apprefund "samarth/payment-service/internal/app/refund"
	domainrefund "samarth/payment-service/internal/domain/refund"
)

type RefundService interface {
	InitiateRefund(ctx context.Context, in apprefund.InitiateInput) (apprefund.InitiateResult, error)
	ProcessRefund(ctx context.Context, refundID uuid.UUID) (*domainrefund.Refund, error)
}

type RefundHandler struct{ svc RefundService }

func NewRefundHandler(svc RefundService) *RefundHandler { return &RefundHandler{svc: svc} }

type createRefundRequest struct {
	Amount      int64  `json:"amount"`
	Reason      string `json:"reason"`
	InitiatedBy string `json:"initiated_by"`
}

type refundResponse struct {
	ID              string `json:"id"`
	TransactionID   string `json:"transaction_id"`
	Status          string `json:"status"`
	Amount          int64  `json:"amount"`
	GatewayRefundID string `json:"gateway_refund_id,omitempty"`
}

func (h *RefundHandler) Create(w http.ResponseWriter, r *http.Request) {
	transactionID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "payment id must be a valid UUID")
		return
	}

	var req createRefundRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_body", "request body is not valid JSON")
		return
	}
	if req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_amount", "amount must be positive")
		return
	}
	if req.Reason == "" {
		writeError(w, http.StatusBadRequest, "missing_reason", "reason is required")
		return
	}
	if req.InitiatedBy == "" {
		req.InitiatedBy = "api"
	}

	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		return
	}

	result, err := h.svc.InitiateRefund(r.Context(), apprefund.InitiateInput{
		TransactionID:  transactionID,
		Amount:         req.Amount,
		Reason:         req.Reason,
		InitiatedBy:    req.InitiatedBy,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		var over domainrefund.ErrOverRefund
		switch {
		case errors.Is(err, apprefund.ErrNotRefundable):
			writeError(w, http.StatusUnprocessableEntity, "not_refundable", "transaction is not in a refundable state")
		case errors.As(err, &over):
			writeError(w, http.StatusUnprocessableEntity, "over_refund", over.Error())
		case errors.Is(err, idempotency.ErrKeyRequired):
			writeError(w, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		default:
			writeError(w, http.StatusInternalServerError, "refund_failed", "could not initiate refund")
		}
		return
	}

	switch result.Verdict {
	case idempotency.Created:
		processed, err := h.svc.ProcessRefund(r.Context(), result.Refund.ID)
		if err != nil {
			writeJSON(w, http.StatusAccepted, toRefundResponse(result.Refund))
			return
		}
		writeJSON(w, http.StatusCreated, toRefundResponse(processed))
	case idempotency.Replayed:
		writeJSON(w, http.StatusOK, toRefundResponse(result.Refund))
	case idempotency.InProgress:
		writeError(w, http.StatusConflict, "idempotency_in_progress", "a request with this idempotency key is already in progress")
	case idempotency.KeyReused:
		writeError(w, http.StatusConflict, "idempotency_key_reused", "idempotency key reused with a different request body")
	default:
		writeError(w, http.StatusInternalServerError, "refund_failed", "could not initiate refund")
	}
}

func toRefundResponse(rf *domainrefund.Refund) refundResponse {
	return refundResponse{
		ID:              rf.ID.String(),
		TransactionID:   rf.TransactionID.String(),
		Status:          string(rf.Status),
		Amount:          rf.Amount,
		GatewayRefundID: rf.GatewayRefundID,
	}
}
