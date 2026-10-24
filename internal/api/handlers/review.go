package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"samarth/payment-service/internal/api/middleware"
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

const maxReviewNoteLength = 500

type ReviewResolver interface {
	ResolveReview(ctx context.Context, transactionID uuid.UUID, in payment.ResolveInput) (*transaction.Transaction, error)
}

type ReviewHandler struct {
	svc ReviewResolver
	log ports.Logger
}

func NewReviewHandler(svc ReviewResolver, log ports.Logger) *ReviewHandler {
	return &ReviewHandler{svc: svc, log: log}
}

type resolveReviewRequest struct {
	Status             string `json:"status"`
	GatewayReferenceID string `json:"gateway_reference_id"`
	Note               string `json:"note"`
}

func (h *ReviewHandler) Resolve(w http.ResponseWriter, r *http.Request) {
	principal, ok := middleware.PrincipalFromContext(r.Context())
	if !ok || principal.Role != middleware.RoleOps {
		writeError(w, http.StatusForbidden, "forbidden", "resolving a review requires an ops token")
		return
	}

	transactionID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "payment id must be a valid UUID")
		return
	}

	var req resolveReviewRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "body must be JSON with status, gateway_reference_id and note")
		return
	}
	if len(req.Note) > maxReviewNoteLength {
		writeError(w, http.StatusBadRequest, "invalid_body", "note must be at most 500 characters")
		return
	}

	txn, err := h.svc.ResolveReview(r.Context(), transactionID, payment.ResolveInput{
		Status:             transaction.Status(req.Status),
		GatewayReferenceID: req.GatewayReferenceID,
		Note:               req.Note,
	})
	switch {
	case errors.Is(err, payment.ErrInvalidResolution):
		writeError(w, http.StatusBadRequest, "invalid_resolution", err.Error())
	case errors.Is(err, ports.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "payment not found")
	case errors.Is(err, payment.ErrNotUnderReview):
		writeError(w, http.StatusConflict, "not_under_review", "payment is not awaiting review")
	case err != nil:
		h.log.Error(ports.LogEventTransactionReviewResolved, map[string]any{
			ports.FieldErrorCode:     "review_resolution_failed",
			ports.FieldTraceID:       "",
			ports.FieldTransactionID: transactionID.String(),
		}, err)
		writeError(w, http.StatusInternalServerError, "internal_error", "could not resolve the review")
	default:
		writeJSON(w, http.StatusOK, toPaymentResponse(txn))
	}
}
