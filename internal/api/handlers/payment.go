package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"samarth/payment-service/internal/app/authz"
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type PaymentService interface {
	CreatePayment(ctx context.Context, in payment.CreatePaymentInput) (payment.CreateResult, error)
	ProcessPayment(ctx context.Context, transactionID uuid.UUID) (*transaction.Transaction, error)
	GetPayment(ctx context.Context, id uuid.UUID, scope authz.Scope) (*transaction.Transaction, error)
}

type PaymentHandler struct{ svc PaymentService }

func NewPaymentHandler(svc PaymentService) *PaymentHandler { return &PaymentHandler{svc: svc} }

type createPaymentRequest struct {
	MerchantID    string         `json:"merchant_id"`
	Amount        int64          `json:"amount"`
	Currency      string         `json:"currency"`
	PaymentMethod string         `json:"payment_method"`
	CustomerID    string         `json:"customer_id"`
	CustomerEmail string         `json:"customer_email"`
	Description   string         `json:"description"`
	Metadata      map[string]any `json:"metadata"`
}

type paymentResponse struct {
	ID                 string                  `json:"id"`
	Status             string                  `json:"status"`
	Amount             int64                   `json:"amount"`
	Currency           string                  `json:"currency"`
	PaymentMethod      string                  `json:"payment_method"`
	Gateway            string                  `json:"gateway"`
	GatewayReferenceID string                  `json:"gateway_reference_id,omitempty"`
	ReviewRequired     bool                    `json:"review_required,omitempty"`
	NextAction         *transaction.NextAction `json:"next_action,omitempty"`
	CreatedAt          string                  `json:"created_at"`
}

func (h *PaymentHandler) Create(w http.ResponseWriter, r *http.Request) {
	c, ok := requireCaller(w, r)
	if !ok {
		return
	}

	var req createPaymentRequest
	if !decodeJSON(w, r, maxPaymentBodyBytes, false, &req) {
		return
	}

	merchantID, err := c.merchantFor(req.MerchantID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_merchant_id", err.Error())
		return
	}
	if req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_amount", "amount must be positive")
		return
	}
	if req.Currency == "" || req.PaymentMethod == "" {
		writeError(w, http.StatusBadRequest, "missing_fields", "currency and payment_method are required")
		return
	}
	if err := transaction.ValidateMetadata(req.Metadata); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_metadata", err.Error())
		return
	}

	var customerID uuid.UUID
	if req.CustomerID != "" {
		if customerID, err = uuid.Parse(req.CustomerID); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_customer_id", "customer_id must be a valid UUID")
			return
		}
	}

	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		return
	}

	result, err := h.svc.CreatePayment(r.Context(), payment.CreatePaymentInput{
		Scope:          c.scope,
		MerchantID:     merchantID,
		Amount:         req.Amount,
		Currency:       req.Currency,
		PaymentMethod:  transaction.PaymentMethod(req.PaymentMethod),
		CustomerID:     customerID,
		CustomerEmail:  req.CustomerEmail,
		Description:    req.Description,
		Metadata:       req.Metadata,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		switch {
		case errors.Is(err, transaction.ErrMetadataTooLarge):
			writeError(w, http.StatusBadRequest, "invalid_metadata", err.Error())
		case errors.Is(err, authz.ErrDenied):
			writeError(w, http.StatusForbidden, "merchant_mismatch", "merchant_id does not match the authenticated merchant")
		case errors.Is(err, payment.ErrNoGateway):
			writeError(w, http.StatusUnprocessableEntity, "no_eligible_gateway", "no gateway can process this payment")
		case errors.Is(err, idempotency.ErrKeyRequired):
			writeError(w, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		default:
			writeError(w, http.StatusInternalServerError, "create_failed", "could not create payment")
		}
		return
	}

	switch result.Verdict {
	case idempotency.Created:
		processed, err := h.svc.ProcessPayment(r.Context(), result.Transaction.ID)
		if err != nil {
			writeJSON(w, http.StatusAccepted, toPaymentResponse(result.Transaction))
			return
		}
		writeJSON(w, http.StatusCreated, toPaymentResponse(processed))
	case idempotency.Replayed:
		writeJSON(w, http.StatusOK, toPaymentResponse(result.Transaction))
	case idempotency.InProgress:
		writeError(w, http.StatusConflict, "idempotency_in_progress", "a request with this idempotency key is already in progress")
	case idempotency.KeyReused:
		writeError(w, http.StatusConflict, "idempotency_key_reused", "idempotency key reused with a different request body")
	default:
		writeError(w, http.StatusInternalServerError, "create_failed", "could not create payment")
	}
}

func (h *PaymentHandler) Get(w http.ResponseWriter, r *http.Request) {
	c, ok := requireCaller(w, r)
	if !ok {
		return
	}

	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "payment id must be a valid UUID")
		return
	}

	txn, err := h.svc.GetPayment(r.Context(), id, c.scope)
	switch {
	case errors.Is(err, ports.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "payment not found")
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, "lookup_failed", "could not load payment")
		return
	}

	writeJSON(w, http.StatusOK, toPaymentResponse(txn))
}

func nextActionWhileAwaiting(t *transaction.Transaction) *transaction.NextAction {
	if !t.AwaitingCustomer() {
		return nil
	}
	return t.NextAction
}

func toPaymentResponse(t *transaction.Transaction) paymentResponse {
	return paymentResponse{
		ID:                 t.ID.String(),
		Status:             string(t.Status),
		Amount:             t.Amount,
		Currency:           t.Currency,
		PaymentMethod:      string(t.PaymentMethod),
		Gateway:            t.ProcessingGateway(),
		GatewayReferenceID: t.GatewayReferenceID,
		ReviewRequired:     t.Status == transaction.StatusProcessing && t.ReviewRequiredAt != nil,
		NextAction:         nextActionWhileAwaiting(t),
		CreatedAt:          t.CreatedAt.Format("2006-01-02T15:04:05.999999999Z07:00"),
	}
}
