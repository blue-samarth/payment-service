package handlers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"

	"samarth/payment-service/internal/api/middleware"
	"samarth/payment-service/internal/domain/transaction"
)

var (
	errMerchantRequired = errors.New("merchant_id is required")
	errMerchantInvalid  = errors.New("merchant_id must be a valid UUID")
	errMerchantMismatch = errors.New("merchant_id does not match the authenticated merchant")
)

type caller struct {
	merchant uuid.UUID
	actor    transaction.Actor
}

func requireCaller(w http.ResponseWriter, r *http.Request) (caller, bool) {
	principal, ok := middleware.PrincipalFromContext(r.Context())
	if !ok {
		return caller{actor: transaction.ActorMerchant}, true
	}
	switch principal.Role {
	case middleware.RoleOps:
		return caller{actor: transaction.ActorOps}, true
	case middleware.RoleService:
		id, err := uuid.Parse(principal.MerchantID)
		if err == nil && id != uuid.Nil {
			return caller{merchant: id, actor: transaction.ActorMerchant}, true
		}
	}
	writeError(w, http.StatusForbidden, "forbidden", "the token is not bound to a merchant")
	return caller{}, false
}

func (c caller) owns(t *transaction.Transaction) bool {
	return c.merchant == uuid.Nil || t.MerchantID == c.merchant
}

func (c caller) merchantFor(requested string) (uuid.UUID, error) {
	if requested == "" {
		if c.merchant == uuid.Nil {
			return uuid.Nil, errMerchantRequired
		}
		return c.merchant, nil
	}
	id, err := uuid.Parse(requested)
	if err != nil {
		return uuid.Nil, errMerchantInvalid
	}
	if c.merchant != uuid.Nil && id != c.merchant {
		return uuid.Nil, errMerchantMismatch
	}
	return id, nil
}

func (c caller) actorFor(field, requested string) (transaction.Actor, error) {
	if requested == "" || transaction.Actor(requested) == c.actor {
		return c.actor, nil
	}
	return "", fmt.Errorf("%s must be %q for this token, or omitted", field, c.actor)
}

func (c caller) viaFor(requested string) (transaction.CancelVia, error) {
	via := transaction.CancelVia(requested)
	switch {
	case via == "":
		return transaction.CancelViaAPI, nil
	case !via.Valid():
		return "", fmt.Errorf("via must be one of %q, %q or %q", transaction.CancelViaAPI, transaction.CancelViaDashboard, transaction.CancelViaOpsTool)
	case via == transaction.CancelViaOpsTool && c.actor != transaction.ActorOps:
		return "", fmt.Errorf("via %q requires an ops token", transaction.CancelViaOpsTool)
	}
	return via, nil
}
