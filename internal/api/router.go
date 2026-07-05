package api

import (
	"net/http"

	"samarth/payment-service/internal/api/handlers"
	"samarth/payment-service/internal/api/middleware"
	"samarth/payment-service/internal/ports"
)

type Deps struct {
	Payment     *handlers.PaymentHandler
	Refund      *handlers.RefundHandler
	Cancel      *handlers.CancelHandler
	Webhook     *handlers.WebhookHandler
	Health      *handlers.HealthHandler
	Logger      ports.Logger
	Auth          middleware.TokenProvider
	Limiter       middleware.Limiter
	RateLimit     middleware.RateLimitConfig
	ResponseCache middleware.ResponseCacheStore
}

func NewRouter(deps Deps) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", deps.Health.Health)
	mux.HandleFunc("POST /payments", deps.Payment.Create)
	mux.HandleFunc("GET /payments/{id}", deps.Payment.Get)
	mux.HandleFunc("POST /payments/{id}/refunds", deps.Refund.Create)
	mux.HandleFunc("POST /payments/{id}/cancel", deps.Cancel.Cancel)
	if deps.Webhook != nil {
		mux.HandleFunc("POST /webhooks/gateway/{gateway_id}", deps.Webhook.Handle)
	}

	chain := []func(http.Handler) http.Handler{
		middleware.RequestID,
		middleware.Recover(deps.Logger),
	}
	if deps.Auth != nil {
		chain = append(chain, middleware.Authenticate(deps.Auth, deps.Logger))
	}
	if deps.Limiter != nil {
		chain = append(chain, middleware.RateLimit(deps.Limiter, deps.RateLimit, deps.Logger))
	}
	if deps.ResponseCache != nil {
		chain = append(chain, middleware.ResponseCache(deps.ResponseCache, deps.Logger))
	}

	return middleware.Chain(mux, chain...)
}
