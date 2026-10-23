package api

import (
	"net/http"

	"samarth/payment-service/internal/api/handlers"
	"samarth/payment-service/internal/api/middleware"
	"samarth/payment-service/internal/ports"
)

type Deps struct {
	Payment      *handlers.PaymentHandler
	Refund       *handlers.RefundHandler
	Cancel       *handlers.CancelHandler
	Review       *handlers.ReviewHandler
	Webhook      *handlers.WebhookHandler
	Health       *handlers.HealthHandler
	Logger       ports.Logger
	Metrics      ports.MetricRecorder
	Auth         middleware.TokenProvider
	AllowNoAuth  bool
	Limiter      middleware.Limiter
	RateLimits   middleware.Limits
	WebhookLimit middleware.Limit
	AuthFailures middleware.Limit
	ClientIP     *middleware.ClientIP
}

func NewMerchantRouter(deps Deps) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", deps.Payment.Create)
	mux.HandleFunc("GET /payments/{id}", deps.Payment.Get)
	mux.HandleFunc("POST /payments/{id}/refunds", deps.Refund.Create)
	mux.HandleFunc("POST /payments/{id}/cancel", deps.Cancel.Cancel)
	if deps.Review != nil {
		mux.HandleFunc("POST /payments/{id}/resolve", deps.Review.Resolve)
	}

	chain := []func(http.Handler) http.Handler{
		middleware.RequestID,
		middleware.Recover(deps.Logger),
	}
	if deps.Auth != nil {
		var throttle *middleware.AuthThrottle
		if deps.Limiter != nil && deps.AuthFailures.Capacity > 0 {
			throttle = &middleware.AuthThrottle{
				Limiter:  deps.Limiter,
				ClientIP: deps.ClientIP,
				Limit:    deps.AuthFailures,
			}
		}
		chain = append(chain, middleware.Authenticate(deps.Auth, deps.Logger, deps.Metrics, throttle))
	} else if deps.AllowNoAuth {
		chain = append(chain, middleware.AllowAnonymous())
	}
	if deps.Limiter != nil {
		chain = append(chain, middleware.RateLimit(deps.Limiter, deps.Logger, deps.Metrics, middleware.RequestKeys(deps.ClientIP, deps.RateLimits)))
	}
	return middleware.Chain(mux, chain...)
}

func NewWebhookRouter(deps Deps) http.Handler {
	mux := http.NewServeMux()
	if deps.Webhook != nil {
		mux.HandleFunc("POST /webhooks/gateway/{gateway_id}", deps.Webhook.Handle)
	}

	chain := []func(http.Handler) http.Handler{
		middleware.RequestID,
		middleware.Recover(deps.Logger),
	}
	if deps.Limiter != nil {
		chain = append(chain, middleware.RateLimit(deps.Limiter, deps.Logger, deps.Metrics, middleware.WebhookKeys(deps.ClientIP, deps.WebhookLimit)))
	}
	return middleware.Chain(mux, chain...)
}

func NewHealthRouter(deps Deps) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", deps.Health.Liveness)
	mux.HandleFunc("GET /health-db", deps.Health.Database)
	mux.HandleFunc("GET /health-cache", deps.Health.Cache)
	mux.HandleFunc("GET /health-overall", deps.Health.Overall)
	mux.HandleFunc("GET /health-gateways", deps.Health.Gateways)
	return middleware.Chain(mux, middleware.Recover(deps.Logger))
}
