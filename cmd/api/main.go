package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/adapters/postgres"
	"samarth/payment-service/internal/adapters/redis"
	"samarth/payment-service/internal/adapters/security"
	"samarth/payment-service/internal/api"
	"samarth/payment-service/internal/api/handlers"
	"samarth/payment-service/internal/api/middleware"
	"samarth/payment-service/internal/app/cancel"
	"samarth/payment-service/internal/app/gatewaystatus"
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/app/refund"
	approuting "samarth/payment-service/internal/app/routing"
	"samarth/payment-service/internal/app/webhook"
	"samarth/payment-service/internal/bootstrap"
	"samarth/payment-service/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := observability.NewSlogLogger(bootstrap.ParseLogLevel(cfg.Observability.LogLevel))
	bootstrap.LogDeprecations(logger, cfg.Deprecations)

	ctx := context.Background()

	metrics, metricsClose, err := observability.NewMetrics(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsClose(flushCtx)
	}()

	connectPolicy := bootstrap.RetryPolicy{
		Attempts:       cfg.Startup.ConnectMaxAttempts,
		AttemptTimeout: cfg.Startup.ConnectAttemptTimeout,
		Backoff:        cfg.Startup.ConnectBackoff,
	}

	var db *postgres.DB
	if err := bootstrap.Connect(ctx, logger, "postgres", connectPolicy, func(c context.Context) error {
		db, err = postgres.New(c, cfg.Database)
		return err
	}); err != nil {
		return err
	}
	defer db.Close()

	queries, err := postgres.LoadQueries()
	if err != nil {
		return fmt.Errorf("load queries: %w", err)
	}

	txnRepo := postgres.NewTransactionRepository(db, queries)
	refundRepo := postgres.NewRefundRepository(db, queries)
	outboxWriter := postgres.NewOutboxWriter(db, queries)
	leaseRepo := postgres.NewLeaseRepository(db, queries)
	transactor := postgres.NewTransactor(db)
	configStore := postgres.NewConfigStore(db, queries)

	registry := bootstrap.GatewayRegistry(cfg)

	router := approuting.NewRouter(configStore)

	paymentSvc := payment.NewService(
		txnRepo,
		outboxWriter,
		router,
		configStore,
		transactor,
		leaseRepo,
		registry,
		logger,
		metrics,
	)

	refundSvc := refund.NewService(txnRepo, refundRepo, outboxWriter, transactor, registry, logger, metrics)
	cancelSvc := cancel.NewService(txnRepo, logger, metrics)
	paymentSvc.SetCancelResolver(refundSvc)

	webhookRepo := postgres.NewWebhookRepository(db, queries)
	webhookSvc := webhook.NewService(txnRepo, webhookRepo, outboxWriter, transactor, logger, metrics)
	webhookSecrets := webhook.NewStaticSecretProvider(cfg.Auth.WebhookSecrets)

	if !cfg.Redis.TLSEnabled {
		logger.Warn("redis.tls_disabled", map[string]any{
			"warning": "REDIS_TLS_ENABLED=false: Redis traffic, including credentials, is sent in plaintext",
		})
	}
	redisClient, err := redis.New(cfg.Redis)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer redisClient.Close()
	if err := bootstrap.Connect(ctx, logger, "redis", connectPolicy, redisClient.Ping); err != nil {
		return err
	}
	rateLimiter := redis.NewRateLimiter(redisClient, cfg.RateLimit, logger, metrics)
	defer rateLimiter.Close()
	idempotencyRepo := postgres.NewIdempotencyRepository(db, queries)

	idemGuard := idempotency.NewGuard(idempotencyRepo, transactor)
	paymentSvc.SetIdempotency(idemGuard)
	refundSvc.SetIdempotency(idemGuard)
	cancelSvc.SetIdempotency(idemGuard)

	cbStore := redis.NewCircuitBreakerStore(redisClient)
	paymentSvc.SetCircuitBreaker(circuitBreakerAdapter{
		store:     cbStore,
		threshold: cfg.Gateway.CircuitBreakerFailureThreshold,
	})
	router.SetBreakerState(breakerStateAdapter{store: cbStore})
	router.SetProbeReader(cbStore)
	paymentSvc.SetCallGate(cbStore)
	paymentSvc.SetMaxGatewayAttempts(cfg.Gateway.MaxAttempts)
	paymentSvc.SetGatewayHTTPTimeout(cfg.Gateway.HTTPTimeout)

	intentStore := redis.NewIntentStore(redisClient)
	router.SetActiveIntents(intentStore)
	paymentSvc.SetIntentTracker(intentStore)

	var authProvider middleware.TokenProvider
	tokenProvider := middleware.NewStaticTokenProvider(cfg.Auth.ServiceTokens, cfg.Auth.OpsTokens)
	if tokenProvider.HasAny() {
		authProvider = tokenProvider
		logger.Info("auth.enabled", nil)
	} else if cfg.App.AllowNoAuth {
		logger.Warn("auth.disabled_allow_no_auth", map[string]any{
			"warning": "ALLOW_NO_AUTH=true: the API is serving unauthenticated requests",
		})
	} else {
		return fmt.Errorf("no auth tokens configured: set SERVICE_TOKENS or OPS_TOKENS, or set ALLOW_NO_AUTH=true to explicitly run without authentication")
	}

	refreshCtx, cancelRefresh := context.WithCancel(context.Background())
	defer cancelRefresh()
	refreshInterval := time.Duration(cfg.Security.CertRefreshIntervalSec) * time.Second

	merchantTransport := "mtls"
	if !cfg.App.MtlsStrictMode {
		merchantTransport = "mtls_optional"
		logger.Warn("mtls.optional_mode", map[string]any{
			"warning": "MTLS_STRICT_MODE=false: client certificates are requested but not enforced",
		})
	}
	merchantMgr, err := security.NewManager(security.Config{
		Name:            "merchant",
		CertFile:        cfg.Security.TLSCertFile,
		KeyFile:         cfg.Security.TLSKeyFile,
		CAFile:          cfg.Security.TLSCAFile,
		OptionalMTLS:    !cfg.App.MtlsStrictMode,
		RefreshInterval: refreshInterval,
	}, logger, metrics)
	if err != nil {
		return fmt.Errorf("configure merchant mTLS: %w", err)
	}
	merchantMgr.StartRefresh(refreshCtx)
	merchantTLS := merchantMgr.TLSConfig()
	certs := []handlers.CertificateChecker{merchantMgr}

	webhookTransport := "plain_lb_terminated"
	var webhookTLS *tls.Config
	if cfg.App.WebhookTLSMode == config.WebhookTLSModeApp {
		webhookTransport = "tls"
		mgr, err := security.NewManager(security.Config{
			Name:            "webhook",
			CertFile:        cfg.Security.WebhookTLSCertFile,
			KeyFile:         cfg.Security.WebhookTLSKeyFile,
			RefreshInterval: refreshInterval,
			MinVersion:      tls.VersionTLS12,
		}, logger, metrics)
		if err != nil {
			return fmt.Errorf("configure webhook TLS: %w", err)
		}
		mgr.StartRefresh(refreshCtx)
		webhookTLS = mgr.TLSConfig()
		certs = append(certs, mgr)
	} else {
		logger.Warn("webhook.tls_terminated_upstream", map[string]any{
			"warning": "WEBHOOK_TLS_MODE=lb: the webhook listener serves plain HTTP and relies on the load balancer to terminate TLS",
		})
		if len(cfg.RateLimit.TrustedProxies) == 0 {
			logger.Warn("rate_limit.proxy_untrusted", map[string]any{
				"warning": "WEBHOOK_TLS_MODE=lb with empty TRUSTED_PROXY_CIDRS: the load balancer terminating TLS is the TCP peer of every request, so all callers share its rate-limit bucket. Set TRUSTED_PROXY_CIDRS to the load balancer's subnets and use one that sets X-Forwarded-For",
			})
		}
	}

	health := handlers.NewHealthHandler(
		db,
		redisClient,
		gatewaystatus.NewService(registry, configStore, router, cbStore, logger),
		logger,
		certs...,
	)
	deps := api.Deps{
		Payment:     handlers.NewPaymentHandler(paymentSvc),
		Refund:      handlers.NewRefundHandler(refundSvc),
		Cancel:      handlers.NewCancelHandler(cancelSvc),
		Review:      handlers.NewReviewHandler(paymentSvc, logger),
		Webhook:     handlers.NewWebhookHandler(webhookSvc, registry, webhookSecrets, configStore, logger),
		Health:      health,
		Logger:      logger,
		Auth:        authProvider,
		AllowNoAuth: authProvider == nil && cfg.App.AllowNoAuth,
		Limiter:     limiterAdapter{rl: rateLimiter},
		Metrics:     metrics,
		RateLimits: middleware.Limits{
			Merchant: middleware.Limit{Capacity: cfg.RateLimit.Capacity, RefillPerSec: cfg.RateLimit.RefillPerSec},
			User:     middleware.Limit{Capacity: cfg.RateLimit.UserCapacity, RefillPerSec: cfg.RateLimit.UserRefillPerSec},
			IP:       middleware.Limit{Capacity: cfg.RateLimit.IPCapacity, RefillPerSec: cfg.RateLimit.IPRefillPerSec},
		},
		WebhookLimit: middleware.Limit{Capacity: cfg.RateLimit.WebhookCapacity, RefillPerSec: cfg.RateLimit.WebhookRefillPerSec},
		AuthFailures: middleware.Limit{Capacity: cfg.RateLimit.AuthFailureCapacity, RefillPerSec: cfg.RateLimit.AuthFailureRefillPerSec},
		ClientIP:     middleware.NewClientIP(cfg.RateLimit.TrustedProxies),
	}

	gatewayBudget := time.Duration(cfg.Gateway.MaxAttempts) * cfg.Gateway.HTTPTimeout

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	context.AfterFunc(sigCtx, stop)

	return server.Run(sigCtx, []server.Listener{
		{
			Name:      "merchant",
			Addr:      fmt.Sprintf(":%d", cfg.App.Port),
			Handler:   api.NewMerchantRouter(deps),
			TLS:       merchantTLS,
			Transport: merchantTransport,
			Timeouts: server.Timeouts{
				ReadHeader: 10 * time.Second,
				Read:       30 * time.Second,
				Write:      gatewayBudget + 30*time.Second,
				Idle:       120 * time.Second,
			},
			MaxHeaderBytes: maxHeaderBytes,
		},
		{
			Name:      "webhook",
			Addr:      fmt.Sprintf(":%d", cfg.App.WebhookPort),
			Handler:   api.NewWebhookRouter(deps),
			TLS:       webhookTLS,
			Transport: webhookTransport,
			Timeouts: server.Timeouts{
				ReadHeader: 10 * time.Second,
				Read:       15 * time.Second,
				Write:      30 * time.Second,
				Idle:       60 * time.Second,
			},
			MaxHeaderBytes: maxHeaderBytes,
		},
		{
			Name:      "health",
			Addr:      fmt.Sprintf(":%d", cfg.App.HealthPort),
			Handler:   api.NewHealthRouter(deps),
			Transport: "plain",
			Timeouts: server.Timeouts{
				ReadHeader: 5 * time.Second,
				Read:       5 * time.Second,
				Write:      10 * time.Second,
				Idle:       30 * time.Second,
			},
			MaxHeaderBytes: healthMaxHeaderBytes,
			DrainLast:      true,
		},
	}, server.Options{
		DrainDelay:      cfg.App.ShutdownDrainDelay,
		ShutdownTimeout: cfg.App.ShutdownTimeout,
		OnReady:         func(map[string]net.Addr) { health.SetReady() },
		OnDraining:      health.SetDraining,
		Logger:          logger,
	})
}

type circuitBreakerAdapter struct {
	store     *redis.CircuitBreakerStore
	threshold int
}

type breakerStateAdapter struct{ store *redis.CircuitBreakerStore }

const (
	maxHeaderBytes       = 32 << 10
	healthMaxHeaderBytes = 8 << 10
)

type limiterAdapter struct{ rl *redis.RateLimiter }

func (a circuitBreakerAdapter) RecordSuccess(ctx context.Context, gatewayID string) error {
	return a.store.RecordSuccess(ctx, gatewayID)
}
func (a circuitBreakerAdapter) RecordFailure(ctx context.Context, gatewayID string) error {
	_, _, err := a.store.RecordFailure(ctx, gatewayID, a.threshold)
	return err
}
func (a breakerStateAdapter) BreakerState(ctx context.Context, gatewayID string) (string, time.Time, error) {
	cb, err := a.store.Get(ctx, gatewayID)
	if err != nil {
		return "", time.Time{}, err
	}
	return string(cb.State), cb.CooldownUntil, nil
}

func (a limiterAdapter) Take(ctx context.Context, buckets []middleware.Bucket, cost int64) middleware.RateDecision {
	keys := make([]redis.Bucket, len(buckets))
	for i, b := range buckets {
		keys[i] = redis.Bucket{Namespace: b.Namespace, ID: b.ID, Capacity: b.Capacity, RefillPerSec: b.RefillPerSec}
	}
	res := a.rl.Take(ctx, keys, cost)
	return middleware.RateDecision{Allowed: res.Allowed, RetryAfter: res.RetryAfter}
}
