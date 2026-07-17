package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/app/refund"
	approuting "samarth/payment-service/internal/app/routing"
	"samarth/payment-service/internal/app/webhook"
	"samarth/payment-service/internal/bootstrap"
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
	webhookSecrets := webhook.NewStaticSecretProvider(parseKeyValues(os.Getenv("WEBHOOK_SECRETS")))

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
		threshold: int(getEnvInt64("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5)),
	})
	router.SetBreakerState(breakerStateAdapter{store: cbStore})
	paymentSvc.SetMaxGatewayAttempts(cfg.Gateway.MaxAttempts)

	intentStore := redis.NewIntentStore(redisClient)
	router.SetActiveIntents(intentStore)
	paymentSvc.SetIntentTracker(intentStore)

	var authProvider middleware.TokenProvider
	tokenProvider := middleware.NewStaticTokenProvider(
		parseKeyValues(os.Getenv("SERVICE_TOKENS")),
		splitEnv("OPS_TOKENS"),
	)
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

	handler := api.NewRouter(api.Deps{
		Payment: handlers.NewPaymentHandler(paymentSvc),
		Refund:  handlers.NewRefundHandler(refundSvc),
		Cancel:  handlers.NewCancelHandler(cancelSvc),
		Webhook: handlers.NewWebhookHandler(webhookSvc, registry, webhookSecrets, configStore, logger),
		Health: handlers.NewHealthHandler(
			handlers.Check{Name: "database", Pinger: db},
			handlers.Check{Name: "redis", Pinger: redisClient},
		),
		Logger:  logger,
		Auth:    authProvider,
		Limiter: limiterAdapter{rl: rateLimiter},
		RateLimit: middleware.RateLimitConfig{
			Capacity:     getEnvInt64("RATE_LIMIT_CAPACITY", 100),
			RefillPerSec: getEnvFloat("RATE_LIMIT_REFILL_PER_SEC", 50),
		},
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.App.Port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	var tlsMgr *security.Manager
	if cfg.Security.TLSCertFile != "" {
		if !cfg.App.MtlsStrictMode {
			logger.Warn("mtls.optional_mode", map[string]any{
				"warning": "MTLS_STRICT_MODE=false: client certificates are requested but not enforced",
			})
		}
		tlsMgr, err = security.NewManager(security.Config{
			CertFile:        cfg.Security.TLSCertFile,
			KeyFile:         cfg.Security.TLSKeyFile,
			CAFile:          cfg.Security.TLSCAFile,
			OptionalMTLS:    !cfg.App.MtlsStrictMode,
			RefreshInterval: time.Duration(cfg.Security.CertRefreshIntervalSec) * time.Second,
		}, logger, metrics)
		if err != nil {
			return fmt.Errorf("configure mTLS: %w", err)
		}
		srv.TLSConfig = tlsMgr.TLSConfig()

		refreshCtx, cancelRefresh := context.WithCancel(context.Background())
		defer cancelRefresh()
		tlsMgr.StartRefresh(refreshCtx)
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("http.server_starting", map[string]any{
			"port": cfg.App.Port,
			"tls":  tlsMgr != nil,
			"mtls": tlsMgr != nil && cfg.App.MtlsStrictMode,
		})
		var serveErr error
		if tlsMgr != nil {
			serveErr = srv.ListenAndServeTLS("", "")
		} else {
			serveErr = srv.ListenAndServe()
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			serverErr <- serveErr
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		return fmt.Errorf("http server: %w", err)
	case <-stop:
		logger.Info("http.server_stopping", nil)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

type circuitBreakerAdapter struct {
	store     *redis.CircuitBreakerStore
	threshold int
}

type breakerStateAdapter struct{ store *redis.CircuitBreakerStore }
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

func (a limiterAdapter) Allow(ctx context.Context, userID, merchantID, ip string, capacity int64, ratePerSec float64) middleware.RateDecision {
	res := a.rl.Allow(ctx, userID, merchantID, ip, capacity, ratePerSec)
	return middleware.RateDecision{Allowed: res.Allowed, RetryAfter: res.RetryAfter}
}

func getEnvInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func parseKeyValues(raw string) map[string]string {
	out := map[string]string{}
	if raw == "" {
		return out
	}
	for _, pair := range strings.Split(raw, ",") {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) == 2 && kv[0] != "" {
			out[kv[0]] = kv[1]
		}
	}
	return out
}

func splitEnv(key string) []string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func getEnvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
