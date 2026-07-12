package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/gateways"
	"samarth/payment-service/internal/adapters/gateways/payu"
	"samarth/payment-service/internal/adapters/gateways/razorpay"
	"samarth/payment-service/internal/adapters/gateways/stripe"
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

	logger := observability.NewSlogLogger(parseLogLevel(cfg.Observability.LogLevel))
	metrics := observability.NewNoopMetrics()

	ctx := context.Background()

	db, err := postgres.New(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
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

	registry := gateways.NewRegistry()
	registry.Register("stripe", stripe.New(stripe.Config{
		APIKey:  os.Getenv("STRIPE_API_KEY"),
		BaseURL: os.Getenv("STRIPE_BASE_URL"),
		Timeout: cfg.Gateway.HTTPTimeout,
	}))
	registry.Register("razorpay", razorpay.New(razorpay.Config{
		KeyID:     os.Getenv("RAZORPAY_KEY_ID"),
		KeySecret: os.Getenv("RAZORPAY_KEY_SECRET"),
		BaseURL:   os.Getenv("RAZORPAY_BASE_URL"),
		Timeout:   cfg.Gateway.HTTPTimeout,
	}))
	registry.Register("payu", payu.New(payu.Config{
		MerchantKey:  os.Getenv("PAYU_MERCHANT_KEY"),
		MerchantSalt: os.Getenv("PAYU_MERCHANT_SALT"),
		BaseURL:      os.Getenv("PAYU_BASE_URL"),
		Timeout:      cfg.Gateway.HTTPTimeout,
	}))

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

	var authProvider middleware.TokenProvider
	tokenProvider := middleware.NewStaticTokenProvider(
		parseKeyValues(os.Getenv("SERVICE_TOKENS")),
		splitEnv("OPS_TOKENS"),
	)
	if tokenProvider.HasAny() {
		authProvider = tokenProvider
		logger.Info("auth.enabled", nil)
	} else {
		logger.Warn("auth.disabled_no_tokens", nil)
	}

	handler := api.NewRouter(api.Deps{
		Payment: handlers.NewPaymentHandler(paymentSvc),
		Refund:  handlers.NewRefundHandler(refundSvc),
		Cancel:  handlers.NewCancelHandler(cancelSvc),
		Webhook: handlers.NewWebhookHandler(webhookSvc, registry, webhookSecrets, configStore, logger),
		Health:  handlers.NewHealthHandler(db),
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

type breakerStateAdapter struct { store *redis.CircuitBreakerStore }
type limiterAdapter struct { rl *redis.RateLimiter }

func (a circuitBreakerAdapter) RecordSuccess(ctx context.Context, gatewayID string) error { return a.store.RecordSuccess(ctx, gatewayID) }
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

func parseLogLevel(level string) slog.Level {
	switch level {
	case "error":
		return slog.LevelError
	case "warn":
		return slog.LevelWarn
	case "debug":
		return slog.LevelDebug
	case "trace":
		return slog.Level(-8)
	default:
		return slog.LevelInfo
	}
}
