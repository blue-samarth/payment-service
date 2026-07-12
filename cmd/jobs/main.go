package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/gateways"
	"samarth/payment-service/internal/adapters/gateways/payu"
	"samarth/payment-service/internal/adapters/gateways/razorpay"
	"samarth/payment-service/internal/adapters/gateways/stripe"
	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/adapters/postgres"
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/app/refund"
	approuting "samarth/payment-service/internal/app/routing"
	leaseexpiry "samarth/payment-service/internal/jobs/lease_expiry"
	partitionmanager "samarth/payment-service/internal/jobs/partition_manager"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	job := os.Getenv("JOB")
	if job == "" {
		job = "partition_manager"
	}

	cfg, err := config.LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := observability.NewSlogLogger(parseLogLevel(cfg.Observability.LogLevel))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db, err := postgres.New(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer db.Close()

	queries, err := postgres.LoadQueries()
	if err != nil {
		return fmt.Errorf("load queries: %w", err)
	}

	switch job {
	case "partition_manager":
		mgr := partitionmanager.New(
			postgres.NewPartitionStore(db, queries),
			logger,
			partitionmanager.Config{},
		)
		logger.Info("job.starting", map[string]any{"job": job})
		if err := mgr.RunOnce(ctx); err != nil {
			return fmt.Errorf("partition_manager: %w", err)
		}
		logger.Info("job.completed", map[string]any{"job": job})
		return nil
	case "lease_expiry":
		reaper := buildReaper(cfg, db, queries, logger)
		logger.Info("job.starting", map[string]any{"job": job})
		if err := reaper.RunOnce(ctx); err != nil {
			return fmt.Errorf("lease_expiry: %w", err)
		}
		logger.Info("job.completed", map[string]any{"job": job})
		return nil
	default:
		return fmt.Errorf("unknown JOB %q", job)
	}
}

func buildReaper(cfg *config.Config, db *postgres.DB, queries *postgres.Queries, logger *observability.SlogLogger) *leaseexpiry.Reaper {
	metrics := observability.NewNoopMetrics()

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

	txnRepo := postgres.NewTransactionRepository(db, queries)
	refundRepo := postgres.NewRefundRepository(db, queries)
	outboxWriter := postgres.NewOutboxWriter(db, queries)
	leaseRepo := postgres.NewLeaseRepository(db, queries)
	transactor := postgres.NewTransactor(db)
	configStore := postgres.NewConfigStore(db, queries)
	idempotencyRepo := postgres.NewIdempotencyRepository(db, queries)

	paymentSvc := payment.NewService(
		txnRepo,
		outboxWriter,
		approuting.NewRouter(configStore),
		configStore,
		transactor,
		leaseRepo,
		registry,
		logger,
		metrics,
	)
	refundSvc := refund.NewService(txnRepo, refundRepo, outboxWriter, transactor, registry, logger, metrics)
	paymentSvc.SetCancelResolver(refundSvc)

	idemGuard := idempotency.NewGuard(idempotencyRepo, transactor)
	paymentSvc.SetIdempotency(idemGuard)
	refundSvc.SetIdempotency(idemGuard)

	return leaseexpiry.New(txnRepo, paymentSvc, idempotencyRepo, logger, leaseexpiry.Config{
		IdempotencyProcessingTimeout: time.Duration(cfg.Jobs.IdempotencyProcessingTimeoutSec) * time.Second,
	})
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
