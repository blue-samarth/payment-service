package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/adapters/postgres"
	"samarth/payment-service/internal/app/idempotency"
	"samarth/payment-service/internal/app/payment"
	"samarth/payment-service/internal/app/refund"
	approuting "samarth/payment-service/internal/app/routing"
	"samarth/payment-service/internal/bootstrap"
	leaseexpiry "samarth/payment-service/internal/jobs/lease_expiry"
	partitionmanager "samarth/payment-service/internal/jobs/partition_manager"
	"samarth/payment-service/internal/ports"
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

	logger := observability.NewSlogLogger(bootstrap.ParseLogLevel(cfg.Observability.LogLevel))

	ctx := context.Background()
	if cfg.Jobs.RunTimeoutMinutes > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cfg.Jobs.RunTimeoutMinutes)*time.Minute)
		defer cancel()
	}

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

	switch job {
	case "partition_manager":
		mgr := partitionmanager.New(
			postgres.NewPartitionStore(db, queries),
			logger,
			partitionmanager.Config{
				WeeksAhead:     cfg.Jobs.PartitionWeeksAhead,
				RetentionWeeks: cfg.Jobs.PartitionRetentionWeeks,
				DropAfter:      time.Duration(cfg.Jobs.PartitionDropAfterDays) * 24 * time.Hour,
			},
		)
		logger.Info("job.starting", map[string]any{"job": job})
		if err := mgr.RunOnce(ctx); err != nil {
			return fmt.Errorf("partition_manager: %w", err)
		}
		logger.Info("job.completed", map[string]any{"job": job})
		return nil
	case "lease_expiry":
		reaper := buildReaper(cfg, db, queries, logger, metrics)
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

func buildReaper(cfg *config.Config, db *postgres.DB, queries *postgres.Queries, logger *observability.SlogLogger, metrics ports.MetricRecorder) *leaseexpiry.Reaper {
	registry := bootstrap.GatewayRegistry(cfg)

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
