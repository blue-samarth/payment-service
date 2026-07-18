package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/adapters/postgres"
	"samarth/payment-service/internal/adapters/sns"
	"samarth/payment-service/internal/bootstrap"
	"samarth/payment-service/internal/relay"
	"samarth/payment-service/internal/relay/publisher"
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	outboxWriter := postgres.NewOutboxWriter(db, queries)
	outboxWriter.SetClaimTTL(time.Duration(cfg.Outbox.ClaimTTLSec) * time.Second)

	pub, err := buildPublisher(ctx, cfg, logger)
	if err != nil {
		return err
	}

	worker := relay.NewWorker(outboxWriter, pub, logger, metrics, relay.Config{
		ShardMin:     0,
		ShardMax:     cfg.Outbox.ShardCount - 1,
		BatchSize:    cfg.Outbox.BatchSize,
		MaxAttempts:  cfg.Outbox.MaxAttempts,
		PollInterval: time.Duration(cfg.Outbox.PollIntervalSec) * time.Second,
	})

	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("relay worker: %w", err)
	}
	logger.Info("relay.stopped", nil)
	return nil
}

func buildPublisher(ctx context.Context, cfg *config.Config, logger *observability.SlogLogger) (relay.Publisher, error) {
	switch cfg.Outbox.Publisher {
	case "sns":
		pub, err := sns.NewPublisherFromConfig(ctx, cfg.AWS.Region, cfg.SNS.PaymentEventsTopic, cfg.Outbox.SNSAggregateVersionAttr, logger)
		if err != nil {
			return nil, fmt.Errorf("build sns publisher: %w", err)
		}
		logger.Info("relay.publisher_selected", map[string]any{"publisher": "sns", "topic": cfg.SNS.PaymentEventsTopic})
		return pub, nil
	default:
		if cfg.App.Environment == "dev" {
			logger.Info("relay.publisher_selected", map[string]any{"publisher": "log"})
		} else {
			logger.Warn("relay.publisher_selected", map[string]any{
				"publisher":   "log",
				"warning":     "OUTBOX_PUBLISHER=log in a non-dev environment: the relay is up but delivers nothing (events are only logged). Set OUTBOX_PUBLISHER=sns for real delivery.",
				"environment": cfg.App.Environment,
			})
		}
		return publisher.NewLogPublisher(logger), nil
	}
}
