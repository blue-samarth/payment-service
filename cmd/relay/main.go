package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"samarth/payment-service/config"
	"samarth/payment-service/internal/adapters/observability"
	"samarth/payment-service/internal/adapters/postgres"
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

	logger := observability.NewSlogLogger(parseLogLevel(cfg.Observability.LogLevel))
	metrics := observability.NewNoopMetrics()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := postgres.New(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer db.Close()

	queries, err := postgres.LoadQueries()
	if err != nil {
		return fmt.Errorf("load queries: %w", err)
	}

	outboxWriter := postgres.NewOutboxWriter(db, queries)
	outboxWriter.SetClaimTTL(time.Duration(cfg.Outbox.ClaimTTLSec) * time.Second)
	pub := publisher.NewLogPublisher(logger)

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
