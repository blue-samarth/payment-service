//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/adapters/postgres"
	partitionmanager "samarth/payment-service/internal/jobs/partition_manager"
	"samarth/payment-service/internal/ports"
	"samarth/payment-service/internal/testsupport"
)

type silentLogger struct{}

func (silentLogger) Info(string, map[string]any)         {}
func (silentLogger) Warn(string, map[string]any)         {}
func (silentLogger) Error(string, map[string]any, error) {}
func (silentLogger) Debug(string, map[string]any)        {}
func (silentLogger) Trace(string, map[string]any)        {}
func (l silentLogger) With(map[string]any) ports.Logger  { return l }

func TestPartitionManager_PreCreateMakesWritesLandInDatedPartition(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "outbox_events")
	ctx := context.Background()

	store := postgres.NewPartitionStore(pg.DB, pg.Q)
	mgr := partitionmanager.New(store, silentLogger{}, partitionmanager.Config{WeeksAhead: 2})

	if err := mgr.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	outbox := postgres.NewOutboxWriter(pg.DB, pg.Q)
	tr := postgres.NewTransactor(pg.DB)
	aggID := uuid.New()
	if err := tr.WithinTx(ctx, func(ctx context.Context) error {
		return outbox.Write(ctx, ports.OutboxEvent{
			AggregateID:   aggID,
			AggregateType: "transaction",
			EventType:     ports.EventTypePaymentCreated,
			Payload:       []byte(`{}`),
			EventVersion:  1,
		})
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	var partition string
	err := pg.DB.Pool().QueryRow(ctx,
		"SELECT tableoid::regclass::text FROM outbox_events WHERE aggregate_id = $1", aggID,
	).Scan(&partition)
	if err != nil {
		t.Fatalf("locate partition: %v", err)
	}
	if partition == "outbox_default" {
		t.Error("event landed in outbox_default; pre-created dated partition should have absorbed it")
	}
}

func TestPartitionManager_DetachesStaleEmptyPartition(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "outbox_events")
	ctx := context.Background()

	store := postgres.NewPartitionStore(pg.DB, pg.Q)

	// An old, empty partition well outside the retention window.
	start := time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC) // ISO week, Monday
	if err := store.CreatePartition(ctx, "outbox_2025_W02", start, start.AddDate(0, 0, 7)); err != nil {
		t.Fatalf("seed old partition: %v", err)
	}

	mgr := partitionmanager.New(store, silentLogger{}, partitionmanager.Config{WeeksAhead: 0, RetentionWeeks: 2})
	if err := mgr.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	names, err := store.ListPartitions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if n == "outbox_2025_W02" {
			t.Error("expected stale empty partition outbox_2025_W02 to be detached")
		}
	}
}
