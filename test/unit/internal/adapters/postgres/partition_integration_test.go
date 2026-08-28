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

	// Positive proof the detach actually ran: if identifiers regressed to
	// unquoted (lowercase) names, ListPartitions would yield a name that
	// weekEndFromName can't parse, detachStale would silently skip it, and no
	// detach would be logged — while the name-absence check above still passed.
	var detaches int
	if err := pg.DB.Pool().QueryRow(ctx,
		"SELECT count(*) FROM partition_management_log WHERE partition_name = 'outbox_2025_W02' AND action = 'detach'",
	).Scan(&detaches); err != nil {
		t.Fatal(err)
	}
	if detaches != 1 {
		t.Errorf("expected exactly one logged detach of outbox_2025_W02, got %d", detaches)
	}
}

// A PUBLISHING row is an event a relay worker is actively delivering (or one
// stranded by a crashed worker) — undelivered by definition. The detach guard
// keys on CountUnpublished, so it must count PUBLISHING rows; otherwise an aged
// partition holding an in-flight event reports zero, gets detached, and is
// dropped → permanent event loss.
func TestPartitionManager_CountUnpublishedIncludesPublishing(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "outbox_events")
	ctx := context.Background()

	store := postgres.NewPartitionStore(pg.DB, pg.Q)

	start := time.Date(2025, 1, 20, 0, 0, 0, 0, time.UTC) // ISO week W04, Monday
	const name = "outbox_2025_W04"
	if err := store.CreatePartition(ctx, name, start, start.AddDate(0, 0, 7)); err != nil {
		t.Fatalf("seed old partition: %v", err)
	}

	if _, err := pg.DB.Pool().Exec(ctx,
		`INSERT INTO outbox_events (id, aggregate_id, aggregate_type, event_type, payload, status, created_at, next_attempt_at, locked_at)
		 VALUES (gen_random_uuid(), gen_random_uuid(), 'transaction', 'PAYMENT_CREATED', '{}'::jsonb, 'PUBLISHING', $1, $1, NOW())`,
		start.AddDate(0, 0, 1),
	); err != nil {
		t.Fatalf("seed PUBLISHING row: %v", err)
	}

	n, err := store.CountUnpublished(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("an in-flight PUBLISHING event must count as unpublished (so its partition is not detached), got %d", n)
	}
}
