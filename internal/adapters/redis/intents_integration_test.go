//go:build integration

package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/adapters/redis"
	"samarth/payment-service/internal/testsupport"
)

func TestIntentStore_EnterExitCounts(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewIntentStore(c)
	ctx := context.Background()
	const gw = "count-gw"

	t1, t2 := uuid.New(), uuid.New()
	if err := store.EnterProcessing(ctx, gw, t1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.EnterProcessing(ctx, gw, t2, time.Minute); err != nil {
		t.Fatal(err)
	}

	counts, err := store.ActiveIntents(ctx, []string{gw})
	if err != nil {
		t.Fatal(err)
	}
	if counts[gw] != 2 {
		t.Fatalf("expected 2 in-flight intents, got %d", counts[gw])
	}

	if err := store.ExitProcessing(ctx, gw, t1); err != nil {
		t.Fatal(err)
	}
	counts, err = store.ActiveIntents(ctx, []string{gw})
	if err != nil {
		t.Fatal(err)
	}
	if counts[gw] != 1 {
		t.Fatalf("expected 1 in-flight intent after exit, got %d", counts[gw])
	}
}

func TestIntentStore_ExpiredEntriesSelfHeal(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewIntentStore(c)
	ctx := context.Background()
	const gw = "leak-gw"

	// A leaked intent (process crashed before Exit) must not count forever:
	// once its per-entry expiry passes, the next count evicts it. Uses a short
	// TTL and waits it out rather than relying on an explicit Exit.
	if err := store.EnterProcessing(ctx, gw, uuid.New(), time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2200 * time.Millisecond)

	counts, err := store.ActiveIntents(ctx, []string{gw})
	if err != nil {
		t.Fatal(err)
	}
	if counts[gw] != 0 {
		t.Fatalf("expected expired intent to self-heal to 0, got %d", counts[gw])
	}
}

func TestIntentStore_BatchesMultipleGateways(t *testing.T) {
	c := testsupport.RequireRedis(t)
	testsupport.FlushRedis(t, c)
	store := redis.NewIntentStore(c)
	ctx := context.Background()

	if err := store.EnterProcessing(ctx, "a", uuid.New(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.EnterProcessing(ctx, "a", uuid.New(), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.EnterProcessing(ctx, "b", uuid.New(), time.Minute); err != nil {
		t.Fatal(err)
	}

	counts, err := store.ActiveIntents(ctx, []string{"a", "b", "c"})
	if err != nil {
		t.Fatal(err)
	}
	if counts["a"] != 2 || counts["b"] != 1 || counts["c"] != 0 {
		t.Fatalf("unexpected counts: a=%d b=%d c=%d", counts["a"], counts["b"], counts["c"])
	}
}
