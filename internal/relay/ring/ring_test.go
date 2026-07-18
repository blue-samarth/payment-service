package ring

import (
	"testing"
)

const shardCount = 64

func TestOwnedShards_EveryShardHasReplicationFactorOwners(t *testing.T) {
	for _, workers := range []int{1, 2, 3, 5, 8} {
		want := ReplicationFactor
		if workers < want {
			want = workers // can't have more distinct replicas than workers
		}
		seen := make(map[int]int) // shard -> how many workers own it
		for w := 0; w < workers; w++ {
			for _, s := range OwnedShards(w, workers, shardCount) {
				seen[s]++
			}
		}
		if len(seen) != shardCount {
			t.Errorf("workers=%d: %d shards covered, want %d", workers, len(seen), shardCount)
		}
		for s, n := range seen {
			if n != want {
				t.Errorf("workers=%d: shard %d owned by %d workers, want %d", workers, s, n, want)
			}
		}
	}
}

func TestOwnedShards_SingleWorkerOwnsAll(t *testing.T) {
	owned := OwnedShards(0, 1, shardCount)
	if len(owned) != shardCount {
		t.Fatalf("single worker should own all %d shards, got %d", shardCount, len(owned))
	}
}

func TestOwnedShards_TwoWorkersEachOwnEverything(t *testing.T) {
	// With exactly RF workers, redundancy means both cover the whole keyspace —
	// this is the "2 nodes store all the data" case.
	for w := 0; w < 2; w++ {
		if n := len(OwnedShards(w, 2, shardCount)); n != shardCount {
			t.Errorf("with 2 workers (RF=2), worker %d should own all %d shards, got %d", w, shardCount, n)
		}
	}
}

func TestOwnedShards_Deterministic(t *testing.T) {
	a := OwnedShards(1, 3, shardCount)
	b := OwnedShards(1, 3, shardCount)
	if len(a) != len(b) {
		t.Fatalf("non-deterministic: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("non-deterministic at %d: %d vs %d", i, a[i], b[i])
		}
	}
}

func TestOwnedShards_ReasonableBalance(t *testing.T) {
	const workers = 4
	// Each shard has ReplicationFactor owners, so total ownership is
	// shardCount*RF spread across the workers.
	ideal := shardCount * ReplicationFactor / workers
	for w := 0; w < workers; w++ {
		n := len(OwnedShards(w, workers, shardCount))
		// Consistent hashing with virtual nodes won't be perfectly even, but no
		// worker should carry more than ~2x its fair share on 64 shards.
		if n == 0 || n > 2*ideal {
			t.Errorf("worker %d owns %d shards, want roughly %d (0 < n <= %d)", w, n, ideal, 2*ideal)
		}
	}
}

// TestOwnedShards_ScaleOutOnlyAddsNewWorker is the property that makes this
// worth doing: growing from N to N+1 workers must never re-assign a shard
// between two existing workers — a shard's replica set only ever gains the new
// worker (index n), displacing its previous last replica.
func TestOwnedShards_ScaleOutOnlyAddsNewWorker(t *testing.T) {
	for n := 2; n <= 6; n++ {
		before := replicaSets(n, shardCount)
		after := replicaSets(n+1, shardCount)
		changed := 0
		for shard := 0; shard < shardCount; shard++ {
			beforeSet := make(map[int]bool, len(before[shard]))
			for _, w := range before[shard] {
				beforeSet[w] = true
			}
			shardChanged := false
			for _, w := range after[shard] {
				if !beforeSet[w] && w != n { // only the new worker n may appear
					t.Fatalf("n=%d->%d: shard %d gained existing worker %d not in its prior replica set %v",
						n, n+1, shard, w, before[shard])
				}
				if !beforeSet[w] {
					shardChanged = true
				}
			}
			if shardChanged {
				changed++
			}
		}
		// The only shards that change are exactly those the new worker joins —
		// nothing churns among existing workers. (A contiguous re-stripe, by
		// contrast, would reassign buckets between existing workers too.)
		newWorkerShards := len(OwnedShards(n, n+1, shardCount))
		if changed != newWorkerShards {
			t.Errorf("n=%d->%d: %d shard sets changed but new worker joined %d — churn beyond the new worker", n, n+1, changed, newWorkerShards)
		}
		// And the new worker's share is bounded (minimal movement), not a full re-stripe.
		if fair := shardCount * ReplicationFactor / (n + 1); newWorkerShards > 2*fair {
			t.Errorf("n=%d->%d: new worker took %d shards, more than ~2x fair share %d", n, n+1, newWorkerShards, fair)
		}
	}
}
