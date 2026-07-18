package ring

import (
	"hash/fnv"
	"sort"
	"strconv"
)

const vnodesPerWorker = 128

// ReplicationFactor is how many workers each shard is assigned to. With RF=2,
// every bucket is polled by two workers for redundancy: because the DB claim
// (FOR UPDATE SKIP LOCKED), not the assignment, is what prevents double
// delivery, the two workers safely split a bucket's rows, and if one dies the
// other is already polling that bucket — failover needs no reconfiguration.
const ReplicationFactor = 2

// OwnedShards returns the subset of shard indices [0, shardCount) assigned to
// the worker at workerIndex, given workerCount workers, via a consistent-hash
// ring with ReplicationFactor replicas per shard. The mapping is deterministic,
// so every worker computes the same ring and selects its own shards without
// coordination.
//
// Consistent hashing gives the scale-out property this exists for: adding a
// worker (workerCount N -> N+1) never re-assigns a shard between two existing
// workers — a shard's replica set only ever gains the new worker (displacing
// its last replica) — so most buckets keep being served across a rolling deploy.
func OwnedShards(workerIndex, workerCount, shardCount int) []int {
	sets := replicaSets(workerCount, shardCount)
	owned := make([]int, 0, shardCount)
	for shard, set := range sets {
		for _, w := range set {
			if w == workerIndex {
				owned = append(owned, shard)
				break
			}
		}
	}
	return owned
}

// replicaSets returns, for every shard in [0, shardCount), the worker indices
// that own it — the first ReplicationFactor distinct workers clockwise on the
// ring (clamped to workerCount when there are fewer workers than replicas).
func replicaSets(workerCount, shardCount int) [][]int {
	sets := make([][]int, shardCount)
	if workerCount <= 1 {
		for s := range sets {
			sets[s] = []int{0} // one worker owns everything; no redundancy possible
		}
		return sets
	}

	rf := ReplicationFactor
	if rf > workerCount {
		rf = workerCount
	}

	type vnode struct {
		pos    uint64
		worker int
	}
	ring := make([]vnode, 0, workerCount*vnodesPerWorker)
	for w := 0; w < workerCount; w++ {
		for v := 0; v < vnodesPerWorker; v++ {
			ring = append(ring, vnode{pos: hashKey("worker:" + strconv.Itoa(w) + ":vnode:" + strconv.Itoa(v)), worker: w})
		}
	}
	sort.Slice(ring, func(i, j int) bool { return ring[i].pos < ring[j].pos })

	for shard := 0; shard < shardCount; shard++ {
		h := hashKey("shard:" + strconv.Itoa(shard))
		idx := sort.Search(len(ring), func(i int) bool { return ring[i].pos >= h })
		if idx == len(ring) {
			idx = 0 // wrap around the ring
		}

		set := make([]int, 0, rf)
		seen := make(map[int]bool, rf)
		for len(set) < rf {
			w := ring[idx].worker
			if !seen[w] {
				seen[w] = true
				set = append(set, w)
			}
			idx++
			if idx == len(ring) {
				idx = 0
			}
		}
		sets[shard] = set
	}
	return sets
}

func hashKey(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return mix64(h.Sum64())
}

// mix64 is the splitmix64 finalizer. FNV-1a alone distributes short, structured
// keys poorly (adjacent worker/vnode strings cluster on the ring); this gives
// the avalanche needed for even, well-spread ring positions.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
