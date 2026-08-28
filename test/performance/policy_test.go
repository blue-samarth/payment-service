//go:build performance

package performance

import (
	"strings"
	"testing"
)

func mkSample(perRound []int64, allocs, bytes int64) sample {
	var total, minNs, maxNs int64
	worst := 0
	for i, ns := range perRound {
		total += ns
		if i == 0 || ns < minNs {
			minNs = ns
		}
		if i == 0 || ns > maxNs {
			maxNs = ns
			worst = i
		}
	}
	return sample{
		meanNs:   total / int64(len(perRound)),
		allocs:   allocs,
		bytes:    bytes,
		perRound: perRound,
		minNs:    minNs,
		maxNs:    maxNs,
		worst:    worst,
	}
}

func TestPolicy_MeanWithinBudgetPasses(t *testing.T) {
	got := mkSample([]int64{100, 110, 120, 105, 115}, 0, 0)
	if v := violations("probe", Baseline{NsPerOp: 100}, got, 1.0); len(v) != 0 {
		t.Fatalf("expected no violations, got %v", v)
	}
}

func TestPolicy_MeanAt125Passes(t *testing.T) {
	got := mkSample([]int64{125, 125, 125, 125}, 0, 0)
	if v := violations("probe", Baseline{NsPerOp: 100}, got, 1.0); len(v) != 0 {
		t.Fatalf("mean exactly at 1.25x must pass, got %v", v)
	}
}

func TestPolicy_MeanAbove125Fails(t *testing.T) {
	got := mkSample([]int64{130, 130, 130, 130}, 0, 0)
	v := violations("probe", Baseline{NsPerOp: 100}, got, 1.0)
	if len(v) != 1 || !strings.Contains(v[0], "mean limit") {
		t.Fatalf("expected a mean violation, got %v", v)
	}
}

func TestPolicy_SingleRoundSpikeFailsWithHealthyMean(t *testing.T) {
	got := mkSample([]int64{100, 100, 250, 100, 100, 100, 100, 100, 100, 100}, 0, 0)
	if got.meanNs > 125 {
		t.Fatalf("probe misconstructed: mean %d should stay within budget", got.meanNs)
	}
	v := violations("probe", Baseline{NsPerOp: 100}, got, 1.0)
	if len(v) != 1 || !strings.Contains(v[0], "spike limit") {
		t.Fatalf("expected a spike violation with a healthy mean, got %v", v)
	}
	if !strings.Contains(v[0], "round 3 of 10") {
		t.Fatalf("spike message must identify the offending round, got %q", v[0])
	}
}

func TestPolicy_SpikeAt2xPasses(t *testing.T) {
	got := mkSample([]int64{100, 100, 200, 100, 100}, 0, 0)
	if v := violations("probe", Baseline{NsPerOp: 100}, got, 1.0); len(v) != 0 {
		t.Fatalf("a round exactly at 2.0x must pass, got %v", v)
	}
}

func TestPolicy_AllocationsAreNotScaled(t *testing.T) {
	got := mkSample([]int64{100}, 4, 128)
	v := violations("probe", Baseline{NsPerOp: 100, AllocsPerOp: 3, BytesPerOp: 128}, got, 10.0)
	if len(v) != 1 || !strings.Contains(v[0], "allocs/op") {
		t.Fatalf("allocation budget must ignore PERF_SCALE, got %v", v)
	}
}

func TestPolicy_AllocationsNotDoubledBySpikeFactor(t *testing.T) {
	got := mkSample([]int64{100}, 6, 256)
	v := violations("probe", Baseline{NsPerOp: 100, AllocsPerOp: 3, BytesPerOp: 256}, got, 1.0)
	if len(v) != 1 || !strings.Contains(v[0], "allocates 6 allocs/op") {
		t.Fatalf("2x must not apply to allocations, got %v", v)
	}
}

func TestPolicy_NegativeBaselineSkipsAllocationCheck(t *testing.T) {
	got := mkSample([]int64{100}, 99, 9999)
	if v := violations("probe", Baseline{NsPerOp: 100, AllocsPerOp: -1, BytesPerOp: -1}, got, 1.0); len(v) != 0 {
		t.Fatalf("-1 must disable allocation checks, got %v", v)
	}
}
