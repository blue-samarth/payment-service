//go:build performance

package performance

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

type Baseline struct {
	NsPerOp     int64
	AllocsPerOp int64
	BytesPerOp  int64
}

const (
	scaleEnv      = "PERF_SCALE"
	roundsEnv     = "PERF_ROUNDS"
	meanFactor    = 1.25
	spikeFactor   = 2.0
	defaultScale  = 1.0
	defaultRounds = 10
	minIterations = 100
)

var (
	sinkBool  bool
	sinkInt   int
	sinkBytes []byte
)

type sample struct {
	meanNs   int64
	allocs   int64
	bytes    int64
	perRound []int64
	minNs    int64
	maxNs    int64
	worst    int
}

func scale(tb testing.TB) float64 {
	tb.Helper()
	raw := os.Getenv(scaleEnv)
	if raw == "" {
		return defaultScale
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 {
		tb.Fatalf("%s=%q is not a positive number", scaleEnv, raw)
	}
	return v
}

func rounds(tb testing.TB) int {
	tb.Helper()
	raw := os.Getenv(roundsEnv)
	if raw == "" {
		return defaultRounds
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		tb.Fatalf("%s=%q is not a positive integer", roundsEnv, raw)
	}
	return v
}

func measure(tb testing.TB, fn func(*testing.B)) sample {
	tb.Helper()
	n := rounds(tb)

	var total, minNs, maxNs, allocs, bytes int64
	worst := 0
	perRound := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		res := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			fn(b)
		})
		if res.N < minIterations {
			tb.Fatalf("benchmark completed only %d iterations, too few to trust", res.N)
		}
		ns := res.NsPerOp()
		total += ns
		perRound = append(perRound, ns)
		if i == 0 || ns < minNs {
			minNs = ns
		}
		if i == 0 || ns > maxNs {
			maxNs = ns
			worst = i
		}
		if a := res.AllocsPerOp(); a > allocs {
			allocs = a
		}
		if by := res.AllocedBytesPerOp(); by > bytes {
			bytes = by
		}
	}

	return sample{
		meanNs:   total / int64(n),
		allocs:   allocs,
		bytes:    bytes,
		perRound: perRound,
		minNs:    minNs,
		maxNs:    maxNs,
		worst:    worst,
	}
}

func violations(name string, base Baseline, got sample, s float64) []string {
	var out []string
	hint := scaleHint(s)

	if base.AllocsPerOp >= 0 && got.allocs > base.AllocsPerOp {
		out = append(out, fmt.Sprintf("%s allocates %d allocs/op, baseline is %d",
			name, got.allocs, base.AllocsPerOp))
	}
	if base.BytesPerOp >= 0 && got.bytes > base.BytesPerOp {
		out = append(out, fmt.Sprintf("%s allocates %d B/op, baseline is %d",
			name, got.bytes, base.BytesPerOp))
	}
	if base.NsPerOp <= 0 {
		return out
	}

	meanLimit := int64(float64(base.NsPerOp) * meanFactor * s)
	if got.meanNs > meanLimit {
		out = append(out, fmt.Sprintf(
			"%s mean %d ns/op is %.2fx the %d ns/op baseline, mean limit is %.2fx (%d ns/op)%s",
			name, got.meanNs, float64(got.meanNs)/float64(base.NsPerOp), base.NsPerOp,
			meanFactor, meanLimit, hint))
	}

	spikeLimit := int64(float64(base.NsPerOp) * spikeFactor * s)
	if got.maxNs > spikeLimit {
		out = append(out, fmt.Sprintf(
			"%s round %d of %d hit %d ns/op, %.2fx the %d ns/op baseline, spike limit is %.1fx (%d ns/op)%s",
			name, got.worst+1, len(got.perRound), got.maxNs,
			float64(got.maxNs)/float64(base.NsPerOp), base.NsPerOp,
			spikeFactor, spikeLimit, hint))
	}
	return out
}

func enforce(t *testing.T, name string, base Baseline, fn func(*testing.B)) {
	t.Helper()
	got := measure(t, fn)

	t.Logf("%s: mean %d ns/op over %d rounds (min %d, max %d), %d allocs/op, %d B/op",
		name, got.meanNs, len(got.perRound), got.minNs, got.maxNs, got.allocs, got.bytes)

	for _, v := range violations(name, base, got, scale(t)) {
		t.Error(v)
	}
}

func scaleHint(s float64) string {
	if s == defaultScale {
		return ""
	}
	return fmt.Sprintf(" (scaled by %g)", s)
}
