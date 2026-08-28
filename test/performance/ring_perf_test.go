//go:build performance

package performance

import (
	"testing"

	"samarth/payment-service/internal/relay/ring"
)

func TestPerf_OwnedShards_SmallFleet(t *testing.T) {
	enforce(t, "ring.OwnedShards (4 workers, 64 shards)", Baseline{NsPerOp: 39460, AllocsPerOp: 182, BytesPerOp: 11_966}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			owned := ring.OwnedShards(i%4, 4, 64)
			if len(owned) == 0 {
				b.Fatal("worker owns no shards")
			}
		}
	})
}

func TestPerf_OwnedShards_LargeFleet(t *testing.T) {
	enforce(t, "ring.OwnedShards (32 workers, 1024 shards)", Baseline{NsPerOp: 481558, AllocsPerOp: 2850, BytesPerOp: 123_308}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			owned := ring.OwnedShards(i%32, 32, 1024)
			if len(owned) == 0 {
				b.Fatal("worker owns no shards")
			}
		}
	})
}
