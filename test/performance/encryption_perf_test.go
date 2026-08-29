//go:build performance

package performance

import (
	"bytes"
	"context"
	"testing"

	"samarth/payment-service/internal/adapters/encryption"
)

func newEnvelope(tb testing.TB) *encryption.Envelope {
	tb.Helper()
	master := bytes.Repeat([]byte{0x2b}, 32)
	km, err := encryption.NewLocalKeyManager("perf-key", master)
	if err != nil {
		tb.Fatalf("key manager: %v", err)
	}
	return encryption.NewEnvelope(km, encryption.Config{})
}

func TestPerf_EnvelopeEncrypt(t *testing.T) {
	env := newEnvelope(t)
	ctx := context.Background()
	plaintext := []byte(`{"pan":"4242424242424242","exp":"12/29","name":"A Buyer"}`)
	aad := []byte("txn:9f1c2d3e")

	enforce(t, "encryption.Encrypt (card payload)", Baseline{NsPerOp: 316, AllocsPerOp: 3, BytesPerOp: 256}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := env.Encrypt(ctx, plaintext, aad); err != nil {
				b.Fatalf("encrypt: %v", err)
			}
		}
	})
}

func TestPerf_EnvelopeDecrypt(t *testing.T) {
	env := newEnvelope(t)
	ctx := context.Background()
	plaintext := []byte(`{"pan":"4242424242424242","exp":"12/29","name":"A Buyer"}`)
	aad := []byte("txn:9f1c2d3e")

	blob, err := env.Encrypt(ctx, plaintext, aad)
	if err != nil {
		t.Fatalf("seed encrypt: %v", err)
	}

	enforce(t, "encryption.Decrypt (card payload)", Baseline{NsPerOp: 231, AllocsPerOp: 3, BytesPerOp: 192}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			out, err := env.Decrypt(ctx, blob, aad)
			if err != nil {
				b.Fatalf("decrypt: %v", err)
			}
			if len(out) != len(plaintext) {
				b.Fatal("decrypt returned wrong length")
			}
		}
	})
}

func TestPerf_EnvelopeRoundTrip(t *testing.T) {
	env := newEnvelope(t)
	ctx := context.Background()
	plaintext := []byte(`{"pan":"4242424242424242","exp":"12/29","name":"A Buyer"}`)
	aad := []byte("txn:9f1c2d3e")

	enforce(t, "encryption round trip", Baseline{NsPerOp: 565, AllocsPerOp: 6, BytesPerOp: 448}, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			blob, err := env.Encrypt(ctx, plaintext, aad)
			if err != nil {
				b.Fatalf("encrypt: %v", err)
			}
			if _, err := env.Decrypt(ctx, blob, aad); err != nil {
				b.Fatalf("decrypt: %v", err)
			}
		}
	})
}
