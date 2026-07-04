package encryption

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingKM struct {
	inner KeyManager
	gen   int32
	dec   int32
}

func (c *countingKM) GenerateDataKey(ctx context.Context) ([]byte, []byte, error) {
	atomic.AddInt32(&c.gen, 1)
	return c.inner.GenerateDataKey(ctx)
}
func (c *countingKM) Decrypt(ctx context.Context, wrapped []byte) ([]byte, error) {
	atomic.AddInt32(&c.dec, 1)
	return c.inner.Decrypt(ctx, wrapped)
}
func (c *countingKM) KeyID() string { return c.inner.KeyID() }

func testKM(t *testing.T) *countingKM {
	t.Helper()
	local, err := NewLocalKeyManager("kek-test", randomKey(t, 32))
	if err != nil {
		t.Fatal(err)
	}
	return &countingKM{inner: local}
}

func TestEnvelope_RoundTrip(t *testing.T) {
	e := NewEnvelope(testKM(t), Config{})
	ctx := context.Background()
	aad := []byte("transaction:card:tx-1")
	plaintext := []byte("4242424242424242")

	blob, err := e.Encrypt(ctx, plaintext, aad)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(blob, plaintext) {
		t.Error("ciphertext should not contain the plaintext")
	}

	got, err := e.Decrypt(ctx, blob, aad)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Errorf("round trip mismatch: got %q", got)
	}
}

func TestEnvelope_EmptyPlaintext(t *testing.T) {
	e := NewEnvelope(testKM(t), Config{})
	ctx := context.Background()
	blob, err := e.Encrypt(ctx, []byte{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := e.Decrypt(ctx, blob, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty plaintext, got %d bytes", len(got))
	}
}

func TestEnvelope_TamperedCiphertextFails(t *testing.T) {
	e := NewEnvelope(testKM(t), Config{})
	ctx := context.Background()
	blob, _ := e.Encrypt(ctx, []byte("secret"), []byte("aad"))
	blob[len(blob)-1] ^= 0xFF
	if _, err := e.Decrypt(ctx, blob, []byte("aad")); err == nil {
		t.Error("GCM must reject a tampered ciphertext")
	}
}

func TestEnvelope_WrongAADFails(t *testing.T) {
	e := NewEnvelope(testKM(t), Config{})
	ctx := context.Background()
	blob, _ := e.Encrypt(ctx, []byte("secret"), []byte("field:a"))
	if _, err := e.Decrypt(ctx, blob, []byte("field:b")); err == nil {
		t.Error("decrypting with mismatched AAD must fail (ciphertext is bound to its context)")
	}
}

func TestEnvelope_ReusesDEKAcrossEncrypts(t *testing.T) {
	km := testKM(t)
	e := NewEnvelope(km, Config{MaxDEKUses: 1000})
	ctx := context.Background()

	b1, _ := e.Encrypt(ctx, []byte("one"), nil)
	b2, _ := e.Encrypt(ctx, []byte("two"), nil)

	w1, _, _, _ := decode(b1)
	w2, _, _, _ := decode(b2)
	if !bytes.Equal(w1, w2) {
		t.Error("consecutive encrypts within the DEK budget should share one wrapped DEK")
	}
	if km.gen != 1 {
		t.Errorf("expected a single GenerateDataKey call, got %d", km.gen)
	}
}

func TestEnvelope_RotatesDEKAfterMaxUses(t *testing.T) {
	km := testKM(t)
	e := NewEnvelope(km, Config{MaxDEKUses: 2})
	ctx := context.Background()

	_, _ = e.Encrypt(ctx, []byte("a"), nil)
	_, _ = e.Encrypt(ctx, []byte("b"), nil)
	_, _ = e.Encrypt(ctx, []byte("c"), nil) // third use forces a fresh DEK

	if km.gen != 2 {
		t.Errorf("expected DEK rotation after max uses (2 generations), got %d", km.gen)
	}
}

func TestEnvelope_RotatesDEKAfterTTL(t *testing.T) {
	km := testKM(t)
	e := NewEnvelope(km, Config{DEKTTL: time.Minute, MaxDEKUses: 1000})
	clock := time.Now()
	e.now = func() time.Time { return clock }

	ctx := context.Background()
	_, _ = e.Encrypt(ctx, []byte("a"), nil)
	clock = clock.Add(2 * time.Minute)
	_, _ = e.Encrypt(ctx, []byte("b"), nil)

	if km.gen != 2 {
		t.Errorf("expected DEK rotation after TTL, got %d generations", km.gen)
	}
}

func TestEnvelope_DecryptCacheAvoidsRepeatUnwrap(t *testing.T) {
	km := testKM(t)
	writer := NewEnvelope(km, Config{})
	ctx := context.Background()
	blob, _ := writer.Encrypt(ctx, []byte("secret"), []byte("aad"))

	// A fresh reader has a cold cache; the first Decrypt unwraps via the KM,
	// the second must be served from the cache.
	reader := NewEnvelope(km, Config{})
	if _, err := reader.Decrypt(ctx, blob, []byte("aad")); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Decrypt(ctx, blob, []byte("aad")); err != nil {
		t.Fatal(err)
	}
	if km.dec != 1 {
		t.Errorf("expected exactly one KM unwrap for a repeated wrapped DEK, got %d", km.dec)
	}
}

func TestEnvelope_DecryptCacheExpires(t *testing.T) {
	km := testKM(t)
	writer := NewEnvelope(km, Config{})
	ctx := context.Background()
	blob, _ := writer.Encrypt(ctx, []byte("secret"), []byte("aad"))

	reader := NewEnvelope(km, Config{CacheTTL: time.Minute})
	clock := time.Now()
	reader.now = func() time.Time { return clock }
	reader.cache.now = func() time.Time { return clock }

	_, _ = reader.Decrypt(ctx, blob, []byte("aad"))
	clock = clock.Add(2 * time.Minute)
	_, _ = reader.Decrypt(ctx, blob, []byte("aad"))

	if km.dec != 2 {
		t.Errorf("expected a second unwrap after cache TTL expiry, got %d", km.dec)
	}
}

func TestEnvelope_BadFormatVersion(t *testing.T) {
	e := NewEnvelope(testKM(t), Config{})
	blob, _ := e.Encrypt(context.Background(), []byte("x"), nil)
	blob[0] = 0x09
	if _, err := e.Decrypt(context.Background(), blob, nil); err == nil {
		t.Error("an unknown format version must be rejected")
	}
}

func TestEnvelope_TruncatedBlob(t *testing.T) {
	e := NewEnvelope(testKM(t), Config{})
	for _, blob := range [][]byte{{}, {formatVersion}, {formatVersion, 0x00, 0x40}} {
		if _, err := e.Decrypt(context.Background(), blob, nil); err == nil {
			t.Errorf("expected error for truncated blob %v", blob)
		}
	}
}

func TestEnvelope_ConcurrentUse(t *testing.T) {
	e := NewEnvelope(testKM(t), Config{MaxDEKUses: 4})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			blob, err := e.Encrypt(ctx, []byte("payload"), []byte("aad"))
			if err != nil {
				t.Error(err)
				return
			}
			got, err := e.Decrypt(ctx, blob, []byte("aad"))
			if err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(got, []byte("payload")) {
				t.Error("concurrent round trip mismatch")
			}
		}()
	}
	wg.Wait()
}
