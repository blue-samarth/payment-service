package encryption

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"
)

func randomKey(t *testing.T, n int) []byte {
	t.Helper()
	k := make([]byte, n)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		t.Fatal(err)
	}
	return k
}

func TestLocalKeyManager_GenerateAndDecryptRoundTrip(t *testing.T) {
	km, err := NewLocalKeyManager("kek-1", randomKey(t, 32))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	plain, wrapped, err := km.GenerateDataKey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(plain) != dekSize {
		t.Errorf("expected %d-byte DEK, got %d", dekSize, len(plain))
	}
	if bytes.Equal(plain, wrapped) {
		t.Error("wrapped DEK must not equal the plaintext DEK")
	}

	got, err := km.Decrypt(ctx, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plain) {
		t.Error("unwrapped DEK does not match the original")
	}
}

func TestLocalKeyManager_EachDataKeyIsUnique(t *testing.T) {
	km, _ := NewLocalKeyManager("kek", randomKey(t, 32))
	ctx := context.Background()
	p1, w1, _ := km.GenerateDataKey(ctx)
	p2, w2, _ := km.GenerateDataKey(ctx)
	if bytes.Equal(p1, p2) {
		t.Error("two generated DEKs should differ")
	}
	if bytes.Equal(w1, w2) {
		t.Error("two wrapped DEKs should differ (unique nonce)")
	}
}

func TestLocalKeyManager_WrongMasterKeyCannotUnwrap(t *testing.T) {
	km1, _ := NewLocalKeyManager("kek", randomKey(t, 32))
	km2, _ := NewLocalKeyManager("kek", randomKey(t, 32))
	ctx := context.Background()

	_, wrapped, _ := km1.GenerateDataKey(ctx)
	if _, err := km2.Decrypt(ctx, wrapped); err == nil {
		t.Error("a DEK wrapped under one KEK must not unwrap under another")
	}
}

func TestLocalKeyManager_DifferentKeyIDCannotUnwrap(t *testing.T) {
	master := randomKey(t, 32)
	km1, _ := NewLocalKeyManager("kek-a", master)
	km2, _ := NewLocalKeyManager("kek-b", master)
	ctx := context.Background()

	_, wrapped, _ := km1.GenerateDataKey(ctx)
	if _, err := km2.Decrypt(ctx, wrapped); err == nil {
		t.Error("key id is bound as AAD; a different key id must fail to unwrap")
	}
}

func TestLocalKeyManager_TamperedWrappedFails(t *testing.T) {
	km, _ := NewLocalKeyManager("kek", randomKey(t, 32))
	ctx := context.Background()
	_, wrapped, _ := km.GenerateDataKey(ctx)
	wrapped[len(wrapped)-1] ^= 0xFF
	if _, err := km.Decrypt(ctx, wrapped); err == nil {
		t.Error("tampered wrapped DEK must fail authentication")
	}
}

func TestNewLocalKeyManager_Validation(t *testing.T) {
	if _, err := NewLocalKeyManager("kek", randomKey(t, 16)); err == nil {
		t.Error("expected error for a non-32-byte master key")
	}
	if _, err := NewLocalKeyManager("", randomKey(t, 32)); err == nil {
		t.Error("expected error for an empty key id")
	}
}
