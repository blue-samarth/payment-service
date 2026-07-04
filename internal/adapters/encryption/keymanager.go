package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

const dekSize = 32

type KeyManager interface {
	GenerateDataKey(ctx context.Context) (plaintextDEK, wrappedDEK []byte, err error)
	Decrypt(ctx context.Context, wrappedDEK []byte) (plaintextDEK []byte, err error)
	KeyID() string
}

type LocalKeyManager struct {
	keyID string
	aead  cipher.AEAD
}

func NewLocalKeyManager(keyID string, masterKey []byte) (*LocalKeyManager, error) {
	if len(masterKey) != 32 {
		return nil, fmt.Errorf("encryption: master key must be 32 bytes, got %d", len(masterKey))
	}
	if keyID == "" {
		return nil, fmt.Errorf("encryption: key id must not be empty")
	}
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, fmt.Errorf("encryption: master cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryption: master GCM: %w", err)
	}
	return &LocalKeyManager{keyID: keyID, aead: aead}, nil
}

func (k *LocalKeyManager) KeyID() string { return k.keyID }

func (k *LocalKeyManager) GenerateDataKey(_ context.Context) (plaintextDEK, wrappedDEK []byte, err error) {
	dek := make([]byte, dekSize)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return nil, nil, fmt.Errorf("encryption: generate data key: %w", err)
	}
	nonce := make([]byte, k.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, fmt.Errorf("encryption: data key nonce: %w", err)
	}
	wrapped := k.aead.Seal(nonce, nonce, dek, []byte(k.keyID))
	return dek, wrapped, nil
}

func (k *LocalKeyManager) Decrypt(_ context.Context, wrappedDEK []byte) ([]byte, error) {
	ns := k.aead.NonceSize()
	if len(wrappedDEK) < ns {
		return nil, fmt.Errorf("encryption: wrapped data key too short")
	}
	nonce, ct := wrappedDEK[:ns], wrappedDEK[ns:]
	dek, err := k.aead.Open(nil, nonce, ct, []byte(k.keyID))
	if err != nil {
		return nil, fmt.Errorf("encryption: unwrap data key: %w", err)
	}
	return dek, nil
}

var _ KeyManager = (*LocalKeyManager)(nil)
