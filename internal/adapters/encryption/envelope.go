package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	formatVersion   = 1
	defaultMaxUses  = 1 << 20
	defaultDEKTTL   = time.Hour
	defaultCacheTTL = time.Hour
)

type Config struct {
	DEKTTL     time.Duration
	CacheTTL   time.Duration
	MaxDEKUses int
}

type Envelope struct {
	km      KeyManager
	dekTTL  time.Duration
	maxUses int

	mu         sync.Mutex
	curAEAD    cipher.AEAD
	curWrapped []byte
	uses       int
	dekExpiry  time.Time

	cache *dekCache
	now   func() time.Time
}

func NewEnvelope(km KeyManager, cfg Config) *Envelope {
	if cfg.DEKTTL <= 0 {
		cfg.DEKTTL = defaultDEKTTL
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = defaultCacheTTL
	}
	if cfg.MaxDEKUses <= 0 {
		cfg.MaxDEKUses = defaultMaxUses
	}
	now := time.Now
	return &Envelope{
		km:      km,
		dekTTL:  cfg.DEKTTL,
		maxUses: cfg.MaxDEKUses,
		cache:   newDEKCache(cfg.CacheTTL, now),
		now:     now,
	}
}

func (e *Envelope) Encrypt(ctx context.Context, plaintext, aad []byte) ([]byte, error) {
	aead, wrapped, err := e.currentDEK(ctx)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("encryption: nonce: %w", err)
	}
	ct := aead.Seal(nil, nonce, plaintext, aad)
	return encode(wrapped, nonce, ct), nil
}

func (e *Envelope) Decrypt(ctx context.Context, blob, aad []byte) ([]byte, error) {
	wrapped, nonce, ct, err := decode(blob)
	if err != nil {
		return nil, err
	}
	aead, err := e.aeadForWrapped(ctx, wrapped)
	if err != nil {
		return nil, err
	}
	pt, err := aead.Open(nil, nonce, ct, aad)
	if err != nil {
		return nil, fmt.Errorf("encryption: open ciphertext: %w", err)
	}
	return pt, nil
}

func (e *Envelope) currentDEK(ctx context.Context) (cipher.AEAD, []byte, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.curAEAD == nil || e.uses >= e.maxUses || e.now().After(e.dekExpiry) {
		dek, wrapped, err := e.km.GenerateDataKey(ctx)
		if err != nil {
			return nil, nil, err
		}
		aead, err := aeadFromKey(dek)
		if err != nil {
			return nil, nil, err
		}
		e.curAEAD = aead
		e.curWrapped = wrapped
		e.uses = 0
		e.dekExpiry = e.now().Add(e.dekTTL)
		e.cache.put(cacheKey(wrapped), aead)
	}
	e.uses++
	return e.curAEAD, e.curWrapped, nil
}

func (e *Envelope) aeadForWrapped(ctx context.Context, wrapped []byte) (cipher.AEAD, error) {
	key := cacheKey(wrapped)
	if aead, ok := e.cache.get(key); ok {
		return aead, nil
	}
	dek, err := e.km.Decrypt(ctx, wrapped)
	if err != nil {
		return nil, err
	}
	aead, err := aeadFromKey(dek)
	if err != nil {
		return nil, err
	}
	e.cache.put(key, aead)
	return aead, nil
}

func aeadFromKey(dek []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("encryption: data cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryption: data GCM: %w", err)
	}
	return aead, nil
}

func cacheKey(wrapped []byte) string {
	sum := sha256.Sum256(wrapped)
	return hex.EncodeToString(sum[:])
}

func encode(wrapped, nonce, ct []byte) []byte {
	buf := make([]byte, 0, 1+2+len(wrapped)+1+len(nonce)+len(ct))
	buf = append(buf, formatVersion)
	var l [2]byte
	binary.BigEndian.PutUint16(l[:], uint16(len(wrapped)))
	buf = append(buf, l[:]...)
	buf = append(buf, wrapped...)
	buf = append(buf, byte(len(nonce)))
	buf = append(buf, nonce...)
	buf = append(buf, ct...)
	return buf
}

func decode(blob []byte) (wrapped, nonce, ct []byte, err error) {
	if len(blob) < 3 || blob[0] != formatVersion {
		return nil, nil, nil, fmt.Errorf("encryption: unrecognised ciphertext format")
	}
	pos := 1
	wrappedLen := int(binary.BigEndian.Uint16(blob[pos : pos+2]))
	pos += 2
	if len(blob) < pos+wrappedLen+1 {
		return nil, nil, nil, fmt.Errorf("encryption: truncated ciphertext (wrapped key)")
	}
	wrapped = blob[pos : pos+wrappedLen]
	pos += wrappedLen
	nonceLen := int(blob[pos])
	pos++
	if len(blob) < pos+nonceLen {
		return nil, nil, nil, fmt.Errorf("encryption: truncated ciphertext (nonce)")
	}
	nonce = blob[pos : pos+nonceLen]
	pos += nonceLen
	ct = blob[pos:]
	return wrapped, nonce, ct, nil
}

type dekEntry struct {
	aead cipher.AEAD
	exp  time.Time
}

type dekCache struct {
	mu  sync.RWMutex
	ttl time.Duration
	m   map[string]dekEntry
	now func() time.Time
}

func newDEKCache(ttl time.Duration, now func() time.Time) *dekCache {
	return &dekCache{ttl: ttl, m: make(map[string]dekEntry), now: now}
}

func (c *dekCache) get(key string) (cipher.AEAD, bool) {
	c.mu.RLock()
	entry, ok := c.m[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if c.now().After(entry.exp) {
		c.mu.Lock()
		delete(c.m, key)
		c.mu.Unlock()
		return nil, false
	}
	return entry.aead, true
}

func (c *dekCache) put(key string, aead cipher.AEAD) {
	c.mu.Lock()
	c.m[key] = dekEntry{aead: aead, exp: c.now().Add(c.ttl)}
	c.mu.Unlock()
}
