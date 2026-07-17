package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultIntentTTL = 60 * time.Second
	intentTTLBuffer  = 30 * time.Second
)

type IntentStore struct {
	client goredis.UniversalClient
}

func NewIntentStore(c *Client) *IntentStore {
	return &IntentStore{client: c.Cache}
}

func intentsKey(gatewayID string) string { return "gateway:intents:" + gatewayID }

func (s *IntentStore) EnterProcessing(ctx context.Context, gatewayID string, txnID uuid.UUID, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = defaultIntentTTL
	}
	key := intentsKey(gatewayID)
	expiry := time.Now().UTC().Add(ttl).Unix()

	pipe := s.client.Pipeline()
	pipe.ZAdd(ctx, key, goredis.Z{Score: float64(expiry), Member: txnID.String()})
	pipe.Expire(ctx, key, ttl+intentTTLBuffer)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("intents: enter %s: %w", gatewayID, err)
	}
	return nil
}

func (s *IntentStore) ExitProcessing(ctx context.Context, gatewayID string, txnID uuid.UUID) error {
	if err := s.client.ZRem(ctx, intentsKey(gatewayID), txnID.String()).Err(); err != nil {
		return fmt.Errorf("intents: exit %s: %w", gatewayID, err)
	}
	return nil
}

func (s *IntentStore) ActiveIntents(ctx context.Context, gatewayIDs []string) (map[string]int, error) {
	out := make(map[string]int, len(gatewayIDs))
	if len(gatewayIDs) == 0 {
		return out, nil
	}

	now := strconv.FormatInt(time.Now().UTC().Unix(), 10)
	pipe := s.client.Pipeline()
	cards := make([]*goredis.IntCmd, len(gatewayIDs))
	for i, g := range gatewayIDs {
		key := intentsKey(g)
		pipe.ZRemRangeByScore(ctx, key, "0", now)
		cards[i] = pipe.ZCard(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, fmt.Errorf("intents: active count: %w", err)
	}

	for i, g := range gatewayIDs {
		out[g] = int(cards[i].Val())
	}
	return out, nil
}
