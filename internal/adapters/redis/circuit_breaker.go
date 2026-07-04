package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"samarth/payment-service/internal/domain/gateway"
)

type CircuitBreakerStore struct {
	client goredis.UniversalClient
}

func NewCircuitBreakerStore(c *Client) *CircuitBreakerStore {
	return &CircuitBreakerStore{client: c.Cache}
}

type cbRecord struct {
	State               string `json:"state"`
	CooldownUntil       string `json:"cooldown_until"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	LastKnownScore      int    `json:"last_known_score"`
}

func cbKey(gatewayID string) string    { return "gateway:cb:" + gatewayID }
func probeKey(gatewayID string) string { return "gateway:probe:" + gatewayID }
func critKey(gatewayID string) string  { return "gateway:critical:" + gatewayID }

func (s *CircuitBreakerStore) Get(ctx context.Context, gatewayID string) (*gateway.CircuitBreaker, error) {
	raw, err := s.client.Get(ctx, cbKey(gatewayID)).Bytes()
	if err == goredis.Nil {
		return &gateway.CircuitBreaker{GatewayID: gatewayID, State: gateway.StateClosed}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("circuit_breaker: get %s: %w", gatewayID, err)
	}
	var r cbRecord
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("circuit_breaker: unmarshal %s: %w", gatewayID, err)
	}
	cb := &gateway.CircuitBreaker{
		GatewayID:                 gatewayID,
		State:                     gateway.CircuitState(r.State),
		ConsecutiveFailures:       r.ConsecutiveFailures,
		LastKnownReliabilityScore: r.LastKnownScore,
	}
	if r.CooldownUntil != "" {
		if t, err := time.Parse(time.RFC3339, r.CooldownUntil); err == nil {
			cb.CooldownUntil = t
		}
	}
	return cb, nil
}

func (s *CircuitBreakerStore) Transition(ctx context.Context, cb *gateway.CircuitBreaker, to gateway.CircuitState) error {
	newFails := cb.ConsecutiveFailures
	if to == gateway.StateOpen {
		newFails++
	} else if to == gateway.StateClosed {
		newFails = 0
	}

	cooldownUntil := ""
	if to == gateway.StateOpen {
		cooldownUntil = time.Now().UTC().Add(gateway.CooldownDuration(newFails)).Format(time.RFC3339)
	}

	n, err := cbTransitionScript.Run(ctx, s.client, []string{cbKey(cb.GatewayID)},
		string(to), cooldownUntil, newFails,
	).Int()
	if err != nil {
		return fmt.Errorf("circuit_breaker: transition script %s: %w", cb.GatewayID, err)
	}
	if n == 0 {
		return fmt.Errorf("circuit_breaker: invalid transition %s → %s", cb.State, to)
	}

	cb.State = to
	cb.ConsecutiveFailures = newFails
	if cooldownUntil != "" {
		t, _ := time.Parse(time.RFC3339, cooldownUntil)
		cb.CooldownUntil = t
	} else {
		cb.CooldownUntil = time.Time{}
	}
	return nil
}

func (s *CircuitBreakerStore) RecordFailure(ctx context.Context, gatewayID string, threshold int) (opened bool, failures int, err error) {
	res, err := cbRecordFailureScript.Run(ctx, s.client, []string{cbKey(gatewayID)}, threshold).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("circuit_breaker: record failure %s: %w", gatewayID, err)
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("circuit_breaker: record failure %s: unexpected result", gatewayID)
	}
	opened, failures = res[0] == 1, int(res[1])

	if opened {
		cooldown := time.Now().UTC().Add(gateway.CooldownDuration(failures)).Format(time.RFC3339)
		if err := cbSetCooldownScript.Run(ctx, s.client, []string{cbKey(gatewayID)}, cooldown).Err(); err != nil {
			return opened, failures, fmt.Errorf("circuit_breaker: set cooldown %s: %w", gatewayID, err)
		}
	}
	return opened, failures, nil
}

func (s *CircuitBreakerStore) RecordSuccess(ctx context.Context, gatewayID string) error {
	if err := cbRecordSuccessScript.Run(ctx, s.client, []string{cbKey(gatewayID)}).Err(); err != nil {
		return fmt.Errorf("circuit_breaker: record success %s: %w", gatewayID, err)
	}
	return nil
}

func (s *CircuitBreakerStore) AcquireProbe(ctx context.Context, gatewayID string, ttl time.Duration) (bool, error) {
	ok, err := s.client.SetNX(ctx, probeKey(gatewayID), "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("circuit_breaker: acquire probe %s: %w", gatewayID, err)
	}
	return ok, nil
}

func (s *CircuitBreakerStore) SetLastKnownScore(ctx context.Context, gatewayID string, score int) error {
	if err := cbSetScoreScript.Run(ctx, s.client, []string{cbKey(gatewayID)}, score).Err(); err != nil {
		return fmt.Errorf("circuit_breaker: set last known score %s: %w", gatewayID, err)
	}
	return nil
}

func (s *CircuitBreakerStore) GetCritical(ctx context.Context, gatewayID string) ([]byte, error) {
	raw, err := s.client.Get(ctx, critKey(gatewayID)).Bytes()
	if err == goredis.Nil {
		return nil, nil
	}
	return raw, err
}

func (s *CircuitBreakerStore) SetCritical(ctx context.Context, gatewayID string, data []byte) error {
	return s.client.Set(ctx, critKey(gatewayID), data, 0).Err()
}
func (s *CircuitBreakerStore) DeleteCritical(ctx context.Context, gatewayID string) error {
	return s.client.Del(ctx, critKey(gatewayID)).Err()
}
