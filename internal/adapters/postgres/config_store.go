package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"samarth/payment-service/internal/ports"
)

var ErrGatewayNotFound = errors.New("gateway not found")

type ConfigStore struct{ db *DB }

func NewConfigStore(db *DB) *ConfigStore { return &ConfigStore{db: db} }

func (s *ConfigStore) GetGatewayConfig(ctx context.Context, gatewayID string) (*ports.GatewayConfig, error) {
	row := s.db.pool.QueryRow(ctx, `
		SELECT
			gc.gateway_id, gc.display_name, gc.is_active,
			gc.min_amount, gc.max_amount,
			gc.supported_currencies, gc.supported_methods,
			gc.idempotency_capable, gc.supports_cancel, gc.supports_partial_refund,
			gc.priority, gc.updated_at,
			COALESCE(cb.state, 'CLOSED'),
			COALESCE(cb.cooldown_until, '0001-01-01 00:00:00+00'),
			COALESCE(gm.discrepancy_rate_24h, 0),
			COALESCE(gm.p99_latency_ms, 0),
			COALESCE(gm.volume_7d, 0),
			COALESCE(gm.fx_rate_spread, 1.0),
			COALESCE(gm.last_updated, NOW())
		FROM gateway_config gc
		LEFT JOIN gateway_circuit_breaker_state cb ON cb.gateway_id = gc.gateway_id
		LEFT JOIN gateway_metrics gm ON gm.gateway_id = gc.gateway_id
		WHERE gc.gateway_id = $1
	`, gatewayID)

	cfg, err := scanGatewayConfig(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrGatewayNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("config_store: get gateway %s: %w", gatewayID, err)
	}

	timeouts, err := s.getTimeouts(ctx, gatewayID)
	if err != nil {
		return nil, err
	}
	cfg.EstimatedTimeouts = timeouts

	return cfg, nil
}

func (s *ConfigStore) ListActiveGateways(ctx context.Context, paymentMethod string) ([]*ports.GatewayConfig, error) {
	rows, err := s.db.pool.Query(ctx, `
		SELECT
			gc.gateway_id, gc.display_name, gc.is_active,
			gc.min_amount, gc.max_amount,
			gc.supported_currencies, gc.supported_methods,
			gc.idempotency_capable, gc.supports_cancel, gc.supports_partial_refund,
			gc.priority, gc.updated_at,
			COALESCE(cb.state, 'CLOSED'),
			COALESCE(cb.cooldown_until, '0001-01-01 00:00:00+00'),
			COALESCE(gm.discrepancy_rate_24h, 0),
			COALESCE(gm.p99_latency_ms, 0),
			COALESCE(gm.volume_7d, 0),
			COALESCE(gm.fx_rate_spread, 1.0),
			COALESCE(gm.last_updated, NOW())
		FROM gateway_config gc
		LEFT JOIN gateway_circuit_breaker_state cb ON cb.gateway_id = gc.gateway_id
		LEFT JOIN gateway_metrics gm ON gm.gateway_id = gc.gateway_id
		WHERE gc.is_active = true
		  AND $1 = ANY(gc.supported_methods)
	`, paymentMethod)
	if err != nil {
		return nil, fmt.Errorf("config_store: list active gateways: %w", err)
	}
	defer rows.Close()

	var configs []*ports.GatewayConfig
	for rows.Next() {
		cfg, err := scanGatewayConfig(rows)
		if err != nil {
			return nil, fmt.Errorf("config_store: scan gateway: %w", err)
		}
		timeouts, err := s.getTimeouts(ctx, cfg.GatewayID)
		if err != nil {
			return nil, err
		}
		cfg.EstimatedTimeouts = timeouts
		configs = append(configs, cfg)
	}
	return configs, rows.Err()
}

func (s *ConfigStore) GetFeeModel(ctx context.Context, gatewayID, paymentMethod string) (*ports.GatewayFeeModel, error) {
	var m ports.GatewayFeeModel
	err := s.db.pool.QueryRow(ctx, `
		SELECT gateway_id, payment_method, fixed_paise, percentage_bps,
		       interchange_cap_paise, discount_volume_threshold_paise
		FROM gateway_fee_models
		WHERE gateway_id = $1 AND payment_method = $2
	`, gatewayID, paymentMethod).Scan(
		&m.GatewayID, &m.PaymentMethod, &m.FixedPaise, &m.PercentageBPS,
		&m.InterchangeCapPaise, &m.DiscountVolumeThresholdPaise,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("config_store: no fee model for %s/%s", gatewayID, paymentMethod)
	}
	if err != nil {
		return nil, fmt.Errorf("config_store: get fee model %s/%s: %w", gatewayID, paymentMethod, err)
	}
	return &m, nil
}

func (s *ConfigStore) GetMetadataSchema(ctx context.Context, gatewayID string) (*ports.GatewayMetadataSchema, error) {
	var schema ports.GatewayMetadataSchema
	err := s.db.pool.QueryRow(ctx, `
		SELECT gateway_id, allowed_keys, required_keys, max_size_bytes
		FROM gateway_metadata_schemas
		WHERE gateway_id = $1
	`, gatewayID).Scan(
		&schema.GatewayID, &schema.AllowedKeys,
		&schema.RequiredKeys, &schema.MaxSizeBytes,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("config_store: no metadata schema for %s", gatewayID)
	}
	if err != nil {
		return nil, fmt.Errorf("config_store: get metadata schema %s: %w", gatewayID, err)
	}
	return &schema, nil
}

func (s *ConfigStore) GetRoutingWeights(ctx context.Context, merchantTier string) (*ports.RoutingWeights, error) {
	var w ports.RoutingWeights

	// Try tier-specific first, fall back to default.
	err := s.db.pool.QueryRow(ctx, `
		SELECT merchant_tier, volume_score, cost_score, reliability_score,
		       fx_efficiency_score, latency_score
		FROM gateway_routing_weights
		WHERE merchant_tier = $1
	`, merchantTier).Scan(
		&w.MerchantTier, &w.VolumeScore, &w.CostScore,
		&w.ReliabilityScore, &w.FXEfficiencyScore, &w.LatencyScore,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		err = s.db.pool.QueryRow(ctx, `
			SELECT merchant_tier, volume_score, cost_score, reliability_score,
			       fx_efficiency_score, latency_score
			FROM gateway_routing_weights
			WHERE merchant_tier = 'default'
		`).Scan(
			&w.MerchantTier, &w.VolumeScore, &w.CostScore,
			&w.ReliabilityScore, &w.FXEfficiencyScore, &w.LatencyScore,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("config_store: get routing weights for tier %s: %w", merchantTier, err)
	}
	return &w, nil
}

func (s *ConfigStore) WebhookPolicy(ctx context.Context, gatewayID string) (replayWindowSec, clockSkewSec int, err error) {
	err = s.db.pool.QueryRow(ctx, `
		SELECT webhook_replay_window_sec, webhook_clock_skew_sec
		FROM gateway_config
		WHERE gateway_id = $1
	`, gatewayID).Scan(&replayWindowSec, &clockSkewSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return 300, 30, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("config_store: webhook policy %s: %w", gatewayID, err)
	}
	return replayWindowSec, clockSkewSec, nil
}

func (s *ConfigStore) GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error) {
	var sec int
	err := s.db.pool.QueryRow(ctx, `
		SELECT estimated_timeout_sec
		FROM gateway_timeouts
		WHERE gateway_id = $1 AND payment_method = $2
	`, gatewayID, paymentMethod).Scan(&sec)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("config_store: no timeout configured for %s/%s", gatewayID, paymentMethod)
	}
	if err != nil {
		return 0, fmt.Errorf("config_store: get processing timeout %s/%s: %w", gatewayID, paymentMethod, err)
	}
	return time.Duration(sec) * time.Second, nil
}

func (s *ConfigStore) getTimeouts(ctx context.Context, gatewayID string) (map[string]time.Duration, error) {
	rows, err := s.db.pool.Query(ctx, `
		SELECT payment_method, estimated_timeout_sec
		FROM gateway_timeouts
		WHERE gateway_id = $1
	`, gatewayID)
	if err != nil {
		return nil, fmt.Errorf("config_store: get timeouts for %s: %w", gatewayID, err)
	}
	defer rows.Close()

	timeouts := make(map[string]time.Duration)
	for rows.Next() {
		var method string
		var sec int
		if err := rows.Scan(&method, &sec); err != nil {
			return nil, fmt.Errorf("config_store: scan timeout: %w", err)
		}
		timeouts[method] = time.Duration(sec) * time.Second
	}
	return timeouts, rows.Err()
}

// scanGatewayConfig scans a row from gateway_config joined with
// gateway_circuit_breaker_state and gateway_metrics.
// Accepts both pgx.Row and pgx.Rows via the scanner interface.
func scanGatewayConfig(row interface {
	Scan(dest ...any) error
}) (*ports.GatewayConfig, error) {
	var cfg ports.GatewayConfig
	var cbState string
	var cbCooldown time.Time

	err := row.Scan(
		&cfg.GatewayID, &cfg.DisplayName, &cfg.IsActive,
		&cfg.MinAmount, &cfg.MaxAmount,
		&cfg.SupportedCurrencies, &cfg.SupportedMethods,
		&cfg.IdempotencyCapable, &cfg.SupportsCancel, &cfg.SupportsPartialRefund,
		&cfg.Priority, &cfg.UpdatedAt,
		&cbState, &cbCooldown,
		&cfg.Metrics.DiscrepancyRate24h, &cfg.Metrics.P99LatencyMs,
		&cfg.Metrics.Volume7d, &cfg.Metrics.FXEfficiencyRatio, &cfg.Metrics.LastUpdated,
	)
	if err != nil {
		return nil, err
	}

	cfg.CircuitBreaker = ports.CircuitBreakerState{
		State:         cbState,
		CooldownUntil: cbCooldown,
	}

	return &cfg, nil
}
