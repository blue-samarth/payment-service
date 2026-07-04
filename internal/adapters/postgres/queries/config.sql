-- name: ConfigGetGatewayConfig
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
    COALESCE(gm.fx_efficiency_ratio, 1.0),
    COALESCE(gm.last_updated, NOW())
FROM gateway_config gc
LEFT JOIN gateway_circuit_breaker_state cb ON cb.gateway_id = gc.gateway_id
LEFT JOIN gateway_metrics gm ON gm.gateway_id = gc.gateway_id
WHERE gc.gateway_id = $1;

-- name: ConfigListActiveGateways
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
    COALESCE(gm.fx_efficiency_ratio, 1.0),
    COALESCE(gm.last_updated, NOW())
FROM gateway_config gc
LEFT JOIN gateway_circuit_breaker_state cb ON cb.gateway_id = gc.gateway_id
LEFT JOIN gateway_metrics gm ON gm.gateway_id = gc.gateway_id
WHERE gc.is_active = true
  AND $1 = ANY(gc.supported_methods);

-- name: ConfigGetFeeModel
SELECT gateway_id, payment_method, fixed_paise, percentage_bps,
       interchange_cap_paise, discount_volume_threshold_paise
FROM gateway_fee_models
WHERE gateway_id = $1 AND payment_method = $2;

-- name: ConfigGetMetadataSchema
SELECT gateway_id, allowed_keys, required_keys, max_size_bytes
FROM gateway_metadata_schemas
WHERE gateway_id = $1;

-- name: ConfigGetRoutingWeights
SELECT merchant_tier, volume_score, cost_score, reliability_score,
       fx_efficiency_score, latency_score
FROM gateway_routing_weights
WHERE merchant_tier = $1;

-- name: ConfigGetRoutingWeightsDefault
SELECT merchant_tier, volume_score, cost_score, reliability_score,
       fx_efficiency_score, latency_score
FROM gateway_routing_weights
WHERE merchant_tier = 'default';

-- name: ConfigWebhookPolicy
SELECT webhook_replay_window_sec, webhook_clock_skew_sec
FROM gateway_config
WHERE gateway_id = $1;

-- name: ConfigGetProcessingTimeout
SELECT estimated_timeout_sec
FROM gateway_timeouts
WHERE gateway_id = $1 AND payment_method = $2;

-- name: ConfigListTimeouts
SELECT payment_method, estimated_timeout_sec
FROM gateway_timeouts
WHERE gateway_id = $1;

-- name: ConfigListTimeoutsForGateways
SELECT gateway_id, payment_method, estimated_timeout_sec
FROM gateway_timeouts
WHERE gateway_id = ANY(string_to_array($1, ','));
