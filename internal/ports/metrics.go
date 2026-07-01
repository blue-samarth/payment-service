package ports

type MetricRecorder interface {
	Increment(metric string, tags map[string]string)
	Histogram(metric string, value float64, tags map[string]string)
	Gauge(metric string, value float64, tags map[string]string)
}

const (
	MetricTransactionCreated   = "transaction.created"
	MetricTransactionSucceeded = "transaction.succeeded"
	MetricTransactionFailed    = "transaction.failed"
	MetricTransactionCancelled = "transaction.cancelled"
	MetricTransactionLatencyMs = "transaction.latency_ms"

	MetricGatewayRequestLatencyMs   = "gateway.request_latency_ms"
	MetricGatewayDiscrepancyRate    = "gateway.discrepancy_rate"
	MetricGatewayCircuitBreakerOpen = "gateway.circuit_breaker_open"
	MetricGatewayFallbackTriggered  = "gateway.fallback_triggered"
	MetricGatewayFeeCalculated      = "gateway.fee_calculated_paise"
	MetricGatewayInconsistencyRate  = "gateway.inconsistency_rate"

	MetricRoutingDecisionMs     = "routing.decision_ms"
	MetricRoutingRescore        = "routing.rescore_total"
	MetricRoutingCandidateCount = "routing.candidate_count"

	MetricRefundInitiated          = "refund.initiated"
	MetricRefundSucceeded          = "refund.succeeded"
	MetricRefundFailed             = "refund.failed"
	MetricRefundDuplicationBlocked = "refund.duplication_blocked"

	MetricOutboxEventWritten     = "outbox.event_written"
	MetricOutboxPublishLatencyMs = "outbox.publish_latency_ms"
	MetricOutboxPublishFailure   = "outbox.publish_failure_total"
	MetricOutboxDeadLetter       = "outbox.dead_letter_total"
	MetricOutboxRelayCDCLagMB    = "outbox_relay_cdc_lag_mb"
	MetricOutboxConsumerHealthy  = "outbox_relay_consumer_healthy"

	MetricPartitionCount                = "outbox.partition_count"
	MetricPartitionAgeDays              = "outbox.partition_age_days"
	MetricPartitionManagementDurationMs = "outbox.partition_management_duration_ms"

	MetricRateLimitAllowed                  = "rate_limit.allowed"
	MetricRateLimitRejected                 = "rate_limit.rejected"
	MetricRateLimitFallbackActive           = "rate_limit.fallback_active"
	MetricRateLimitRedisAvailable           = "rate_limit_redis_available"
	MetricRateLimitFallbackActivationsTotal = "rate_limit_fallback_activations_total"
	MetricRateLimitFallbackDurationSeconds  = "rate_limit_fallback_duration_seconds"

	MetricFeeCacheHit       = "gateway_fee_cache.hit"
	MetricFeeCacheMiss      = "gateway_fee_cache.miss"
	MetricFeeCacheStale     = "gateway_fee_cache.stale"
	MetricFeeCacheStaleness = "GATEWAY_FEE_CACHE_STALENESS"

	MetricReconciliationMismatchRate    = "reconciliation.mismatch_rate"
	MetricReconciliationJobDurationMs   = "reconciliation.job_duration_ms"
	MetricReconciliationMissingInternal = "reconciliation.missing_internal"
	MetricFXVariancePct                 = "reconciliation.fx_variance_pct"
	MetricFeeMismatchCount              = "reconciliation.fee_mismatch_count"
	MetricAutoResolutionCount           = "reconciliation.auto_resolution_count"
	MetricAutoResolutionFailureRate     = "reconciliation.auto_resolution_failure_rate"

	MetricConfigPushLatency    = "gateway_config_push_latency"
	MetricConfigUpdateErrors   = "gateway_config_update_errors"
	MetricConfigConsistencyLag = "gateway_config_consistency_lag"
)

func StandardTags(env, version, gatewayID, paymentMethod, merchantID string) map[string]string {
	return map[string]string{
		"environment":     env,
		"service_version": version,
		"gateway_id":      gatewayID,
		"payment_method":  paymentMethod,
		"merchant_id":     merchantID,
	}
}

func MergeTags(base, additional map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(additional))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range additional {
		out[k] = v
	}
	return out
}
