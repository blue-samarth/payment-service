package ports

type Logger interface {
	Info(event string, fields map[string]any)
	Warn(event string, fields map[string]any)
	Error(event string, fields map[string]any, err error)
	Debug(event string, fields map[string]any)
	Trace(event string, fields map[string]any)
	With(fields map[string]any) Logger
}

const (
	LogEventTransactionCreated      = "transaction.created"
	LogEventTransactionTransition   = "transaction.state_transition"
	LogEventTransactionLeaseExpired = "transaction.lease_expired"

	LogEventGatewayRequest       = "gateway.request"
	LogEventGatewayResponse      = "gateway.response"
	LogEventGatewayFallback      = "gateway.fallback_triggered"
	LogEventGatewayTimeout       = "gateway.timeout"
	LogEventGatewayCircuitOpen   = "gateway.circuit_breaker_opened"
	LogEventGatewayCircuitClosed = "gateway.circuit_breaker_closed"
	LogEventGatewayCircuitProbe  = "gateway.circuit_breaker_probe"

	LogEventRoutingDecision    = "routing.decision"
	LogEventRoutingRescore     = "routing.rescore"
	LogEventRoutingNoCandidate = "routing.no_candidate"

	LogEventOutboxWrite          = "outbox.write"
	LogEventOutboxPublish        = "outbox.publish"
	LogEventOutboxPublishFailure = "outbox.publish_failure"
	LogEventOutboxDeadLetter     = "outbox.dead_letter"
	LogEventOutboxReplay         = "outbox.replay"
	LogEventRelayModeSwitch      = "relay.mode_switch"
	LogEventRelayConsumerStale   = "relay.consumer_stale"

	LogEventRefundInitiated         = "refund.initiated"
	LogEventRefundSucceeded         = "refund.succeeded"
	LogEventRefundFailed            = "refund.failed"
	LogEventRefundRetry             = "refund.retry"
	LogEventRefundOverRefundBlocked = "refund.over_refund_blocked"

	LogEventCancelIntent     = "cancel.intent_set"
	LogEventCancelResolution = "cancel.resolution"

	LogEventReconciliationStart    = "reconciliation.started"
	LogEventReconciliationComplete = "reconciliation.completed"
	LogEventReconciliationMismatch = "reconciliation.mismatch_detected"
	LogEventAutoResolution         = "reconciliation.auto_resolution"

	LogEventConfigReload  = "config.hot_reload"
	LogEventConfigInvalid = "config.invalid_rejected"

	LogEventTLSCertReloaded     = "tls.cert_reloaded"
	LogEventTLSCertReloadFailed = "tls.cert_reload_failed"

	LogEventRateLimitRejected = "rate_limit.rejected"
	LogEventRateLimitFallback = "rate_limit.fallback_activated"
	LogEventRateLimitRestored = "rate_limit.redis_restored"

	LogEventWebhookInboundReceived   = "webhook.inbound_received"
	LogEventWebhookInboundDuplicate  = "webhook.inbound_duplicate"
	LogEventWebhookInboundInvalid    = "webhook.inbound_invalid_signature"
	LogEventWebhookOutboundDelivered = "webhook.outbound_delivered"
	LogEventWebhookOutboundFailed    = "webhook.outbound_failed"

	LogEventPartitionCreated           = "partition.created"
	LogEventPartitionDetached          = "partition.detached"
	LogEventPartitionDropped           = "partition.dropped"
	LogEventPartitionManagementSkipped = "partition.management_skipped_wal_lag"
)

const (
	FieldTraceID        = "trace_id"
	FieldSpanID         = "span_id"
	FieldTransactionID  = "transaction_id"
	FieldMerchantID     = "merchant_id"
	FieldPaymentMethod  = "payment_method"
	FieldGatewayID      = "gateway_id"
	FieldEnvironment    = "environment"
	FieldServiceVersion = "service_version"
	FieldErrorCode      = "error_code"
	FieldRefundID       = "refund_id"
	FieldAttemptNumber  = "attempt_number"
	FieldPreviousState  = "previous_state"
	FieldNewState       = "new_state"
	FieldActor          = "actor"
	FieldDurationMs     = "duration_ms"
	FieldRelayMode      = "relay_mode"
	FieldWALLagMB       = "wal_lag_mb"
	FieldPartitionName  = "partition_name"
	FieldMismatchType   = "mismatch_type"
)
