package ports

import "context"

type Tracer interface {
	StartSpan(ctx context.Context, name string) (context.Context, Span)
	SpanFromContext(ctx context.Context) Span
}

type Span interface {
	AddAttribute(key string, value any)
	RecordError(err error)
	SetStatus(msg string)
	End()
	TraceID() string
	SpanID() string
}

const (
	SpanCreatePayment    = "payment.create"
	SpanGetPaymentStatus = "payment.get_status"
	SpanCancelPayment    = "payment.cancel"
	SpanInitiateRefund   = "refund.initiate"
	SpanGetRefundStatus  = "refund.get_status"

	SpanGatewayInitiate    = "gateway.initiate_payment"
	SpanGatewayCheckStatus = "gateway.check_status"
	SpanGatewayRefund      = "gateway.refund"
	SpanGatewayCancel      = "gateway.cancel"

	SpanRoutingDecision = "routing.decide"
	SpanRoutingScore    = "routing.score"
	SpanRoutingFilter   = "routing.filter_candidates"

	SpanOutboxWrite   = "outbox.write"
	SpanOutboxPublish = "outbox.publish"

	SpanReconcileTransaction  = "reconciliation.reconcile_transaction"
	SpanFetchSettlementReport = "reconciliation.fetch_settlement_report"

	SpanWebhookInbound  = "webhook.inbound_process"
	SpanWebhookOutbound = "webhook.outbound_deliver"

	SpanDBQuery  = "db.query"
	SpanCacheGet = "cache.get"
	SpanCacheSet = "cache.set"
)

const (
	AttrSampledError           = "sampled.error"
	AttrSampledLatencyExceeded = "sampled.latency_exceeded"
	AttrSampledRetry           = "sampled.retry"
	AttrSampledFallback        = "sampled.fallback"
)
