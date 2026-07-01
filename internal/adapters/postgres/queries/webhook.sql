-- name: WebhookEventRecord
INSERT INTO webhook_events (event_id, gateway_id) VALUES ($1, $2)
ON CONFLICT (event_id, gateway_id) DO NOTHING;

-- name: TransactionGetByGatewayRef
SELECT
    id, merchant_id, amount, currency, payment_method, status, version,
    gateway_id, gateway_reference_id, gateway_idempotency_key,
    attempted_gateway, actual_gateway, original_gateway,
    estimated_timeout_seconds, failure_reason, method_details, metadata,
    description, customer_id, customer_email,
    cancel_intent, cancel_requested_by, cancel_requested_at, cancel_requested_via,
    processing_started_at, processing_timeout,
    created_at, updated_at
FROM transactions
WHERE gateway_id = $1 AND gateway_reference_id = $2;

-- name: RawMetadataInsert
INSERT INTO transaction_raw_metadata (transaction_id, gateway_id, payload)
VALUES ($1, $2, $3);
