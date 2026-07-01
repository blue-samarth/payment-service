-- name: TransactionInsert
INSERT INTO transactions (
    id, merchant_id, amount, currency, payment_method, status, version,
    gateway_id, gateway_reference_id, gateway_idempotency_key,
    attempted_gateway, actual_gateway, original_gateway,
    estimated_timeout_seconds, failure_reason, method_details, metadata,
    description, customer_id, customer_email,
    cancel_intent, cancel_requested_by, cancel_requested_at, cancel_requested_via,
    processing_started_at, processing_timeout,
    created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10,
    $11, $12, $13,
    $14, $15, $16, $17,
    $18, $19, $20,
    $21, $22, $23, $24,
    $25, $26,
    $27, $28
);

-- name: TransactionGetByID
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
WHERE id = $1;

-- name: TransactionUpdateStatus
-- $1=status $2=actual_gateway $3=gateway_reference_id $4=failure_reason
-- $5=method_details $6=processing_started_at $7=processing_timeout
-- $8=updated_at $9=id $10=current_version (before increment)
UPDATE transactions SET
    status                = $1,
    version               = version + 1,
    actual_gateway        = $2,
    gateway_reference_id  = $3,
    failure_reason        = $4,
    method_details        = $5,
    processing_started_at = $6,
    processing_timeout    = $7,
    updated_at            = $8
WHERE id = $9
  AND version = $10
RETURNING version;

-- name: TransactionSetCancelIntent
UPDATE transactions
SET
    cancel_intent        = true,
    cancel_requested_by  = $2,
    cancel_requested_at  = NOW(),
    cancel_requested_via = $3,
    updated_at           = NOW()
WHERE id = $1
  AND cancel_intent = false;

-- name: TransactionListExpiredLeases
SELECT id FROM transactions
WHERE status = 'PROCESSING'
  AND processing_started_at IS NOT NULL
  AND processing_timeout IS NOT NULL
  AND NOW() > processing_started_at + processing_timeout;