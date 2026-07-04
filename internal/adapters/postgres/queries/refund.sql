-- name: RefundInsert
INSERT INTO refunds (
    id, transaction_id, amount, reason, status,
    initiated_by, gateway_refund_id, attempted_gateway, actual_gateway,
    attempts, failure_reason, initiated_at, resolved_at
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9,
    $10, $11, $12, $13
);

-- name: RefundGetByID
SELECT
    id, transaction_id, amount, reason, status,
    initiated_by, gateway_refund_id, attempted_gateway, actual_gateway,
    attempts, failure_reason, initiated_at, resolved_at
FROM refunds
WHERE id = $1;

-- name: RefundSumActive
SELECT COALESCE(SUM(amount), 0)
FROM refunds
WHERE transaction_id = $1
  AND status IN ('REFUND_INITIATED', 'REFUND_PROCESSING', 'REFUNDED');

-- name: RefundUpdateStatus
UPDATE refunds SET
    status            = $1,
    gateway_refund_id = $2,
    actual_gateway    = $3,
    attempts          = $4,
    failure_reason    = $5,
    resolved_at       = $6
WHERE id = $7;

-- name: RefundLockParent
SELECT id FROM transactions
WHERE id = $1
FOR UPDATE;

-- name: RefundExistsByReason
SELECT EXISTS(
    SELECT 1 FROM refunds WHERE transaction_id = $1 AND reason = $2
);