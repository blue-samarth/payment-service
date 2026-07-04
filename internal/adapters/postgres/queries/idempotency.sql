-- name: IdempotencyReserve
INSERT INTO idempotency_keys (composite_key_hash, request_hash, response, status)
VALUES ($1, $2, '{}'::jsonb, 'PROCESSING')
ON CONFLICT (composite_key_hash) DO NOTHING;

-- name: IdempotencyLookup
SELECT request_hash, status, response
FROM idempotency_keys
WHERE composite_key_hash = $1;

-- name: IdempotencyComplete
UPDATE idempotency_keys
SET response = $2, status = 'COMPLETED'
WHERE composite_key_hash = $1;

-- name: IdempotencyRelease
DELETE FROM idempotency_keys WHERE composite_key_hash = $1;

-- name: IdempotencySweepStaleProcessing
DELETE FROM idempotency_keys
WHERE status = 'PROCESSING'
  AND created_at < NOW() - ($1 * INTERVAL '1 second');

-- name: IdempotencyDeleteExpired
DELETE FROM idempotency_keys WHERE expires_at < NOW();
