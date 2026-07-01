-- name: LeaseAcquire
INSERT INTO processing_lease (idempotency_key, payment_intent_id, lease_ttl_sec)
VALUES ($1, $2, $3)
ON CONFLICT (idempotency_key) DO NOTHING;

-- name: LeaseGetCached
SELECT cached_response FROM processing_lease WHERE idempotency_key = $1;

-- name: LeaseWriteCached
UPDATE processing_lease SET cached_response = $2 WHERE idempotency_key = $1;
