-- name: OutboxInsert
INSERT INTO outbox_events
    (id, aggregate_id, aggregate_type, event_type, payload,
     event_version, status, created_at, next_attempt_at, attempts)
VALUES
    ($1, $2, $3, $4, $5, $6, 'PENDING', NOW(), COALESCE($7, NOW()), 0);

-- name: OutboxMarkPublished
UPDATE outbox_events
SET status = 'PUBLISHED', published_at = NOW()
WHERE id = $1 AND created_at = $2 AND status = 'PENDING';

-- name: OutboxMarkFailed
UPDATE outbox_events
SET
    attempts        = attempts + 1,
    last_error      = $3,
    next_attempt_at = $4
WHERE id = $1 AND created_at = $2 AND status = 'PENDING';

-- name: OutboxMarkExhausted
UPDATE outbox_events
SET status = 'FAILED', last_error = $3
WHERE id = $1 AND created_at = $2 AND status = 'PENDING'
RETURNING aggregate_id, aggregate_type, event_type, payload, event_version;

-- name: OutboxDeadLetterInsert
INSERT INTO outbox_dead_letters
    (id, original_event_id, aggregate_id, aggregate_type, event_type, payload, event_version, failure_reason, failed_at)
VALUES
    (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, NOW());

-- name: OutboxPollPending
SELECT id, aggregate_id, aggregate_type, event_type, payload, event_version, attempts, created_at
FROM outbox_events
WHERE status = 'PENDING'
  AND shard_index BETWEEN $1 AND $2
  AND next_attempt_at <= NOW()
ORDER BY attempts ASC, created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT $3;

-- name: OutboxDeadLetterGet
SELECT aggregate_id, aggregate_type, event_type, payload, event_version, resolved_at
FROM outbox_dead_letters
WHERE id = $1;

-- name: OutboxDeadLetterResolve
UPDATE outbox_dead_letters
SET resolved_at = NOW(), resolved_by = $2
WHERE id = $1;

-- name: OutboxReplayInsert
INSERT INTO outbox_events
    (id, aggregate_id, aggregate_type, event_type, payload,
     event_version, status, created_at, next_attempt_at, attempts)
VALUES
    ($1, $2, $3, $4, $5, $6, 'PENDING', NOW(), NOW(), 0);

-- name: MerchantWebhookInsert
INSERT INTO merchant_webhook_deliveries
    (id, merchant_id, transaction_id, event_type, payload, endpoint_url,
     status, attempts, next_attempt_at, created_at)
VALUES
    ($1, $2, $3, $4, $5, $6, 'PENDING', 0, NOW(), NOW());