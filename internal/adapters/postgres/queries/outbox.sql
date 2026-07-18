-- name: OutboxInsert
INSERT INTO outbox_events
    (id, aggregate_id, aggregate_type, event_type, payload,
     event_version, aggregate_version, status, created_at, next_attempt_at, attempts)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, 'PENDING', NOW(), COALESCE($8, NOW()), 0);

-- name: OutboxMarkPublished
UPDATE outbox_events
SET status = 'PUBLISHED', published_at = NOW(), locked_at = NULL
WHERE id = $1 AND created_at = $2 AND status = 'PUBLISHING';

-- name: OutboxMarkFailed
UPDATE outbox_events
SET
    status          = 'PENDING',
    attempts        = attempts + 1,
    last_error      = $3,
    next_attempt_at = $4,
    locked_at       = NULL
WHERE id = $1 AND created_at = $2 AND status = 'PUBLISHING';

-- name: OutboxMarkExhausted
UPDATE outbox_events
SET status = 'FAILED', last_error = $3, locked_at = NULL
WHERE id = $1 AND created_at = $2 AND status = 'PUBLISHING'
RETURNING aggregate_id, aggregate_type, event_type, payload, event_version, aggregate_version;

-- name: OutboxDeadLetterInsert
INSERT INTO outbox_dead_letters
    (id, original_event_id, aggregate_id, aggregate_type, event_type, payload, event_version, aggregate_version, failure_reason, failed_at)
VALUES
    (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, NOW());

-- name: OutboxPollPending
UPDATE outbox_events o
SET status = 'PUBLISHING', locked_at = NOW()
FROM (
    SELECT id, created_at
    FROM outbox_events
    WHERE shard_index = ANY($1::int[])
      AND next_attempt_at <= NOW()
      AND (
            status = 'PENDING'
         OR (status = 'PUBLISHING' AND locked_at < NOW() - make_interval(secs => $3))
      )
    ORDER BY attempts ASC, created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $2
) AS claimed
WHERE o.id = claimed.id AND o.created_at = claimed.created_at
RETURNING o.id, o.aggregate_id, o.aggregate_type, o.event_type,
          o.payload, o.event_version, o.aggregate_version, o.attempts, o.created_at;

-- name: OutboxDeadLetterGet
SELECT aggregate_id, aggregate_type, event_type, payload, event_version, aggregate_version, resolved_at
FROM outbox_dead_letters
WHERE id = $1;

-- name: OutboxDeadLetterResolve
UPDATE outbox_dead_letters
SET resolved_at = NOW(), resolved_by = $2
WHERE id = $1;

-- name: OutboxReplayInsert
INSERT INTO outbox_events
    (id, aggregate_id, aggregate_type, event_type, payload,
     event_version, aggregate_version, status, created_at, next_attempt_at, attempts)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, 'PENDING', NOW(), NOW(), 0);

-- name: MerchantWebhookInsert
INSERT INTO merchant_webhook_deliveries
    (id, merchant_id, transaction_id, event_type, payload, endpoint_url,
     status, attempts, next_attempt_at, created_at)
VALUES
    ($1, $2, $3, $4, $5, $6, 'PENDING', 0, NOW(), NOW());