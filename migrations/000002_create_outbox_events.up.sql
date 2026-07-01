CREATE TABLE outbox_events (
    id              UUID        NOT NULL,
    aggregate_id    UUID        NOT NULL,
    aggregate_type  TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    event_version   INT         NOT NULL DEFAULT 1 CHECK (event_version >= 1),
    status          TEXT        NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    shard_index     INT         NOT NULL GENERATED ALWAYS AS (ABS(MOD(hashtext(aggregate_id::TEXT), 64))) STORED,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts        INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error      TEXT,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

CREATE TABLE outbox_dead_letters (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    original_event_id UUID        NOT NULL,
    aggregate_id      UUID        NOT NULL,
    aggregate_type    TEXT        NOT NULL,
    event_type        TEXT        NOT NULL,
    payload           JSONB       NOT NULL,
    event_version     INT         NOT NULL DEFAULT 1,
    failure_reason    TEXT        NOT NULL,
    failed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at       TIMESTAMPTZ,
    resolved_by       TEXT
);

CREATE TABLE partition_management_log (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_name TEXT        NOT NULL,
    action         TEXT        NOT NULL CHECK (action IN ('create', 'detach', 'drop')),
    wal_lag_mb     BIGINT,
    replica_lag_ms INT,
    duration_ms    INT,
    executed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE outbox_2026_W19 PARTITION OF outbox_events FOR VALUES FROM ('2026-05-04 00:00:00+00') TO ('2026-05-11 00:00:00+00');
CREATE TABLE outbox_2026_W20 PARTITION OF outbox_events FOR VALUES FROM ('2026-05-11 00:00:00+00') TO ('2026-05-18 00:00:00+00');
CREATE TABLE outbox_2026_W21 PARTITION OF outbox_events FOR VALUES FROM ('2026-05-18 00:00:00+00') TO ('2026-05-25 00:00:00+00');
CREATE TABLE outbox_default PARTITION OF outbox_events DEFAULT;
CREATE INDEX idx_outbox_shard ON outbox_events (shard_index, status, next_attempt_at, attempts, created_at) WHERE status = 'PENDING';
CREATE INDEX idx_dead_letters_unresolved ON outbox_dead_letters (failed_at DESC) WHERE resolved_at IS NULL;
CREATE INDEX idx_dead_letters_original_event ON outbox_dead_letters (original_event_id);
CREATE INDEX idx_partition_log_name_action ON partition_management_log (partition_name, action, executed_at DESC);