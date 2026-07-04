CREATE TABLE transactions (
    id                       UUID        PRIMARY KEY,
    merchant_id              UUID        NOT NULL,
    amount                   BIGINT      NOT NULL CHECK (amount > 0),
    currency                 CHAR(3)     NOT NULL CHECK (currency = upper(currency)),
    payment_method           TEXT        NOT NULL
                             CHECK (payment_method IN ('card', 'upi', 'netbanking', 'wallet')),
    status                   TEXT        NOT NULL DEFAULT 'PENDING'
                             CHECK (status IN (
                                 'PENDING', 'PROCESSING', 'SUCCEEDED', 'FAILED',
                                 'CANCELLED', 'REFUNDED', 'REFUND_FAILED'
                             )),
    version                  INT         NOT NULL DEFAULT 1 CHECK (version >= 1),

    gateway_id               TEXT        NOT NULL,
    gateway_reference_id     TEXT,
    gateway_idempotency_key  TEXT,
    attempted_gateway        TEXT,
    actual_gateway           TEXT,
    original_gateway         TEXT,       -- set only on retry to alternative gateway

    estimated_timeout_seconds INT        NOT NULL CHECK (estimated_timeout_seconds > 0),
    failure_reason           JSONB,
    method_details           JSONB,
    metadata                 JSONB,
    CONSTRAINT metadata_size CHECK (pg_column_size(metadata) <= 4096),
    description              TEXT,
    customer_id              UUID,
    customer_email           TEXT,
    cancel_intent            BOOLEAN     NOT NULL DEFAULT false,
    cancel_requested_by      TEXT
                             CHECK (cancel_requested_by IN ('system', 'merchant', 'ops', 'gateway') OR cancel_requested_by IS NULL),
    cancel_requested_at      TIMESTAMPTZ,
    cancel_requested_via     TEXT
                             CHECK (cancel_requested_via IN ('api', 'dashboard', 'ops-tool') OR cancel_requested_via IS NULL),
    processing_started_at    TIMESTAMPTZ,
    processing_timeout       INTERVAL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE idempotency_keys (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    composite_key_hash TEXT        NOT NULL UNIQUE,
    request_hash       TEXT        NOT NULL,
    response           JSONB       NOT NULL,
    status             TEXT        NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at         TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours')
);

CREATE TABLE processing_lease (
    idempotency_key   UUID        PRIMARY KEY,
    payment_intent_id UUID        NOT NULL REFERENCES transactions(id),
    lease_acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_ttl_sec     INT         NOT NULL DEFAULT 30 CHECK (lease_ttl_sec > 0),
    cached_response   JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE transaction_raw_metadata (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID        NOT NULL REFERENCES transactions(id),
    gateway_id     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    captured_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE audit_log (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID        NOT NULL REFERENCES transactions(id),
    event_type     TEXT        NOT NULL,
    actor          TEXT        NOT NULL
                   CHECK (actor IN ('system', 'merchant', 'ops', 'gateway')),
    previous_state TEXT,
    new_state      TEXT        NOT NULL,
    metadata       JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE webhook_events (
    event_id    TEXT        NOT NULL,
    gateway_id  TEXT        NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, gateway_id)
);

CREATE TABLE refunds (
    id                 UUID        PRIMARY KEY,
    transaction_id     UUID        NOT NULL REFERENCES transactions(id),
    amount             BIGINT      NOT NULL CHECK (amount > 0),
    reason             TEXT,
    status             TEXT        NOT NULL DEFAULT 'REFUND_INITIATED'
                       CHECK (status IN ('REFUND_INITIATED', 'REFUND_PROCESSING', 'REFUNDED', 'REFUND_FAILED')),
    initiated_by       TEXT        NOT NULL,
    gateway_refund_id  TEXT,
    attempted_gateway  TEXT,
    actual_gateway     TEXT,
    attempts           INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    failure_reason     JSONB,
    initiated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at        TIMESTAMPTZ
);

CREATE TABLE merchant_webhook_deliveries (
    id              UUID        PRIMARY KEY,
    merchant_id     UUID        NOT NULL,
    transaction_id  UUID        NOT NULL REFERENCES transactions(id),
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    endpoint_url    TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'DELIVERED', 'FAILED')),
    attempts        INT         NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transactions_merchant_status ON transactions (merchant_id, status);
CREATE INDEX idx_transactions_gateways ON transactions (attempted_gateway, actual_gateway) WHERE actual_gateway IS NOT NULL;
CREATE INDEX idx_transactions_lease_expiry ON transactions (processing_started_at, processing_timeout) WHERE status = 'PROCESSING';
CREATE INDEX idx_transactions_gateway_reference ON transactions (gateway_reference_id) WHERE gateway_reference_id IS NOT NULL;
CREATE INDEX idx_transactions_metadata ON transactions USING GIN (metadata);
CREATE INDEX idx_idempotency_keys_expires ON idempotency_keys (expires_at);
CREATE INDEX idx_raw_metadata_transaction ON transaction_raw_metadata (transaction_id, captured_at DESC);
CREATE INDEX idx_audit_log_transaction ON audit_log (transaction_id, created_at ASC);
CREATE INDEX idx_refunds_transaction_status ON refunds (transaction_id, status);
CREATE INDEX idx_merchant_webhook_pending ON merchant_webhook_deliveries (status, next_attempt_at) WHERE status = 'PENDING';
CREATE INDEX idx_merchant_webhook_transaction ON merchant_webhook_deliveries (transaction_id);