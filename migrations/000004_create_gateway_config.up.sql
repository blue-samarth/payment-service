CREATE TABLE gateway_config (
    gateway_id               TEXT        PRIMARY KEY,
    display_name             TEXT        NOT NULL,
    is_active                BOOLEAN     NOT NULL DEFAULT true,
    min_amount               BIGINT      NOT NULL DEFAULT 0,
    max_amount               BIGINT      NOT NULL,
    supported_currencies     TEXT[]      NOT NULL DEFAULT '{}',
    supported_methods        TEXT[]      NOT NULL DEFAULT '{}',
    idempotency_capable      BOOLEAN     NOT NULL DEFAULT true,
    supports_cancel          BOOLEAN     NOT NULL DEFAULT false,
    supports_partial_refund  BOOLEAN     NOT NULL DEFAULT false,
    priority                 INT         NOT NULL DEFAULT 100,
    credentials_secret_arn   TEXT,
    webhook_secret_arn       TEXT,
    webhook_ip_allowlist     TEXT[],
    webhook_replay_window_sec INT        NOT NULL DEFAULT 300,
    webhook_clock_skew_sec   INT         NOT NULL DEFAULT 30,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE gateway_timeouts (
    gateway_id                TEXT NOT NULL REFERENCES gateway_config(gateway_id),
    payment_method            TEXT NOT NULL CHECK (payment_method IN ('card', 'upi', 'netbanking', 'wallet')),
    gateway_timeout_sec       INT  NOT NULL,
    payment_method_buffer_sec INT  NOT NULL,
    -- Always consistent with its components; never set manually.
    estimated_timeout_sec     INT  NOT NULL GENERATED ALWAYS AS (gateway_timeout_sec + payment_method_buffer_sec) STORED,
    PRIMARY KEY (gateway_id, payment_method)
);

CREATE TABLE gateway_fee_models (
    gateway_id                              TEXT        NOT NULL REFERENCES gateway_config(gateway_id),
    payment_method                          TEXT        NOT NULL CHECK (payment_method IN ('card', 'upi', 'netbanking', 'wallet')),
    fixed_paise                             BIGINT      NOT NULL DEFAULT 0,
    percentage_bps                          BIGINT      NOT NULL DEFAULT 0,
    interchange_cap_paise                   BIGINT,
    discount_volume_threshold_paise         BIGINT      NOT NULL DEFAULT 0,
    created_at                              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (gateway_id, payment_method)
);

-- Written hourly by the fee_snapshot job; used as Redis fallback during outages.
CREATE TABLE gateway_fees_snapshot (
    gateway_id                              TEXT        NOT NULL,
    payment_method                          TEXT        NOT NULL,
    fixed_paise                             BIGINT      NOT NULL DEFAULT 0,
    percentage_bps                          BIGINT      NOT NULL DEFAULT 0,
    interchange_cap_paise                   BIGINT,
    discount_volume_threshold_paise         BIGINT      NOT NULL DEFAULT 0,
    snapshotted_at                          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (gateway_id, payment_method)
);

CREATE TABLE gateway_metadata_schemas (
    gateway_id     TEXT        PRIMARY KEY REFERENCES gateway_config(gateway_id),
    allowed_keys   TEXT[]      NOT NULL DEFAULT '{}',
    required_keys  TEXT[]      NOT NULL DEFAULT '{}',
    max_size_bytes INT         NOT NULL DEFAULT 4096,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE gateway_routing_weights (
    merchant_tier       TEXT         PRIMARY KEY,
    volume_score        NUMERIC(5,4) NOT NULL,
    cost_score          NUMERIC(5,4) NOT NULL,
    reliability_score   NUMERIC(5,4) NOT NULL,
    fx_efficiency_score NUMERIC(5,4) NOT NULL,
    latency_score       NUMERIC(5,4) NOT NULL,
    -- ROUND avoids binary float precision issues (e.g. 0.35+0.25+0.20+0.10+0.10).
    CONSTRAINT weights_sum CHECK (ROUND(volume_score + cost_score + reliability_score + fx_efficiency_score + latency_score, 4) = 1.0000),
    updated_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO gateway_routing_weights (merchant_tier, volume_score, cost_score, reliability_score, fx_efficiency_score, latency_score)
VALUES ('default', 0.35, 0.25, 0.20, 0.10, 0.10);

CREATE TABLE gateway_circuit_breaker_state (
    gateway_id                   TEXT        PRIMARY KEY REFERENCES gateway_config(gateway_id),
    state                        TEXT        NOT NULL DEFAULT 'CLOSED' CHECK (state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    cooldown_until               TIMESTAMPTZ,
    consecutive_failures         INT         NOT NULL DEFAULT 0,
    last_known_reliability_score INT         NOT NULL DEFAULT 100,
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE gateway_metrics (
    gateway_id               TEXT         PRIMARY KEY REFERENCES gateway_config(gateway_id),
    discrepancy_rate_5min    NUMERIC(6,5) NOT NULL DEFAULT 0,
    discrepancy_rate_30min   NUMERIC(6,5) NOT NULL DEFAULT 0,
    discrepancy_rate_24h     NUMERIC(6,5) NOT NULL DEFAULT 0,
    last_discrepancy_at      TIMESTAMPTZ,
    days_since_discrepancy   INT          NOT NULL DEFAULT 0,
    p99_latency_ms           INT          NOT NULL DEFAULT 0,
    volume_7d                BIGINT       NOT NULL DEFAULT 0,
    fx_efficiency_ratio      NUMERIC(8,6) NOT NULL DEFAULT 1.0,
    active_payment_intents   INT          NOT NULL DEFAULT 0,
    last_updated             TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE gateway_config_history (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    gateway_id     TEXT        NOT NULL,
    changed_fields TEXT[]      NOT NULL,
    previous_value JSONB,
    new_value      JSONB,
    changed_by     TEXT        NOT NULL,
    changed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_gateway_config_history_gateway ON gateway_config_history (gateway_id, changed_at DESC);