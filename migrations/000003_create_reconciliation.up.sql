CREATE TABLE reconciliation_jobs (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    gateway_id     TEXT        NOT NULL,
    transaction_id UUID        REFERENCES transactions(id),
    period_start   TIMESTAMPTZ,
    period_end     TIMESTAMPTZ,
    status         TEXT        NOT NULL DEFAULT 'PENDING'
                   CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    triggered_by   TEXT        NOT NULL CHECK (triggered_by IN ('system', 'ops')),
    actor          TEXT        NOT NULL DEFAULT 'system',
    mismatch_count INT         NOT NULL DEFAULT 0,
    error          TEXT,
    started_at     TIMESTAMPTZ,
    completed_at   TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT job_scope CHECK (
        (transaction_id IS NOT NULL AND period_start IS NULL     AND period_end IS NULL)
        OR
        (transaction_id IS NULL     AND period_start IS NOT NULL AND period_end IS NOT NULL)
    )
);

CREATE TABLE reconciliation_entries (
    id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                 UUID        NOT NULL REFERENCES reconciliation_jobs(id),
    transaction_id         UUID        NOT NULL REFERENCES transactions(id),
    internal_status        TEXT        NOT NULL,
    gateway_status         TEXT        NOT NULL,
    internal_amount        BIGINT      NOT NULL,
    gateway_amount         BIGINT      NOT NULL,
    internal_fees          BIGINT,
    gateway_fees           BIGINT,
    fx_rate_applied        NUMERIC(18,8),
    fx_rate_at_settlement  NUMERIC(18,8),
    fee_mismatch_reason    TEXT,
    mismatch_type          TEXT        NOT NULL
                           CHECK (mismatch_type IN ('STATUS_MISMATCH', 'AMOUNT_MISMATCH', 'FEE_MISMATCH', 'MISSING_INTERNAL', 'MISSING_GATEWAY')),
    resolution_status      TEXT        NOT NULL DEFAULT 'UNRESOLVED'
                           CHECK (resolution_status IN ('UNRESOLVED', 'RESOLVED', 'AUTO_REFUND_ISSUED', 'AUTO_INVOICED')),
    resolved_by            TEXT,
    resolved_at            TIMESTAMPTZ,
    notes                  TEXT,
    auto_resolution_action TEXT,
    auto_resolution_at     TIMESTAMPTZ,
    auto_resolution_by     TEXT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE settlement_auto_resolution_log (
    id                       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    settlement_id            UUID        NOT NULL,
    discrepancy_amount_paise BIGINT      NOT NULL,
    threshold_bps            INT         NOT NULL,
    absolute_cap_paise       BIGINT      NOT NULL,
    qualified_percentage     BOOLEAN     NOT NULL,
    qualified_absolute       BOOLEAN     NOT NULL,
    action                   TEXT,
    executed_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executed_by              TEXT        NOT NULL DEFAULT 'system'
);

CREATE INDEX idx_reconciliation_jobs_gateway_status ON reconciliation_jobs (gateway_id, status, created_at DESC);
CREATE INDEX idx_reconciliation_jobs_transaction ON reconciliation_jobs (transaction_id) WHERE transaction_id IS NOT NULL;
CREATE INDEX idx_reconciliation_entries_job ON reconciliation_entries (job_id);
CREATE INDEX idx_reconciliation_entries_transaction ON reconciliation_entries (transaction_id);
CREATE INDEX idx_reconciliation_entries_unresolved ON reconciliation_entries (mismatch_type, created_at DESC) WHERE resolution_status = 'UNRESOLVED';
CREATE INDEX idx_auto_resolution_log_settlement ON settlement_auto_resolution_log (settlement_id, executed_at DESC);