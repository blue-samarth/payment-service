ALTER TABLE refunds ADD COLUMN review_required_at TIMESTAMPTZ;
ALTER TABLE refunds ADD COLUMN review_reason TEXT;
CREATE INDEX idx_refunds_recoverable ON refunds (initiated_at)
    WHERE status IN ('REFUND_INITIATED', 'REFUND_PROCESSING') AND review_required_at IS NULL;
CREATE INDEX idx_refunds_review_required ON refunds (review_required_at) WHERE review_required_at IS NOT NULL;
