DROP INDEX IF EXISTS idx_refunds_review_required;
DROP INDEX IF EXISTS idx_refunds_recoverable;
ALTER TABLE refunds DROP COLUMN IF EXISTS review_reason;
ALTER TABLE refunds DROP COLUMN IF EXISTS review_required_at;
