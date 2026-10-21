ALTER TABLE transactions ADD COLUMN review_required_at TIMESTAMPTZ;
ALTER TABLE transactions ADD COLUMN review_reason TEXT;
CREATE INDEX idx_transactions_review_required ON transactions (review_required_at) WHERE review_required_at IS NOT NULL;
