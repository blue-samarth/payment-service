DROP INDEX IF EXISTS idx_transactions_review_required;
ALTER TABLE transactions DROP COLUMN IF EXISTS review_reason;
ALTER TABLE transactions DROP COLUMN IF EXISTS review_required_at;
