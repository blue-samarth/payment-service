ALTER TABLE partition_management_log ADD COLUMN IF NOT EXISTS wal_lag_mb BIGINT;
