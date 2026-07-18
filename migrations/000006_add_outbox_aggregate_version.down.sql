ALTER TABLE outbox_events DROP COLUMN IF EXISTS aggregate_version;
ALTER TABLE outbox_dead_letters DROP COLUMN IF EXISTS aggregate_version;
