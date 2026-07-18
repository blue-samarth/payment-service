ALTER TABLE outbox_events ADD COLUMN aggregate_version INT NOT NULL DEFAULT 0;
ALTER TABLE outbox_dead_letters ADD COLUMN aggregate_version INT NOT NULL DEFAULT 0;
