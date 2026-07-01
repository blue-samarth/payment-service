-- name: PartitionAdvisoryLock
SELECT pg_try_advisory_lock(42);

-- name: PartitionAdvisoryUnlock
SELECT pg_advisory_unlock(42);

-- name: PartitionList
SELECT c.relname
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
WHERE p.relname = 'outbox_events'
  AND c.relname <> 'outbox_default';

-- name: PartitionLogAction
INSERT INTO partition_management_log (partition_name, action) VALUES ($1, $2);

-- name: PartitionDetachedBefore
SELECT DISTINCT l.partition_name
FROM partition_management_log l
WHERE l.action = 'detach'
  AND l.executed_at < $1
  AND NOT EXISTS (
    SELECT 1 FROM partition_management_log d
    WHERE d.partition_name = l.partition_name AND d.action = 'drop'
  );
