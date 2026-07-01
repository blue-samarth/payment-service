package postgres

import (
	"embed"
	"fmt"
	"strings"
)

//go:embed queries/*.sql
var sqlFiles embed.FS

type Queries struct {
	OutboxInsert            string
	OutboxMarkPublished     string
	OutboxMarkFailed        string
	OutboxMarkExhausted     string
	OutboxDeadLetterInsert  string
	OutboxPollPending       string
	OutboxDeadLetterGet     string
	OutboxDeadLetterResolve string
	OutboxReplayInsert      string
	MerchantWebhookInsert   string

	TransactionInsert            string
	TransactionGetByID           string
	TransactionUpdateStatus      string
	TransactionSetCancelIntent   string
	TransactionListExpiredLeases string

	RefundInsert         string
	RefundGetByID        string
	RefundSumActive      string
	RefundUpdateStatus   string
	RefundLockParent     string
	RefundExistsByReason string

	LeaseAcquire     string
	LeaseGetCached   string
	LeaseWriteCached string

	PartitionAdvisoryLock   string
	PartitionAdvisoryUnlock string
	PartitionList           string
	PartitionLogAction      string
	PartitionDetachedBefore string

	IdempotencyReserve              string
	IdempotencyLookup               string
	IdempotencyComplete             string
	IdempotencyRelease              string
	IdempotencySweepStaleProcessing string
	IdempotencyDeleteExpired        string

	WebhookEventRecord         string
	TransactionGetByGatewayRef string
	RawMetadataInsert          string
}

func LoadQueries() (*Queries, error) {
	named, err := parseAll()
	if err != nil {
		return nil, err
	}

	get := func(name string) (string, error) {
		q, ok := named[name]
		if !ok {
			return "", fmt.Errorf("queries: missing query %q", name)
		}
		return q, nil
	}

	var q Queries
	var errs []string

	fields := []struct {
		dest *string
		name string
	}{
		{&q.OutboxInsert, "OutboxInsert"},
		{&q.OutboxMarkPublished, "OutboxMarkPublished"},
		{&q.OutboxMarkFailed, "OutboxMarkFailed"},
		{&q.OutboxMarkExhausted, "OutboxMarkExhausted"},
		{&q.OutboxDeadLetterInsert, "OutboxDeadLetterInsert"},
		{&q.OutboxPollPending, "OutboxPollPending"},
		{&q.OutboxDeadLetterGet, "OutboxDeadLetterGet"},
		{&q.OutboxDeadLetterResolve, "OutboxDeadLetterResolve"},
		{&q.OutboxReplayInsert, "OutboxReplayInsert"},
		{&q.MerchantWebhookInsert, "MerchantWebhookInsert"},
		{&q.TransactionInsert, "TransactionInsert"},
		{&q.TransactionGetByID, "TransactionGetByID"},
		{&q.TransactionUpdateStatus, "TransactionUpdateStatus"},
		{&q.TransactionSetCancelIntent, "TransactionSetCancelIntent"},
		{&q.TransactionListExpiredLeases, "TransactionListExpiredLeases"},
		{&q.RefundInsert, "RefundInsert"},
		{&q.RefundGetByID, "RefundGetByID"},
		{&q.RefundSumActive, "RefundSumActive"},
		{&q.RefundUpdateStatus, "RefundUpdateStatus"},
		{&q.RefundLockParent, "RefundLockParent"},
		{&q.RefundExistsByReason, "RefundExistsByReason"},
		{&q.LeaseAcquire, "LeaseAcquire"},
		{&q.LeaseGetCached, "LeaseGetCached"},
		{&q.LeaseWriteCached, "LeaseWriteCached"},
		{&q.PartitionAdvisoryLock, "PartitionAdvisoryLock"},
		{&q.PartitionAdvisoryUnlock, "PartitionAdvisoryUnlock"},
		{&q.PartitionList, "PartitionList"},
		{&q.PartitionLogAction, "PartitionLogAction"},
		{&q.PartitionDetachedBefore, "PartitionDetachedBefore"},
		{&q.IdempotencyReserve, "IdempotencyReserve"},
		{&q.IdempotencyLookup, "IdempotencyLookup"},
		{&q.IdempotencyComplete, "IdempotencyComplete"},
		{&q.IdempotencyRelease, "IdempotencyRelease"},
		{&q.IdempotencySweepStaleProcessing, "IdempotencySweepStaleProcessing"},
		{&q.IdempotencyDeleteExpired, "IdempotencyDeleteExpired"},
		{&q.WebhookEventRecord, "WebhookEventRecord"},
		{&q.TransactionGetByGatewayRef, "TransactionGetByGatewayRef"},
		{&q.RawMetadataInsert, "RawMetadataInsert"},
	}

	for _, f := range fields {
		v, err := get(f.name)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		*f.dest = v
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("queries: load failed:\n  %s", strings.Join(errs, "\n  "))
	}

	return &q, nil
}

func parseAll() (map[string]string, error) {
	entries, err := sqlFiles.ReadDir("queries")
	if err != nil {
		return nil, fmt.Errorf("queries: read dir: %w", err)
	}

	result := make(map[string]string)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		data, err := sqlFiles.ReadFile("queries/" + entry.Name())
		if err != nil {
			return nil, fmt.Errorf("queries: read %s: %w", entry.Name(), err)
		}

		parsed, err := parseFile(string(data), entry.Name())
		if err != nil {
			return nil, err
		}

		for name, sql := range parsed {
			if _, exists := result[name]; exists {
				return nil, fmt.Errorf("queries: duplicate query name %q in %s", name, entry.Name())
			}
			result[name] = sql
		}
	}

	return result, nil
}
func parseFile(content, filename string) (map[string]string, error) {
	result := make(map[string]string)
	var currentName string
	var currentLines []string

	flush := func() {
		if currentName == "" {
			return
		}
		sql := strings.TrimSpace(strings.Join(currentLines, "\n"))
		if sql != "" {
			result[currentName] = sql
		}
	}

	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "-- name:") {
			flush()
			currentName = strings.TrimSpace(strings.TrimPrefix(trimmed, "-- name:"))
			currentLines = nil
			if currentName == "" {
				return nil, fmt.Errorf("queries: empty name marker in %s", filename)
			}
			continue
		}

		if currentName != "" {
			currentLines = append(currentLines, line)
		}
	}

	flush()
	return result, nil
}
