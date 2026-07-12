package bootstrap

import (
	"context"
	"fmt"
	"time"

	"samarth/payment-service/internal/ports"
)

type RetryPolicy struct {
	Attempts       int
	AttemptTimeout time.Duration
	Backoff        time.Duration
}

func Connect(ctx context.Context, log ports.Logger, name string, policy RetryPolicy, fn func(context.Context) error) error {
	if policy.Attempts < 1 {
		policy.Attempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= policy.Attempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, policy.AttemptTimeout)
		lastErr = fn(attemptCtx)
		cancel()
		if lastErr == nil {
			return nil
		}

		if log != nil {
			log.Warn("startup.connect_retry", map[string]any{
				"dependency":   name,
				"attempt":      attempt,
				"max_attempts": policy.Attempts,
				"error":        lastErr.Error(),
			})
		}

		if attempt == policy.Attempts {
			break
		}
		select {
		case <-time.After(policy.Backoff):
		case <-ctx.Done():
			return fmt.Errorf("%s: connect aborted: %w", name, ctx.Err())
		}
	}
	return fmt.Errorf("%s: not reachable after %d attempts: %w", name, policy.Attempts, lastErr)
}
