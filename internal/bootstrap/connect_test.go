package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConnect_SucceedsAfterRetries(t *testing.T) {
	calls := 0
	policy := RetryPolicy{Attempts: 5, AttemptTimeout: time.Second, Backoff: time.Millisecond}

	err := Connect(context.Background(), nil, "dep", policy, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("not ready")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls)
	}
}

func TestConnect_FailsAfterMaxAttempts(t *testing.T) {
	calls := 0
	policy := RetryPolicy{Attempts: 3, AttemptTimeout: time.Second, Backoff: time.Millisecond}

	err := Connect(context.Background(), nil, "dep", policy, func(context.Context) error {
		calls++
		return errors.New("down")
	})
	if err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
	if calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls)
	}
}

func TestConnect_AbortsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	policy := RetryPolicy{Attempts: 5, AttemptTimeout: time.Second, Backoff: time.Minute}

	err := Connect(ctx, nil, "dep", policy, func(context.Context) error {
		return errors.New("down")
	})
	if err == nil {
		t.Fatal("expected error when context is cancelled")
	}
}
