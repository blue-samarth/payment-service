package transaction

import (
	"errors"
	"testing"
	"time"
)

func TestTransitionState_ValidTransitions(t *testing.T) {
	cases := []struct {
		from Status
		to   Status
	}{
		{StatusPending, StatusProcessing},
		{StatusPending, StatusCancelled},
		{StatusPending, StatusFailed},
		{StatusProcessing, StatusSucceeded},
		{StatusProcessing, StatusFailed},
		{StatusSucceeded, StatusRefunded},
		{StatusSucceeded, StatusRefundFailed},
		{StatusRefundFailed, StatusRefunded},
	}
	for _, c := range cases {
		tx := &Transaction{Status: c.from}
		if err := TransitionState(tx, c.to, ActorSystem); err != nil {
			t.Errorf("%s → %s: unexpected error %v", c.from, c.to, err)
		}
		if tx.Status != c.to {
			t.Errorf("%s → %s: status not updated, got %s", c.from, c.to, tx.Status)
		}
	}
}

func TestTransitionState_InvalidTransitions(t *testing.T) {
	cases := []struct {
		from Status
		to   Status
	}{
		{StatusPending, StatusSucceeded},
		{StatusPending, StatusRefunded},
		{StatusProcessing, StatusCancelled},
		{StatusSucceeded, StatusProcessing},
		{StatusCancelled, StatusPending},
		{StatusRefunded, StatusRefundFailed},
		{StatusProcessing, StatusPending},
	}
	for _, c := range cases {
		tx := &Transaction{Status: c.from}
		err := TransitionState(tx, c.to, ActorSystem)
		var invalid ErrInvalidTransition
		if !errors.As(err, &invalid) {
			t.Errorf("%s → %s: expected ErrInvalidTransition, got %v", c.from, c.to, err)
		}
		if tx.Status != c.from {
			t.Errorf("%s → %s: status mutated on invalid transition, got %s", c.from, c.to, tx.Status)
		}
	}
}

func TestTransitionState_TerminalStatesHaveNoTransitions(t *testing.T) {
	for _, terminal := range []Status{StatusCancelled, StatusRefunded} {
		for _, to := range AllStatuses() {
			tx := &Transaction{Status: terminal}
			if err := TransitionState(tx, to, ActorSystem); err == nil {
				t.Errorf("terminal %s → %s should be rejected", terminal, to)
			}
		}
	}
}

func TestTransitionState_FailedToCancelledRequiresCancelIntent(t *testing.T) {
	tx := &Transaction{Status: StatusFailed, CancelIntent: false}
	if err := TransitionState(tx, StatusCancelled, ActorSystem); err == nil {
		t.Fatal("expected error for FAILED → CANCELLED without cancel intent")
	}
	if tx.Status != StatusFailed {
		t.Errorf("status should remain FAILED, got %s", tx.Status)
	}

	tx = &Transaction{Status: StatusFailed, CancelIntent: true}
	if err := TransitionState(tx, StatusCancelled, ActorSystem); err != nil {
		t.Fatalf("expected success for FAILED → CANCELLED with cancel intent, got %v", err)
	}
}

func TestTransitionState_ClearsLeaseFieldsLeavingProcessing(t *testing.T) {
	now := time.Now().UTC()
	timeout := 30 * time.Second
	for _, to := range []Status{StatusSucceeded, StatusFailed} {
		tx := &Transaction{
			Status:              StatusProcessing,
			ProcessingStartedAt: &now,
			ProcessingTimeout:   &timeout,
		}
		if err := TransitionState(tx, to, ActorSystem); err != nil {
			t.Fatalf("PROCESSING → %s: %v", to, err)
		}
		if tx.ProcessingStartedAt != nil || tx.ProcessingTimeout != nil {
			t.Errorf("PROCESSING → %s: lease fields not cleared", to)
		}
	}
}

func TestTransitionState_NilTransaction(t *testing.T) {
	if err := TransitionState(nil, StatusProcessing, ActorSystem); err == nil {
		t.Fatal("expected error for nil transaction")
	}
}

func TestTransitionState_UpdatesTimestamp(t *testing.T) {
	tx := &Transaction{Status: StatusPending, UpdatedAt: time.Unix(0, 0)}
	if err := TransitionState(tx, StatusProcessing, ActorSystem); err != nil {
		t.Fatal(err)
	}
	if tx.UpdatedAt.Equal(time.Unix(0, 0)) {
		t.Error("UpdatedAt not refreshed on transition")
	}
}

func TestValidTransitionsFrom_ReturnsCopy(t *testing.T) {
	got := ValidTransitionsFrom(StatusPending)
	if len(got) != 3 {
		t.Fatalf("expected 3 transitions from PENDING, got %d", len(got))
	}
	got[0] = StatusRefunded
	again := ValidTransitionsFrom(StatusPending)
	if again[0] == StatusRefunded {
		t.Error("ValidTransitionsFrom returned a mutable reference to the internal table")
	}
}

func TestAllStatuses(t *testing.T) {
	if len(AllStatuses()) != 7 {
		t.Errorf("expected 7 statuses, got %d", len(AllStatuses()))
	}
}
