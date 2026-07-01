package refund

import (
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestNew_Valid(t *testing.T) {
	rf, err := New(uuid.New(), 50000, 150000, 0, "customer_request", "support:agent1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rf.Status != StatusInitiated {
		t.Errorf("expected REFUND_INITIATED, got %s", rf.Status)
	}
	if rf.Attempts != 0 {
		t.Errorf("expected 0 attempts, got %d", rf.Attempts)
	}
	if rf.ID == uuid.Nil {
		t.Error("expected generated ID")
	}
}

func TestNew_Validation(t *testing.T) {
	cases := []struct {
		name                                         string
		amount, original, already                    int64
		reason, initiatedBy                          string
	}{
		{"zero amount", 0, 150000, 0, "r", "by"},
		{"negative amount", -1, 150000, 0, "r", "by"},
		{"empty reason", 50000, 150000, 0, "", "by"},
		{"empty initiatedBy", 50000, 150000, 0, "r", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := New(uuid.New(), c.amount, c.original, c.already, c.reason, c.initiatedBy); err == nil {
				t.Errorf("expected error for %s", c.name)
			}
		})
	}
}

func TestNew_OverRefund(t *testing.T) {
	t.Run("exceeds", func(t *testing.T) {
		_, err := New(uuid.New(), 60000, 100000, 60000, "r", "by")
		var over ErrOverRefund
		if !errors.As(err, &over) {
			t.Fatalf("expected ErrOverRefund, got %v", err)
		}
	})
	t.Run("exact boundary allowed", func(t *testing.T) {
		if _, err := New(uuid.New(), 40000, 100000, 60000, "r", "by"); err != nil {
			t.Errorf("exact boundary (already+amount == original) should be allowed, got %v", err)
		}
	})
	t.Run("one over boundary rejected", func(t *testing.T) {
		if _, err := New(uuid.New(), 40001, 100000, 60000, "r", "by"); err == nil {
			t.Error("one paise over boundary should be rejected")
		}
	})
}

func TestTransition_Valid(t *testing.T) {
	cases := []struct {
		from Status
		to   Status
	}{
		{StatusInitiated, StatusProcessing},
		{StatusInitiated, StatusFailed},
		{StatusProcessing, StatusRefunded},
		{StatusProcessing, StatusFailed},
	}
	for _, c := range cases {
		rf := &Refund{Status: c.from}
		if err := rf.Transition(c.to); err != nil {
			t.Errorf("%s → %s: unexpected error %v", c.from, c.to, err)
		}
		if rf.Status != c.to {
			t.Errorf("%s → %s: status not updated", c.from, c.to)
		}
	}
}

func TestTransition_Invalid(t *testing.T) {
	cases := []struct {
		from Status
		to   Status
	}{
		{StatusInitiated, StatusRefunded},
		{StatusRefunded, StatusFailed},
		{StatusFailed, StatusProcessing},
		{StatusProcessing, StatusInitiated},
	}
	for _, c := range cases {
		rf := &Refund{Status: c.from}
		err := rf.Transition(c.to)
		var invalid ErrInvalidTransition
		if !errors.As(err, &invalid) {
			t.Errorf("%s → %s: expected ErrInvalidTransition, got %v", c.from, c.to, err)
		}
	}
}

func TestTransition_SetsResolvedAtOnTerminal(t *testing.T) {
	rf := &Refund{Status: StatusProcessing}
	if err := rf.Transition(StatusRefunded); err != nil {
		t.Fatal(err)
	}
	if rf.ResolvedAt == nil {
		t.Error("expected ResolvedAt set on terminal transition")
	}

	rf2 := &Refund{Status: StatusInitiated}
	if err := rf2.Transition(StatusProcessing); err != nil {
		t.Fatal(err)
	}
	if rf2.ResolvedAt != nil {
		t.Error("ResolvedAt should remain nil on non-terminal transition")
	}
}

func TestStatus_IsTerminal(t *testing.T) {
	want := map[Status]bool{
		StatusRefunded: true, StatusFailed: true,
		StatusInitiated: false, StatusProcessing: false,
	}
	for s, w := range want {
		if s.IsTerminal() != w {
			t.Errorf("%s.IsTerminal() = %v, want %v", s, s.IsTerminal(), w)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	if !(&Refund{Status: StatusFailed}).IsRetryable() {
		t.Error("failed refund should be retryable")
	}
	if (&Refund{Status: StatusRefunded}).IsRetryable() {
		t.Error("refunded refund should not be retryable")
	}
}
