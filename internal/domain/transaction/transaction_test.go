package transaction

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func validNewArgs() (uuid.UUID, int64, string, PaymentMethod, string, uuid.UUID, string, string, map[string]any, int) {
	return uuid.New(), 150000, "INR", PaymentMethodCard, "razorpay", uuid.New(), "buyer@example.com", "order #42", nil, 30
}

func TestNew_Valid(t *testing.T) {
	m, a, c, pm, g, cust, email, desc, meta, to := validNewArgs()
	tx, err := New(m, a, c, pm, g, cust, email, desc, meta, to)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tx.Status != StatusPending {
		t.Errorf("expected PENDING, got %s", tx.Status)
	}
	if tx.Version != 1 {
		t.Errorf("expected version 1, got %d", tx.Version)
	}
	if tx.ID == uuid.Nil {
		t.Error("expected generated ID")
	}
	if tx.EstimatedTimeoutSeconds != 30 {
		t.Errorf("expected timeout 30, got %d", tx.EstimatedTimeoutSeconds)
	}
}

func TestNew_Invalid(t *testing.T) {
	m, a, c, pm, g, cust, email, desc, meta, to := validNewArgs()
	cases := []struct {
		name string
		mut  func() (uuid.UUID, int64, string, PaymentMethod, string, uuid.UUID, string, string, map[string]any, int)
	}{
		{"zero amount", func() (uuid.UUID, int64, string, PaymentMethod, string, uuid.UUID, string, string, map[string]any, int) {
			return m, 0, c, pm, g, cust, email, desc, meta, to
		}},
		{"negative amount", func() (uuid.UUID, int64, string, PaymentMethod, string, uuid.UUID, string, string, map[string]any, int) {
			return m, -5, c, pm, g, cust, email, desc, meta, to
		}},
		{"lowercase currency", func() (uuid.UUID, int64, string, PaymentMethod, string, uuid.UUID, string, string, map[string]any, int) {
			return m, a, "inr", pm, g, cust, email, desc, meta, to
		}},
		{"bad currency length", func() (uuid.UUID, int64, string, PaymentMethod, string, uuid.UUID, string, string, map[string]any, int) {
			return m, a, "RUPEE", pm, g, cust, email, desc, meta, to
		}},
		{"invalid method", func() (uuid.UUID, int64, string, PaymentMethod, string, uuid.UUID, string, string, map[string]any, int) {
			return m, a, c, PaymentMethod("crypto"), g, cust, email, desc, meta, to
		}},
		{"nil merchant", func() (uuid.UUID, int64, string, PaymentMethod, string, uuid.UUID, string, string, map[string]any, int) {
			return uuid.Nil, a, c, pm, g, cust, email, desc, meta, to
		}},
		{"empty gateway", func() (uuid.UUID, int64, string, PaymentMethod, string, uuid.UUID, string, string, map[string]any, int) {
			return m, a, c, pm, "", cust, email, desc, meta, to
		}},
		{"zero timeout", func() (uuid.UUID, int64, string, PaymentMethod, string, uuid.UUID, string, string, map[string]any, int) {
			return m, a, c, pm, g, cust, email, desc, meta, 0
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.mut()); err == nil {
				t.Errorf("expected error for %s", tc.name)
			}
		})
	}
}

func TestStatus_IsTerminal(t *testing.T) {
	terminal := map[Status]bool{
		StatusSucceeded: true, StatusCancelled: true, StatusRefunded: true, StatusRefundFailed: true,
		StatusPending: false, StatusProcessing: false, StatusFailed: false,
	}
	for s, want := range terminal {
		if s.IsTerminal() != want {
			t.Errorf("%s.IsTerminal() = %v, want %v", s, s.IsTerminal(), want)
		}
	}
}

func TestIsLeaseExpired(t *testing.T) {
	past := time.Now().UTC().Add(-time.Hour)
	future := time.Now().UTC().Add(time.Hour)
	short := time.Second

	t.Run("expired", func(t *testing.T) {
		tx := &Transaction{Status: StatusProcessing, ProcessingStartedAt: &past, ProcessingTimeout: &short}
		if !tx.IsLeaseExpired() {
			t.Error("expected lease expired")
		}
	})
	t.Run("not expired", func(t *testing.T) {
		tx := &Transaction{Status: StatusProcessing, ProcessingStartedAt: &future, ProcessingTimeout: &short}
		if tx.IsLeaseExpired() {
			t.Error("expected lease not expired")
		}
	})
	t.Run("not processing", func(t *testing.T) {
		tx := &Transaction{Status: StatusPending, ProcessingStartedAt: &past, ProcessingTimeout: &short}
		if tx.IsLeaseExpired() {
			t.Error("non-PROCESSING transaction never has expired lease")
		}
	})
	t.Run("nil fields", func(t *testing.T) {
		tx := &Transaction{Status: StatusProcessing}
		if tx.IsLeaseExpired() {
			t.Error("nil lease fields => not expired")
		}
	})
}

func TestHasGatewayDiscrepancy(t *testing.T) {
	if (&Transaction{AttemptedGateway: "a", ActualGateway: ""}).HasGatewayDiscrepancy() {
		t.Error("empty actual gateway => no discrepancy")
	}
	if (&Transaction{AttemptedGateway: "a", ActualGateway: "a"}).HasGatewayDiscrepancy() {
		t.Error("same gateway => no discrepancy")
	}
	if !(&Transaction{AttemptedGateway: "a", ActualGateway: "b"}).HasGatewayDiscrepancy() {
		t.Error("different gateway => discrepancy")
	}
}

func TestSetCancelIntent(t *testing.T) {
	tx := &Transaction{}
	tx.SetCancelIntent(ActorMerchant, CancelViaAPI)
	if !tx.CancelIntent {
		t.Error("cancel intent not set")
	}
	if tx.CancelRequestedBy != ActorMerchant || tx.CancelRequestedVia != CancelViaAPI {
		t.Error("cancel actor/via not recorded")
	}
	if tx.CancelRequestedAt == nil {
		t.Error("cancel requested timestamp not set")
	}
}

func TestValidate(t *testing.T) {
	m, a, c, pm, g, cust, email, desc, meta, to := validNewArgs()
	tx, _ := New(m, a, c, pm, g, cust, email, desc, meta, to)
	if err := tx.Validate(); err != nil {
		t.Fatalf("valid transaction failed validation: %v", err)
	}

	bad := *tx
	bad.ID = uuid.Nil
	if err := bad.Validate(); err == nil {
		t.Error("expected validation error for nil ID")
	}
}
