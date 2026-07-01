package reconciliation

import "testing"

func TestMismatchType_IsCritical(t *testing.T) {
	want := map[MismatchType]bool{
		MismatchStatus:          true,
		MismatchMissingInternal: true,
		MismatchAmount:          false,
		MismatchFee:             false,
		MismatchMissingGateway:  false,
	}
	for m, w := range want {
		if m.IsCritical() != w {
			t.Errorf("%s.IsCritical() = %v, want %v", m, m.IsCritical(), w)
		}
	}
}

func TestEligibleForAutoResolution_OnlyAmountMismatch(t *testing.T) {
	cfg := AutoResolutionConfig{ThresholdBPS: 100, AbsoluteCapPaise: 100000}
	for _, mt := range []MismatchType{MismatchStatus, MismatchFee, MismatchMissingInternal, MismatchMissingGateway} {
		e := &Entry{MismatchType: mt, GatewayAmount: 100000, InternalAmount: 100000}
		if ok, _ := e.EligibleForAutoResolution(cfg); ok {
			t.Errorf("%s should never be auto-resolvable", mt)
		}
	}
}

func TestEligibleForAutoResolution_WithinThresholds(t *testing.T) {
	cfg := AutoResolutionConfig{ThresholdBPS: 10, AbsoluteCapPaise: 100}
	e := &Entry{MismatchType: MismatchAmount, GatewayAmount: 100000, InternalAmount: 99950}
	ok, reason := e.EligibleForAutoResolution(cfg)
	if !ok {
		t.Errorf("expected eligible, got reason: %s", reason)
	}
}

func TestEligibleForAutoResolution_FailsPercentageCheck(t *testing.T) {
	cfg := AutoResolutionConfig{ThresholdBPS: 3, AbsoluteCapPaise: 100000}
	e := &Entry{MismatchType: MismatchAmount, GatewayAmount: 100000, InternalAmount: 99950}
	if ok, _ := e.EligibleForAutoResolution(cfg); ok {
		t.Error("expected percentage check to fail (5 BPS > 3 BPS)")
	}
}

func TestEligibleForAutoResolution_FailsAbsoluteCap(t *testing.T) {
	cfg := AutoResolutionConfig{ThresholdBPS: 100, AbsoluteCapPaise: 40}
	e := &Entry{MismatchType: MismatchAmount, GatewayAmount: 100000, InternalAmount: 99950}
	if ok, _ := e.EligibleForAutoResolution(cfg); ok {
		t.Error("expected absolute cap check to fail (50 paise > 40 paise)")
	}
}

func TestEligibleForAutoResolution_ZeroGatewayAmount(t *testing.T) {
	cfg := AutoResolutionConfig{ThresholdBPS: 100, AbsoluteCapPaise: 100000}
	e := &Entry{MismatchType: MismatchAmount, GatewayAmount: 0, InternalAmount: 100}
	if ok, reason := e.EligibleForAutoResolution(cfg); ok || reason == "" {
		t.Error("zero gateway amount should be ineligible with a reason")
	}
}

func TestEligibleForAutoResolution_AbsoluteDiscrepancy(t *testing.T) {
	cfg := AutoResolutionConfig{ThresholdBPS: 100, AbsoluteCapPaise: 100}
	e := &Entry{MismatchType: MismatchAmount, GatewayAmount: 99950, InternalAmount: 100000}
	if ok, reason := e.EligibleForAutoResolution(cfg); !ok {
		t.Errorf("negative discrepancy should use absolute value and be eligible, got: %s", reason)
	}
}
