package reconciliation

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type JobStatus string
type ResolutionStatus string
type TriggerSource string
type MismatchType string

type Job struct {
	ID            uuid.UUID
	GatewayID     string
	TransactionID *uuid.UUID
	PeriodStart   *time.Time
	PeriodEnd     *time.Time
	Status        JobStatus
	TriggeredBy   TriggerSource
	Actor         string
	MismatchCount int
	Error         string
	StartedAt     *time.Time
	CompletedAt   *time.Time
	CreatedAt     time.Time
}

type Entry struct {
	ID                   uuid.UUID
	JobID                uuid.UUID
	TransactionID        uuid.UUID
	InternalStatus       string
	GatewayStatus        string
	InternalAmount       int64
	GatewayAmount        int64
	InternalFees         *int64
	GatewayFees          *int64
	FXRateApplied        *float64
	FXRateAtSettlement   *float64
	FeeMismatchReason    string
	MismatchType         MismatchType
	ResolutionStatus     ResolutionStatus
	ResolvedBy           string
	ResolvedAt           *time.Time
	Notes                string
	AutoResolutionAction string
	AutoResolutionAt     *time.Time
	AutoResolutionBy     string
}

type AutoResolutionConfig struct {
	PaymentMethod    string
	Region           string
	ThresholdBPS     int
	AbsoluteCapPaise int64
	Action           string
}

type AutoResolutionLog struct {
	ID                     uuid.UUID
	SettlementID           uuid.UUID
	DiscrepancyAmountPaise int64
	ThresholdBPS           int
	AbsoluteCapPaise       int64
	QualifiedPercentage    bool
	QualifiedAbsolute      bool
	Action                 string
	ExecutedAt             time.Time
	ExecutedBy             string
}

const (
	JobStatusPending   JobStatus = "PENDING"
	JobStatusRunning   JobStatus = "RUNNING"
	JobStatusCompleted JobStatus = "COMPLETED"
	JobStatusFailed    JobStatus = "FAILED"
)

const (
	TriggerSystem TriggerSource = "system"
	TriggerOps    TriggerSource = "ops"
)

const (
	MismatchStatus          MismatchType = "STATUS_MISMATCH"
	MismatchAmount          MismatchType = "AMOUNT_MISMATCH"
	MismatchFee             MismatchType = "FEE_MISMATCH"
	MismatchMissingInternal MismatchType = "MISSING_INTERNAL"
	MismatchMissingGateway  MismatchType = "MISSING_GATEWAY"
)

const (
	ResolutionUnresolved   ResolutionStatus = "UNRESOLVED"
	ResolutionResolved     ResolutionStatus = "RESOLVED"
	ResolutionAutoRefund   ResolutionStatus = "AUTO_REFUND_ISSUED"
	ResolutionAutoInvoiced ResolutionStatus = "AUTO_INVOICED"
)

func (m MismatchType) IsCritical() bool {
	return m == MismatchStatus || m == MismatchMissingInternal
}

func (e *Entry) EligibleForAutoResolution(cfg AutoResolutionConfig) (bool, string) {
	if e.MismatchType != MismatchAmount {
		return false, fmt.Sprintf("mismatch type %s requires manual review", e.MismatchType)
	}

	discrepancy := e.GatewayAmount - e.InternalAmount
	if discrepancy < 0 {
		discrepancy = -discrepancy
	}

	settledAmount := e.GatewayAmount
	if settledAmount == 0 {
		return false, "gateway amount is zero, cannot compute percentage"
	}

	pctCheck := discrepancy*10000/settledAmount <= int64(cfg.ThresholdBPS)
	absCheck := discrepancy <= cfg.AbsoluteCapPaise

	if !pctCheck {
		return false, fmt.Sprintf(
			"percentage check failed: %.4f%% > %.4f%%",
			float64(discrepancy*10000/settledAmount)/100,
			float64(cfg.ThresholdBPS)/100,
		)
	}
	if !absCheck {
		return false, fmt.Sprintf(
			"absolute cap check failed: %d paise > %d paise",
			discrepancy, cfg.AbsoluteCapPaise,
		)
	}

	return true, ""
}
