package refundrecovery

import (
	"context"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/refund"
	"samarth/payment-service/internal/ports"
)

type RecoverableLister interface {
	ListRecoverableRefundIDs(ctx context.Context, olderThan time.Duration, limit int) ([]uuid.UUID, error)
	CountPendingReview(ctx context.Context) (int64, error)
}

type Recoverer interface {
	RecoverRefund(ctx context.Context, id uuid.UUID) (*refund.Refund, error)
}

type Config struct {
	MinAge    time.Duration
	BatchSize int
}

type Job struct {
	refunds RecoverableLister
	recover Recoverer
	log     ports.Logger
	metrics ports.MetricRecorder
	cfg     Config
}

func New(refunds RecoverableLister, recover Recoverer, log ports.Logger, metrics ports.MetricRecorder, cfg Config) *Job {
	if cfg.MinAge <= 0 {
		cfg.MinAge = 5 * time.Minute
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	return &Job{refunds: refunds, recover: recover, log: log, metrics: metrics, cfg: cfg}
}

func (j *Job) RunOnce(ctx context.Context) error {
	ids, err := j.refunds.ListRecoverableRefundIDs(ctx, j.cfg.MinAge, j.cfg.BatchSize)
	if err != nil {
		j.log.Error(ports.LogEventRefundRecovery, map[string]any{
			ports.FieldErrorCode:     "list_recoverable_refunds_failed",
			ports.FieldTraceID:       "",
			ports.FieldTransactionID: "",
		}, err)
		return err
	}

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		rf, err := j.recover.RecoverRefund(ctx, id)
		if err != nil {
			j.metrics.Increment(ports.MetricRefundRecoveryFailed, nil)
			j.log.Error(ports.LogEventRefundRecovery, map[string]any{
				ports.FieldErrorCode:     "refund_recovery_failed",
				ports.FieldTraceID:       "",
				ports.FieldTransactionID: "",
				ports.FieldRefundID:      id.String(),
			}, err)
			continue
		}
		j.log.Info(ports.LogEventRefundRecovery, map[string]any{
			ports.FieldRefundID: id.String(),
			ports.FieldNewState: string(rf.Status),
		})
	}

	if pending, err := j.refunds.CountPendingReview(ctx); err == nil {
		j.metrics.Gauge(ports.MetricRefundReviewPending, float64(pending), nil)
	}
	return nil
}
