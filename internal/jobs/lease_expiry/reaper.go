package leaseexpiry

import (
	"context"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

type ExpiredLeaseLister interface {
	ListExpiredLeaseIDs(ctx context.Context) ([]uuid.UUID, error)
}

type LeaseRecoverer interface {
	RecoverExpiredLease(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error)
}

type IdempotencySweeper interface {
	SweepStaleProcessing(ctx context.Context, olderThan time.Duration) (int64, error)
	DeleteExpired(ctx context.Context) (int64, error)
}

type Config struct {
	IdempotencyProcessingTimeout time.Duration
}

type Reaper struct {
	txns    ExpiredLeaseLister
	recover LeaseRecoverer
	idem    IdempotencySweeper
	log     ports.Logger
	cfg     Config
}

func New(txns ExpiredLeaseLister, recover LeaseRecoverer, idem IdempotencySweeper, log ports.Logger, cfg Config) *Reaper {
	if cfg.IdempotencyProcessingTimeout <= 0 {
		cfg.IdempotencyProcessingTimeout = 5 * time.Minute
	}
	return &Reaper{txns: txns, recover: recover, idem: idem, log: log, cfg: cfg}
}

func (r *Reaper) RunOnce(ctx context.Context) error {
	r.reapLeases(ctx)
	r.sweepIdempotency(ctx)
	return nil
}

func (r *Reaper) reapLeases(ctx context.Context) {
	ids, err := r.txns.ListExpiredLeaseIDs(ctx)
	if err != nil {
		r.log.Error(ports.LogEventTransactionLeaseExpired, map[string]any{
			ports.FieldErrorCode:     "list_expired_leases_failed",
			ports.FieldTraceID:       "",
			ports.FieldTransactionID: "",
		}, err)
		return
	}

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return
		}
		txn, err := r.recover.RecoverExpiredLease(ctx, id)
		if err != nil {
			r.log.Error(ports.LogEventTransactionLeaseExpired, map[string]any{
				ports.FieldErrorCode:     "lease_recovery_failed",
				ports.FieldTraceID:       "",
				ports.FieldTransactionID: id.String(),
			}, err)
			continue
		}
		r.log.Info(ports.LogEventTransactionLeaseExpired, map[string]any{
			ports.FieldTransactionID: id.String(),
			ports.FieldNewState:      string(txn.Status),
		})
	}
}

func (r *Reaper) sweepIdempotency(ctx context.Context) {
	stale, err := r.idem.SweepStaleProcessing(ctx, r.cfg.IdempotencyProcessingTimeout)
	if err != nil {
		r.log.Error(ports.LogEventIdempotencySweep, map[string]any{
			ports.FieldErrorCode:     "idempotency_sweep_failed",
			ports.FieldTraceID:       "",
			ports.FieldTransactionID: "",
		}, err)
	} else if stale > 0 {
		r.log.Warn(ports.LogEventIdempotencySweep, map[string]any{"swept": stale})
	}

	expired, err := r.idem.DeleteExpired(ctx)
	if err != nil {
		r.log.Error(ports.LogEventIdempotencySweep, map[string]any{
			ports.FieldErrorCode:     "idempotency_expiry_failed",
			ports.FieldTraceID:       "",
			ports.FieldTransactionID: "",
		}, err)
	} else if expired > 0 {
		r.log.Info(ports.LogEventIdempotencySweep, map[string]any{"purged": expired})
	}
}
