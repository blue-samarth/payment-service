package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/domain/routing"
	"samarth/payment-service/internal/domain/transaction"
	"samarth/payment-service/internal/ports"
)

var ErrNoGateway = errors.New("payment: no eligible gateway for transaction")

type TransactionRepo interface {
	Insert(ctx context.Context, t *transaction.Transaction) error
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error)
	UpdateStatus(ctx context.Context, t *transaction.Transaction) error
}
type EventWriter interface {
	Write(ctx context.Context, event ports.OutboxEvent) error
}
type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}
type ConfigReader interface {
	GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error)
}
type Router interface {
	Route(ctx context.Context, in RouteInput) (*routing.Decision, error)
}
type LeaseStore interface {
	Acquire(ctx context.Context, leaseKey, paymentIntentID uuid.UUID, ttlSec int) (bool, []byte, error)
	WriteCachedResponse(ctx context.Context, leaseKey uuid.UUID, response []byte) error
}
type GatewayRegistry interface {
	Get(gatewayID string) (ports.GatewayAdapter, error)
}
type CancelResolver interface {
	ResolveCancelRefund(ctx context.Context, transactionID uuid.UUID, amount int64) error
}
type CircuitBreaker interface {
	RecordFailure(ctx context.Context, gatewayID string) error
	RecordSuccess(ctx context.Context, gatewayID string) error
}

type RouteInput struct {
	Amount          int64
	Currency        string
	PaymentMethod   transaction.PaymentMethod
	MerchantTier    string
	IsDomestic      bool
	ExcludeGateways []string
}
type CreatePaymentInput struct {
	MerchantID    uuid.UUID
	Amount        int64
	Currency      string
	PaymentMethod transaction.PaymentMethod
	CustomerID    uuid.UUID
	CustomerEmail string
	Description   string
	Metadata      map[string]any
	MerchantTier  string
	IsDomestic    bool
}

type Service struct {
	repo           TransactionRepo
	outbox         EventWriter
	router         Router
	config         ConfigReader
	tx             Transactor
	lease          LeaseStore
	gateways       GatewayRegistry
	cancelResolver CancelResolver
	breaker        CircuitBreaker
	maxAttempts    int
	log            ports.Logger
	metrics        ports.MetricRecorder
}

func (s *Service) SetCancelResolver(r CancelResolver) { s.cancelResolver = r }
func (s *Service) SetCircuitBreaker(b CircuitBreaker) { s.breaker = b }
func (s *Service) SetMaxGatewayAttempts(n int)        { s.maxAttempts = n }

func NewService(
	repo TransactionRepo,
	outbox EventWriter,
	router Router,
	config ConfigReader,
	tx Transactor,
	lease LeaseStore,
	gateways GatewayRegistry,
	log ports.Logger,
	metrics ports.MetricRecorder,
) *Service {
	return &Service{
		repo:     repo,
		outbox:   outbox,
		router:   router,
		config:   config,
		tx:       tx,
		lease:    lease,
		gateways: gateways,
		log:      log,
		metrics:  metrics,
	}
}

type paymentCreatedPayload struct {
	TransactionID string `json:"transaction_id"`
	MerchantID    string `json:"merchant_id"`
	Amount        int64  `json:"amount"`
	Currency      string `json:"currency"`
	PaymentMethod string `json:"payment_method"`
	Gateway       string `json:"gateway"`
	Status        string `json:"status"`
	CreatedAt     string `json:"created_at"`
}

func (s *Service) GetPayment(ctx context.Context, id uuid.UUID) (*transaction.Transaction, error) {
	return s.repo.GetByID(ctx, id)
}

func (s *Service) CreatePayment(ctx context.Context, in CreatePaymentInput) (*transaction.Transaction, error) {
	decision, err := s.router.Route(ctx, RouteInput{
		Amount:        in.Amount,
		Currency:      in.Currency,
		PaymentMethod: in.PaymentMethod,
		MerchantTier:  in.MerchantTier,
		IsDomestic:    in.IsDomestic,
	})
	if err != nil {
		var noCandidate routing.ErrNoCandidate
		if errors.As(err, &noCandidate) {
			s.log.Warn(ports.LogEventRoutingNoCandidate, map[string]any{
				ports.FieldMerchantID:    in.MerchantID.String(),
				ports.FieldPaymentMethod: string(in.PaymentMethod),
			})
			return nil, ErrNoGateway
		}
		return nil, fmt.Errorf("payment: routing: %w", err)
	}

	gateway := decision.SelectedGateway

	timeout, err := s.config.GetProcessingTimeout(ctx, gateway, string(in.PaymentMethod))
	if err != nil {
		return nil, fmt.Errorf("payment: processing timeout for %s/%s: %w", gateway, in.PaymentMethod, err)
	}

	timeoutSec := int(timeout.Seconds())
	if timeoutSec <= 0 {
		return nil, fmt.Errorf("payment: invalid processing timeout %s for %s/%s", timeout, gateway, in.PaymentMethod)
	}

	txn, err := transaction.New(
		in.MerchantID,
		in.Amount,
		in.Currency,
		in.PaymentMethod,
		gateway,
		in.CustomerID,
		in.CustomerEmail,
		in.Description,
		in.Metadata,
		timeoutSec,
	)
	if err != nil {
		return nil, fmt.Errorf("payment: build transaction: %w", err)
	}
	txn.AttemptedGateway = gateway

	payload, err := json.Marshal(paymentCreatedPayload{
		TransactionID: txn.ID.String(),
		MerchantID:    txn.MerchantID.String(),
		Amount:        txn.Amount,
		Currency:      txn.Currency,
		PaymentMethod: string(txn.PaymentMethod),
		Gateway:       gateway,
		Status:        string(txn.Status),
		CreatedAt:     txn.CreatedAt.Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, fmt.Errorf("payment: marshal created event: %w", err)
	}

	event := ports.OutboxEvent{
		AggregateID:   txn.ID,
		AggregateType: "transaction",
		EventType:     ports.EventTypePaymentCreated,
		Payload:       payload,
		EventVersion:  1,
	}

	err = s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := s.repo.Insert(ctx, txn); err != nil {
			return fmt.Errorf("insert transaction: %w", err)
		}
		if err := s.outbox.Write(ctx, event); err != nil {
			return fmt.Errorf("write outbox event: %w", err)
		}
		return nil
	})
	if err != nil {
		s.log.Error(ports.LogEventTransactionCreated, map[string]any{
			ports.FieldErrorCode:     "payment_persist_failed",
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldMerchantID:    txn.MerchantID.String(),
			ports.FieldGatewayID:     gateway,
		}, err)
		return nil, fmt.Errorf("payment: persist transaction %s: %w", txn.ID, err)
	}

	s.log.Info(ports.LogEventTransactionCreated, map[string]any{
		ports.FieldTransactionID: txn.ID.String(),
		ports.FieldMerchantID:    txn.MerchantID.String(),
		ports.FieldGatewayID:     gateway,
		ports.FieldPaymentMethod: string(txn.PaymentMethod),
	})
	s.metrics.Increment(ports.MetricTransactionCreated, map[string]string{
		"gateway_id":     gateway,
		"payment_method": string(txn.PaymentMethod),
	})

	return txn, nil
}
