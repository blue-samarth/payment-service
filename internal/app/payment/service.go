package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"samarth/payment-service/internal/app/idempotency"
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
	Metadata       map[string]any
	MerchantTier   string
	IsDomestic     bool
	IdempotencyKey string
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
	idem           *idempotency.Guard
	maxAttempts    int
	log            ports.Logger
	metrics        ports.MetricRecorder
}

func (s *Service) SetCancelResolver(r CancelResolver)   { s.cancelResolver = r }
func (s *Service) SetCircuitBreaker(b CircuitBreaker)   { s.breaker = b }
func (s *Service) SetMaxGatewayAttempts(n int)          { s.maxAttempts = n }
func (s *Service) SetIdempotency(g *idempotency.Guard)  { s.idem = g }

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

type CreateResult struct {
	Verdict     idempotency.Verdict
	Transaction *transaction.Transaction
}

type idempotencyPaymentResponse struct {
	TransactionID string `json:"transaction_id"`
}

func (s *Service) CreatePayment(ctx context.Context, in CreatePaymentInput) (CreateResult, error) {
	// Routing and config reads acquire their own pool connections, so they must
	// run BEFORE (never nested inside) the reservation transaction — otherwise a
	// burst of concurrent creates each hold a tx connection while waiting for a
	// second one to route, and the pool deadlocks.
	txn, event, err := s.buildTransaction(ctx, in)
	if err != nil {
		return CreateResult{}, err
	}

	if s.idem == nil {
		if err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
			return s.insertTransaction(ctx, txn, event)
		}); err != nil {
			return CreateResult{}, err
		}
		s.logCreated(txn)
		return CreateResult{Verdict: idempotency.Created, Transaction: txn}, nil
	}

	if in.IdempotencyKey == "" {
		return CreateResult{}, idempotency.ErrKeyRequired
	}
	composite := idempotency.Composite(in.MerchantID.String(), "create_payment", in.IdempotencyKey)
	requestHash := createRequestHash(in)

	res, err := s.idem.Execute(ctx, composite, requestHash, func(ctx context.Context) ([]byte, error) {
		if err := s.insertTransaction(ctx, txn, event); err != nil {
			return nil, err
		}
		return json.Marshal(idempotencyPaymentResponse{TransactionID: txn.ID.String()})
	})
	if err != nil {
		return CreateResult{}, err
	}

	switch res.Verdict {
	case idempotency.Created:
		s.logCreated(txn)
		return CreateResult{Verdict: res.Verdict, Transaction: txn}, nil
	case idempotency.Replayed:
		var stored idempotencyPaymentResponse
		if err := json.Unmarshal(res.Response, &stored); err != nil {
			return CreateResult{}, fmt.Errorf("payment: decode idempotent response: %w", err)
		}
		id, err := uuid.Parse(stored.TransactionID)
		if err != nil {
			return CreateResult{}, fmt.Errorf("payment: bad stored transaction id: %w", err)
		}
		existing, err := s.repo.GetByID(ctx, id)
		if err != nil {
			return CreateResult{}, fmt.Errorf("payment: reload idempotent transaction %s: %w", id, err)
		}
		return CreateResult{Verdict: res.Verdict, Transaction: existing}, nil
	default:
		return CreateResult{Verdict: res.Verdict}, nil
	}
}

func (s *Service) buildTransaction(ctx context.Context, in CreatePaymentInput) (*transaction.Transaction, *ports.OutboxEvent, error) {
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
			return nil, nil, ErrNoGateway
		}
		return nil, nil, fmt.Errorf("payment: routing: %w", err)
	}

	gateway := decision.SelectedGateway

	timeout, err := s.config.GetProcessingTimeout(ctx, gateway, string(in.PaymentMethod))
	if err != nil {
		return nil, nil, fmt.Errorf("payment: processing timeout for %s/%s: %w", gateway, in.PaymentMethod, err)
	}

	timeoutSec := int(timeout.Seconds())
	if timeoutSec <= 0 {
		return nil, nil, fmt.Errorf("payment: invalid processing timeout %s for %s/%s", timeout, gateway, in.PaymentMethod)
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
		return nil, nil, fmt.Errorf("payment: build transaction: %w", err)
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
		return nil, nil, fmt.Errorf("payment: marshal created event: %w", err)
	}

	return txn, &ports.OutboxEvent{
		AggregateID:   txn.ID,
		AggregateType: "transaction",
		EventType:     ports.EventTypePaymentCreated,
		Payload:       payload,
		EventVersion:  1,
	}, nil
}

func (s *Service) insertTransaction(ctx context.Context, txn *transaction.Transaction, event *ports.OutboxEvent) error {
	if err := s.repo.Insert(ctx, txn); err != nil {
		return fmt.Errorf("insert transaction: %w", err)
	}
	if err := s.outbox.Write(ctx, *event); err != nil {
		return fmt.Errorf("write outbox event: %w", err)
	}
	return nil
}

func (s *Service) logCreated(txn *transaction.Transaction) {
	s.log.Info(ports.LogEventTransactionCreated, map[string]any{
		ports.FieldTransactionID: txn.ID.String(),
		ports.FieldMerchantID:    txn.MerchantID.String(),
		ports.FieldGatewayID:     txn.GatewayID,
		ports.FieldPaymentMethod: string(txn.PaymentMethod),
	})
	s.metrics.Increment(ports.MetricTransactionCreated, map[string]string{
		"gateway_id":     txn.GatewayID,
		"payment_method": string(txn.PaymentMethod),
	})
}

func createRequestHash(in CreatePaymentInput) string {
	return idempotency.RequestHash(
		in.MerchantID.String(),
		strconv.FormatInt(in.Amount, 10),
		in.Currency,
		string(in.PaymentMethod),
		in.CustomerID.String(),
		in.CustomerEmail,
		in.Description,
	)
}
