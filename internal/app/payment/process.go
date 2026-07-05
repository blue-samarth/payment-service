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

type paymentTerminalPayload struct {
	TransactionID      string `json:"transaction_id"`
	Status             string `json:"status"`
	Gateway            string `json:"gateway"`
	GatewayReferenceID string `json:"gateway_reference_id"`
	Amount             int64  `json:"amount"`
}

type cachedResponse struct {
	Status             string `json:"status"`
	GatewayReferenceID string `json:"gateway_reference_id"`
}

type outcome struct {
	terminal      bool
	newStatus     transaction.Status
	failureReason *transaction.FailureReason
}

func (s *Service) ProcessPayment(ctx context.Context, transactionID uuid.UUID) (*transaction.Transaction, error) {
	txn, err := s.repo.GetByID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("payment: load transaction %s: %w", transactionID, err)
	}
	if txn.Status != transaction.StatusPending {
		return txn, nil
	}

	adapter, err := s.gateways.Get(txn.GatewayID)
	if err != nil {
		return nil, fmt.Errorf("payment: resolve adapter for %s: %w", txn.GatewayID, err)
	}

	ttlSec := txn.EstimatedTimeoutSeconds
	timeout := time.Duration(ttlSec) * time.Second

	var acquired bool
	err = s.tx.WithinTx(ctx, func(ctx context.Context) error {
		ok, _, err := s.lease.Acquire(ctx, txn.ID, txn.ID, ttlSec)
		if err != nil {
			return err
		}
		acquired = ok
		if !ok {
			return nil
		}

		if err := transaction.TransitionState(txn, transaction.StatusProcessing, transaction.ActorSystem); err != nil {
			return err
		}
		now := time.Now().UTC()
		txn.ProcessingStartedAt = &now
		txn.ProcessingTimeout = &timeout
		return s.repo.UpdateStatus(ctx, txn)
	})
	if err != nil {
		return nil, fmt.Errorf("payment: begin processing %s: %w", txn.ID, err)
	}
	if !acquired {
		return s.repo.GetByID(ctx, txn.ID)
	}

	resp, result := s.attemptGateways(ctx, adapter, txn)
	return s.finalize(ctx, txn, resp, result)
}

func (s *Service) RecoverExpiredLease(ctx context.Context, transactionID uuid.UUID) (*transaction.Transaction, error) {
	txn, err := s.repo.GetByID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("payment: load stuck transaction %s: %w", transactionID, err)
	}
	if txn.Status != transaction.StatusProcessing || !txn.IsLeaseExpired() {
		return txn, nil
	}

	gateway := txn.ActualGateway
	if gateway == "" {
		gateway = txn.GatewayID
	}
	adapter, err := s.gateways.Get(gateway)
	if err != nil {
		return nil, fmt.Errorf("payment: resolve adapter for %s: %w", gateway, err)
	}
	txn.GatewayID = gateway

	resp, gwErr := s.checkStatus(ctx, adapter, txn)
	result := resolveOutcome(resp, gwErr)
	s.recordBreaker(ctx, gateway, gwErr)

	if !result.terminal {
		s.log.Info(ports.LogEventTransactionLeaseExpired, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldGatewayID:     gateway,
			ports.FieldNewState:      string(txn.Status),
		})
		return txn, nil
	}

	s.log.Info(ports.LogEventTransactionLeaseExpired, map[string]any{
		ports.FieldTransactionID: txn.ID.String(),
		ports.FieldGatewayID:     gateway,
		ports.FieldNewState:      string(result.newStatus),
	})
	return s.finalize(ctx, txn, resp, result)
}

func (s *Service) checkStatus(ctx context.Context, adapter ports.GatewayAdapter, txn *transaction.Transaction) (*ports.GatewayPaymentResponse, *ports.GatewayError) {
	resp, err := adapter.CheckStatus(ctx, ports.GatewayStatusRequest{
		TransactionID:      txn.ID,
		GatewayReferenceID: txn.GatewayReferenceID,
		IdempotencyKey:     txn.GatewayIdempotencyKey,
	})
	if err != nil {
		var gwErr *ports.GatewayError
		if errors.As(err, &gwErr) {
			return nil, gwErr
		}
		return nil, &ports.GatewayError{
			Category:       ports.ErrorCategoryGatewayError,
			Code:           "status_check_failed",
			GatewayMessage: err.Error(),
			Underlying:     err,
		}
	}
	return resp, nil
}

func (s *Service) finalize(ctx context.Context, txn *transaction.Transaction, resp *ports.GatewayPaymentResponse, result outcome) (*transaction.Transaction, error) {
	if resp != nil {
		txn.GatewayReferenceID = resp.GatewayReferenceID
		if md := toMethodDetails(resp.MethodResponse); md != nil {
			txn.MethodDetails = md
		}
	}
	txn.ActualGateway = txn.GatewayID

	cached, err := json.Marshal(cachedResponse{
		Status:             string(result.newStatusOr(txn.Status)),
		GatewayReferenceID: txn.GatewayReferenceID,
	})
	if err != nil {
		return nil, fmt.Errorf("payment: marshal cached response: %w", err)
	}

	var event *ports.OutboxEvent
	if result.terminal {
		event, err = s.buildTerminalEvent(txn, result.newStatus)
		if err != nil {
			return nil, err
		}
	}

	err = s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if result.terminal {
			if err := transaction.TransitionState(txn, result.newStatus, transaction.ActorGateway); err != nil {
				return err
			}
			txn.FailureReason = result.failureReason
		}
		if err := s.repo.UpdateStatus(ctx, txn); err != nil {
			return err
		}
		if event != nil {
			if err := s.outbox.Write(ctx, *event); err != nil {
				return err
			}
		}
		return s.lease.WriteCachedResponse(ctx, txn.ID, cached)
	})
	if err != nil {
		s.log.Error(ports.LogEventGatewayResponse, map[string]any{
			ports.FieldErrorCode:     "payment_finalize_failed",
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldGatewayID:     txn.GatewayID,
		}, err)
		return nil, fmt.Errorf("payment: finalize %s: %w", txn.ID, err)
	}

	s.recordOutcome(txn, result)
	s.resolveCancelIfRequested(ctx, txn, result)
	return txn, nil
}

func (s *Service) resolveCancelIfRequested(ctx context.Context, txn *transaction.Transaction, result outcome) {
	if !result.terminal || result.newStatus != transaction.StatusSucceeded {
		return
	}
	if !txn.CancelIntent || s.cancelResolver == nil {
		return
	}
	if err := s.cancelResolver.ResolveCancelRefund(ctx, txn.ID, txn.Amount); err != nil {
		s.log.Error(ports.LogEventCancelResolution, map[string]any{
			ports.FieldErrorCode:     "cancel_resolution_failed",
			ports.FieldTraceID:       "",
			ports.FieldTransactionID: txn.ID.String(),
		}, err)
		return
	}
	s.log.Info(ports.LogEventCancelResolution, map[string]any{ports.FieldTransactionID: txn.ID.String()})
}

func (s *Service) attemptGateways(ctx context.Context, adapter ports.GatewayAdapter, txn *transaction.Transaction) (*ports.GatewayPaymentResponse, outcome) {
	maxAttempts := s.maxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	tried := make([]string, 0, maxAttempts)
	var resp *ports.GatewayPaymentResponse
	var result outcome

	for attempt := 1; ; attempt++ {
		var gwErr *ports.GatewayError
		resp, gwErr = s.callGateway(ctx, adapter, txn)
		result = resolveOutcome(resp, gwErr)
		s.recordBreaker(ctx, txn.GatewayID, gwErr)
		tried = append(tried, txn.GatewayID)

		if attempt >= maxAttempts || !shouldFallback(gwErr) {
			return resp, result
		}

		next, nextAdapter, ok := s.rerouteExcluding(ctx, txn, tried)
		if !ok {
			return resp, result
		}

		s.log.Info(ports.LogEventGatewayFallback, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldGatewayID:     txn.GatewayID,
			"fallback_gateway":       next,
			ports.FieldAttemptNumber: attempt + 1,
		})
		s.metrics.Increment(ports.MetricGatewayFallbackTriggered, map[string]string{
			"from_gateway": txn.GatewayID,
			"to_gateway":   next,
		})

		txn.GatewayID = next
		adapter = nextAdapter
	}
}

func shouldFallback(gwErr *ports.GatewayError) bool {
	if gwErr == nil {
		return false
	}
	return gwErr.Category == ports.ErrorCategorySoftDecline
}

func (s *Service) rerouteExcluding(ctx context.Context, txn *transaction.Transaction, exclude []string) (string, ports.GatewayAdapter, bool) {
	if s.router == nil {
		return "", nil, false
	}
	decision, err := s.router.Route(ctx, RouteInput{
		Amount:          txn.Amount,
		Currency:        txn.Currency,
		PaymentMethod:   txn.PaymentMethod,
		IsDomestic:      isDomesticCurrency(txn.Currency),
		ExcludeGateways: exclude,
	})
	if err != nil {
		var noCandidate routing.ErrNoCandidate
		if !errors.As(err, &noCandidate) {
			s.log.Warn(ports.LogEventGatewayFallback, map[string]any{
				ports.FieldTransactionID: txn.ID.String(),
				"error":                  err.Error(),
			})
		}
		return "", nil, false
	}
	adapter, err := s.gateways.Get(decision.SelectedGateway)
	if err != nil {
		s.log.Warn(ports.LogEventGatewayFallback, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldGatewayID:     decision.SelectedGateway,
			"error":                  err.Error(),
		})
		return "", nil, false
	}
	return decision.SelectedGateway, adapter, true
}

func isDomesticCurrency(currency string) bool { return currency == "INR" }

func (s *Service) recordBreaker(ctx context.Context, gatewayID string, gwErr *ports.GatewayError) {
	if s.breaker == nil {
		return
	}
	var err error
	if isGatewayHealthFailure(gwErr) {
		err = s.breaker.RecordFailure(ctx, gatewayID)
	} else {
		err = s.breaker.RecordSuccess(ctx, gatewayID)
	}
	if err != nil {
		s.log.Warn(ports.LogEventGatewayCircuitOpen, map[string]any{
			ports.FieldGatewayID: gatewayID,
			"error":              err.Error(),
		})
	}
}

func isGatewayHealthFailure(gwErr *ports.GatewayError) bool {
	if gwErr == nil {
		return false
	}
	switch gwErr.Category {
	case ports.ErrorCategoryNetworkTimeout, ports.ErrorCategoryGatewayError, ports.ErrorCategoryAmbiguous:
		return true
	default:
		return false
	}
}

func (s *Service) callGateway(ctx context.Context, adapter ports.GatewayAdapter, txn *transaction.Transaction) (*ports.GatewayPaymentResponse, *ports.GatewayError) {
	resp, err := adapter.InitiatePayment(ctx, ports.GatewayPaymentRequest{
		TransactionID:  txn.ID,
		MerchantID:     txn.MerchantID,
		Amount:         txn.Amount,
		Currency:       txn.Currency,
		PaymentMethod:  txn.PaymentMethod,
		IdempotencyKey: txn.GatewayIdempotencyKey,
		Metadata:       txn.Metadata,
		CustomerEmail:  txn.CustomerEmail,
		Description:    txn.Description,
		AttemptNumber:  1,
	})
	if err != nil {
		var gwErr *ports.GatewayError
		if errors.As(err, &gwErr) {
			return nil, gwErr
		}
		return nil, &ports.GatewayError{
			Category:       ports.ErrorCategoryGatewayError,
			Code:           "gateway_call_failed",
			GatewayMessage: err.Error(),
			Underlying:     err,
		}
	}
	return resp, nil
}

func resolveOutcome(resp *ports.GatewayPaymentResponse, gwErr *ports.GatewayError) outcome {
	if gwErr != nil {
		if gwErr.Category == ports.ErrorCategoryAmbiguous || gwErr.Category == ports.ErrorCategoryNetworkTimeout {
			return outcome{terminal: false}
		}
		return outcome{
			terminal:  true,
			newStatus: transaction.StatusFailed,
			failureReason: &transaction.FailureReason{
				Category:       string(gwErr.Category),
				Code:           gwErr.Code,
				GatewayCode:    gwErr.GatewayCode,
				GatewayMessage: gwErr.GatewayMessage,
				Source:         transaction.FailureReasonSourceGateway,
			},
		}
	}

	switch resp.Status {
	case ports.GatewayPaymentStatusSucceeded:
		return outcome{terminal: true, newStatus: transaction.StatusSucceeded}
	case ports.GatewayPaymentStatusFailed:
		return outcome{
			terminal:  true,
			newStatus: transaction.StatusFailed,
			failureReason: &transaction.FailureReason{
				Category:       "gateway_declined",
				Code:           resp.ErrorCode,
				GatewayCode:    resp.ErrorCode,
				GatewayMessage: resp.ErrorMessage,
				Source:         transaction.FailureReasonSourceGateway,
			},
		}
	default:
		return outcome{terminal: false}
	}
}

func (o outcome) newStatusOr(current transaction.Status) transaction.Status {
	if o.terminal {
		return o.newStatus
	}
	return current
}

func (s *Service) buildTerminalEvent(txn *transaction.Transaction, status transaction.Status) (*ports.OutboxEvent, error) {
	eventType := ports.EventTypePaymentFailed
	if status == transaction.StatusSucceeded {
		eventType = ports.EventTypePaymentSucceeded
	}

	payload, err := json.Marshal(paymentTerminalPayload{
		TransactionID:      txn.ID.String(),
		Status:             string(status),
		Gateway:            txn.ActualGateway,
		GatewayReferenceID: txn.GatewayReferenceID,
		Amount:             txn.Amount,
	})
	if err != nil {
		return nil, fmt.Errorf("payment: marshal terminal event: %w", err)
	}

	return &ports.OutboxEvent{
		AggregateID:   txn.ID,
		AggregateType: "transaction",
		EventType:     eventType,
		Payload:       payload,
		EventVersion:  1,
	}, nil
}

func (s *Service) recordOutcome(txn *transaction.Transaction, result outcome) {
	tags := map[string]string{
		"gateway_id":     txn.ActualGateway,
		"payment_method": string(txn.PaymentMethod),
	}
	switch {
	case !result.terminal:
		s.log.Info(ports.LogEventGatewayResponse, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldGatewayID:     txn.ActualGateway,
			ports.FieldNewState:      string(txn.Status),
		})
	case result.newStatus == transaction.StatusSucceeded:
		s.log.Info(ports.LogEventTransactionTransition, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldNewState:      string(transaction.StatusSucceeded),
		})
		s.metrics.Increment(ports.MetricTransactionSucceeded, tags)
	default:
		s.log.Info(ports.LogEventTransactionTransition, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldNewState:      string(transaction.StatusFailed),
		})
		s.metrics.Increment(ports.MetricTransactionFailed, tags)
	}
}

func toMethodDetails(mr ports.GatewayMethodResponse) *transaction.MethodDetails {
	switch v := mr.(type) {
	case *ports.GatewayCardResponse:
		return &transaction.MethodDetails{Card: &transaction.CardDetails{
			CardBrand: v.CardBrand,
			Last4:     v.Last4,
			Network:   v.Network,
			RiskScore: v.RiskScore,
			AuthCode:  v.AuthCode,
		}}
	case *ports.GatewayUPIResponse:
		return &transaction.MethodDetails{UPI: &transaction.UPIDetails{
			VPA:              v.VPA,
			UPITransactionID: v.UPITransactionID,
			PayerBank:        v.PayerBank,
		}}
	case *ports.GatewayNetbankingResponse:
		return &transaction.MethodDetails{Netbanking: &transaction.NetbankingDetails{
			BankCode:        v.BankCode,
			BankReferenceID: v.BankReferenceID,
		}}
	case *ports.GatewayWalletResponse:
		return &transaction.MethodDetails{Wallet: &transaction.WalletDetails{
			WalletProvider:      v.WalletProvider,
			WalletTransactionID: v.WalletTransactionID,
		}}
	default:
		return nil
	}
}
