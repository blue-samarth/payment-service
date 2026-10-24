package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	domaingateway "samarth/payment-service/internal/domain/gateway"
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
	AggregateVersion   int    `json:"aggregate_version"`
}

type cachedResponse struct {
	Status             string `json:"status"`
	GatewayReferenceID string `json:"gateway_reference_id"`
}

const (
	reinitiateWindow = 23 * time.Hour
	workMargin       = 10 * time.Second
	gateTimeout      = 500 * time.Millisecond
	leaseMargin      = 30 * time.Second

	reviewReasonNotIdempotent      = "gateway_not_idempotent"
	reviewReasonReplayWindowPassed = "replay_window_passed"
)

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

	timeout := time.Duration(txn.EstimatedTimeoutSeconds) * time.Second
	if floor := s.gatewayBudget() + leaseMargin; timeout < floor {
		timeout = floor
	}
	ttlSec := int(timeout / time.Second)

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
		txn.ActualGateway = txn.GatewayID
		return s.repo.UpdateStatus(ctx, txn)
	})
	if err != nil {
		return nil, fmt.Errorf("payment: begin processing %s: %w", txn.ID, err)
	}
	if !acquired {
		return s.repo.GetByID(ctx, txn.ID)
	}

	work := context.WithoutCancel(ctx)
	if budget := s.gatewayBudget(); budget > 0 {
		var cancel context.CancelFunc
		work, cancel = context.WithTimeout(work, budget+workMargin)
		defer cancel()
	}
	s.enterIntent(work, txn.GatewayID, txn.ID, timeout)

	resp, result := s.attemptGateways(work, adapter, txn)
	s.awaitCustomer(txn, resp, result)
	final, err := s.finalize(work, txn, resp, result)
	if err == nil {
		return final, nil
	}
	current, getErr := s.repo.GetByID(context.WithoutCancel(ctx), txn.ID)
	if getErr == nil && current.Status != transaction.StatusProcessing {
		return current, nil
	}
	return nil, err
}

func (s *Service) RecoverExpiredLease(ctx context.Context, transactionID uuid.UUID) (*transaction.Transaction, error) {
	txn, err := s.repo.GetByID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("payment: load stuck transaction %s: %w", transactionID, err)
	}
	if txn.Status != transaction.StatusProcessing || !txn.IsLeaseExpired() {
		return txn, nil
	}

	gateway := txn.ProcessingGateway()
	adapter, err := s.gateways.Get(gateway)
	if err != nil {
		return nil, fmt.Errorf("payment: resolve adapter for %s: %w", gateway, err)
	}
	txn.GatewayID = gateway

	var resp *ports.GatewayPaymentResponse
	var gwErr *ports.GatewayError
	if txn.GatewayReferenceID == "" {
		switch {
		case !adapter.Capabilities().IdempotencyCapable:
			return s.requireReview(ctx, txn, gateway, reviewReasonNotIdempotent)
		case txn.ProcessingStartedAt == nil || time.Since(*txn.ProcessingStartedAt) > reinitiateWindow:
			return s.requireReview(ctx, txn, gateway, reviewReasonReplayWindowPassed)
		}
		resp, gwErr = s.callGateway(ctx, adapter, txn)
	} else {
		resp, gwErr = s.checkStatus(ctx, adapter, txn)
	}
	result := resolveOutcome(resp, gwErr)
	if txn.GatewayReferenceID == "" && gwErr != nil && !isDecline(gwErr) {
		result = outcome{terminal: false}
	}
	if !result.terminal && txn.NextAction != nil && gwErr == nil && resp != nil && resp.Status == ports.GatewayPaymentStatusPending {
		result = outcome{
			terminal:  true,
			newStatus: transaction.StatusFailed,
			failureReason: &transaction.FailureReason{
				Category: "customer_action_expired",
				Code:     "abandoned",
				Source:   transaction.FailureReasonSourceTimeout,
			},
		}
	}
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

func (s *Service) requireReview(ctx context.Context, txn *transaction.Transaction, gateway, reason string) (*transaction.Transaction, error) {
	marked, err := s.repo.MarkReviewRequired(ctx, txn.ID, reason)
	if err != nil {
		return nil, fmt.Errorf("payment: mark %s for review: %w", txn.ID, err)
	}
	if !marked {
		return txn, nil
	}
	s.metrics.Increment(ports.MetricTransactionReviewRequired, map[string]string{
		"gateway_id": gateway,
		"reason":     reason,
	})
	s.log.Error(ports.LogEventTransactionLeaseExpired, map[string]any{
		ports.FieldErrorCode:     "recovery_requires_manual_review",
		ports.FieldTraceID:       "",
		ports.FieldTransactionID: txn.ID.String(),
		ports.FieldGatewayID:     gateway,
		"reason":                 reason,
	}, errors.New("no gateway reference and no safe way to replay the request"))
	return txn, nil
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
		if result.terminal {
			event, err := s.buildTerminalEvent(txn, result.newStatus)
			if err != nil {
				return err
			}
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
	if result.terminal {
		s.exitIntent(ctx, txn.GatewayID, txn.ID)
	}
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

	if adm := s.admitCall(ctx, txn.GatewayID); !adm.Admitted {
		tried = append(tried, txn.GatewayID)
		refusal := adm.Refusal
		nextAdapter, ok := s.moveToNextAdmitted(ctx, txn, &tried, &refusal, "breaker_refused", 1)
		if !ok {
			return nil, outcome{
				terminal:  true,
				newStatus: transaction.StatusFailed,
				failureReason: &transaction.FailureReason{
					Category: "gateway_unavailable",
					Code:     refusal,
					Source:   transaction.FailureReasonSourceInternal,
				},
			}
		}
		adapter = nextAdapter
	}

	for calls := 1; ; calls++ {
		resp, gwErr := s.callGateway(ctx, adapter, txn)
		s.saveReference(ctx, txn, resp)
		result := resolveOutcome(resp, gwErr)
		s.recordBreaker(ctx, txn.GatewayID, gwErr)
		tried = append(tried, txn.GatewayID)

		if calls >= maxAttempts || !shouldFallback(gwErr) {
			return resp, result
		}

		var refusal string
		nextAdapter, ok := s.moveToNextAdmitted(ctx, txn, &tried, &refusal, "soft_decline", calls+1)
		if !ok {
			return resp, result
		}
		adapter = nextAdapter
	}
}

func (s *Service) moveToNextAdmitted(ctx context.Context, txn *transaction.Transaction, tried *[]string, refusal *string, reason string, attempt int) (ports.GatewayAdapter, bool) {
	for {
		next, adapter, ok := s.rerouteExcluding(ctx, txn, *tried)
		if !ok || slices.Contains(*tried, next) {
			return nil, false
		}
		adm := s.admitCall(ctx, next)
		if !adm.Admitted {
			*tried = append(*tried, next)
			*refusal = adm.Refusal
			continue
		}
		if !s.switchGateway(ctx, txn, next, reason, attempt) {
			if adm.HoldsProbe {
				s.releaseProbe(ctx, next)
			}
			return nil, false
		}
		return adapter, true
	}
}

func (s *Service) awaitCustomer(txn *transaction.Transaction, resp *ports.GatewayPaymentResponse, result outcome) {
	if result.terminal || resp == nil || resp.NextAction == nil {
		return
	}
	txn.NextAction = resp.NextAction
	if s.customerActionWindow > 0 && (txn.ProcessingTimeout == nil || s.customerActionWindow > *txn.ProcessingTimeout) {
		window := s.customerActionWindow
		txn.ProcessingTimeout = &window
	}
}

func (s *Service) admitCall(ctx context.Context, gatewayID string) domaingateway.Admission {
	if s.gate == nil {
		return domaingateway.Admission{Admitted: true}
	}
	gctx, cancel := context.WithTimeout(ctx, gateTimeout)
	defer cancel()
	adm, err := s.gate.AdmitCall(gctx, gatewayID, s.httpTimeout+workMargin)
	if err != nil {
		s.metrics.Increment(ports.MetricGatewayProbeGateError, map[string]string{"gateway_id": gatewayID})
		return domaingateway.Admission{Admitted: true}
	}
	return adm
}

func (s *Service) releaseProbe(ctx context.Context, gatewayID string) {
	gctx, cancel := context.WithTimeout(ctx, gateTimeout)
	defer cancel()
	if err := s.gate.ReleaseProbe(gctx, gatewayID); err != nil {
		s.metrics.Increment(ports.MetricGatewayProbeGateError, map[string]string{"gateway_id": gatewayID})
	}
}

func (s *Service) switchGateway(ctx context.Context, txn *transaction.Transaction, next, reason string, attempt int) bool {
	prevGateway, prevReference := txn.GatewayID, txn.GatewayReferenceID
	txn.GatewayID = next
	txn.ActualGateway = next
	txn.GatewayReferenceID = ""
	if err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		return s.repo.UpdateStatus(ctx, txn)
	}); err != nil {
		s.log.Warn(ports.LogEventGatewayFallback, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldGatewayID:     next,
			"error":                  err.Error(),
			"stage":                  "persist_fallback",
		})
		txn.GatewayID = prevGateway
		txn.ActualGateway = prevGateway
		txn.GatewayReferenceID = prevReference
		return false
	}
	s.log.Info(ports.LogEventGatewayFallback, map[string]any{
		ports.FieldTransactionID: txn.ID.String(),
		ports.FieldGatewayID:     prevGateway,
		"fallback_gateway":       next,
		"reason":                 reason,
		ports.FieldAttemptNumber: attempt,
	})
	s.metrics.Increment(ports.MetricGatewayFallbackTriggered, map[string]string{
		"from_gateway": prevGateway,
		"to_gateway":   next,
		"reason":       reason,
	})
	if txn.ProcessingTimeout != nil {
		s.exitIntent(ctx, prevGateway, txn.ID)
		s.enterIntent(ctx, next, txn.ID, *txn.ProcessingTimeout)
	}
	return true
}

func isDecline(gwErr *ports.GatewayError) bool {
	return gwErr.Category == ports.ErrorCategoryHardDecline || gwErr.Category == ports.ErrorCategorySoftDecline
}

func (s *Service) saveReference(ctx context.Context, txn *transaction.Transaction, resp *ports.GatewayPaymentResponse) {
	if resp == nil || resp.GatewayReferenceID == "" || resp.GatewayReferenceID == txn.GatewayReferenceID {
		return
	}
	txn.GatewayReferenceID = resp.GatewayReferenceID
	if err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		return s.repo.UpdateStatus(ctx, txn)
	}); err != nil {
		s.log.Warn(ports.LogEventGatewayResponse, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldGatewayID:     txn.GatewayID,
			"error":                  err.Error(),
			"stage":                  "persist_reference",
		})
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
		MerchantTier:    s.merchantTier(txn.MerchantID),
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

func (s *Service) enterIntent(ctx context.Context, gatewayID string, txnID uuid.UUID, ttl time.Duration) {
	if s.intents == nil {
		return
	}
	if err := s.intents.EnterProcessing(ctx, gatewayID, txnID, ttl); err != nil {
		s.log.Warn(ports.LogEventGatewayResponse, map[string]any{
			ports.FieldTransactionID: txnID.String(),
			ports.FieldGatewayID:     gatewayID,
			"error":                  err.Error(),
			"stage":                  "intent_enter",
		})
	}
}

func (s *Service) exitIntent(ctx context.Context, gatewayID string, txnID uuid.UUID) {
	if s.intents == nil {
		return
	}
	if err := s.intents.ExitProcessing(ctx, gatewayID, txnID); err != nil {
		s.log.Warn(ports.LogEventGatewayResponse, map[string]any{
			ports.FieldTransactionID: txnID.String(),
			ports.FieldGatewayID:     gatewayID,
			"error":                  err.Error(),
			"stage":                  "intent_exit",
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
		switch gwErr.Category {
		case ports.ErrorCategoryAmbiguous, ports.ErrorCategoryNetworkTimeout, ports.ErrorCategoryGatewayError:
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
		AggregateVersion:   txn.Version,
	})
	if err != nil {
		return nil, fmt.Errorf("payment: marshal terminal event: %w", err)
	}

	return &ports.OutboxEvent{
		AggregateID:      txn.ID,
		AggregateType:    "transaction",
		EventType:        eventType,
		Payload:          payload,
		EventVersion:     1,
		AggregateVersion: txn.Version,
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
