package transaction

import (
	"fmt"
	"time"
)

type ErrInvalidTransition struct {
	From  Status
	To    Status
	Actor Actor
}

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("transaction: invalid state transition %s → %s (actor: %s)", e.From, e.To, e.Actor)
}

var transitionTable = map[Status][]Status{
	StatusPending:      {StatusProcessing, StatusCancelled, StatusFailed},
	StatusProcessing:   {StatusSucceeded, StatusFailed},
	StatusSucceeded:    {StatusRefunded, StatusRefundFailed},
	StatusFailed:       {StatusCancelled},
	StatusRefundFailed: {StatusRefunded},
	StatusCancelled:    {},
	StatusRefunded:     {},
}

func isValidTransition(from, to Status) bool {
	allowed, ok := transitionTable[from]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == to {
			return true
		}
	}
	return false
}

func applyTransitionEffects(tx *Transaction, from, to Status, now time.Time) {
	switch {
	case to == StatusFailed || to == StatusCancelled || to == StatusSucceeded:
		if from == StatusProcessing {
			tx.ProcessingStartedAt = nil
			tx.ProcessingTimeout = nil
		}
	}
}

func TransitionState(tx *Transaction, toState Status, actor Actor) error {
	if tx == nil {
		return fmt.Errorf("transaction: nil transaction")
	}
	if !isValidTransition(tx.Status, toState) {
		return ErrInvalidTransition{From: tx.Status, To: toState, Actor: actor}
	}
	if tx.Status == StatusFailed && toState == StatusCancelled && !tx.CancelIntent {
		return fmt.Errorf("transaction: cancel intent required for FAILED → CANCELLED")
	}

	previous := tx.Status
	now := time.Now().UTC()

	tx.Status = toState
	tx.UpdatedAt = now

	applyTransitionEffects(tx, previous, toState, now)

	return nil
}

func ValidTransitionsFrom(s Status) []Status {
	result, ok := transitionTable[s]
	if !ok {
		return nil
	}
	out := make([]Status, len(result))
	copy(out, result)
	return out
}

func AllStatuses() []Status {
	all := []Status{
		StatusPending,
		StatusProcessing,
		StatusSucceeded,
		StatusFailed,
		StatusCancelled,
		StatusRefunded,
		StatusRefundFailed,
	}
	out := make([]Status, len(all))
	copy(out, all)
	return out
}
