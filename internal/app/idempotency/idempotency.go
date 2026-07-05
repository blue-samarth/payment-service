package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

var ErrKeyRequired = errors.New("idempotency: key required")

type Verdict int

const (
	Created Verdict = iota
	Replayed
	InProgress
	KeyReused
)

const statusCompleted = "COMPLETED"

type Store interface {
	Reserve(ctx context.Context, compositeHash, requestHash string) (bool, error)
	Lookup(ctx context.Context, compositeHash string) (found bool, requestHash, status string, response []byte, err error)
	Complete(ctx context.Context, compositeHash string, response []byte) error
}

type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type Result struct {
	Verdict  Verdict
	Response []byte
}

type Guard struct {
	store Store
	tx    Transactor
}

func NewGuard(store Store, tx Transactor) *Guard {
	return &Guard{store: store, tx: tx}
}

func (g *Guard) Execute(ctx context.Context, composite, requestHash string, op func(ctx context.Context) ([]byte, error)) (Result, error) {
	var res Result
	err := g.tx.WithinTx(ctx, func(ctx context.Context) error {
		claimed, err := g.store.Reserve(ctx, composite, requestHash)
		if err != nil {
			return err
		}
		if !claimed {
			found, prevHash, status, response, err := g.store.Lookup(ctx, composite)
			if err != nil {
				return err
			}
			switch {
			case !found:
				res = Result{Verdict: InProgress}
			case prevHash != requestHash:
				res = Result{Verdict: KeyReused}
			case status != statusCompleted:
				res = Result{Verdict: InProgress}
			default:
				res = Result{Verdict: Replayed, Response: response}
			}
			return nil
		}

		response, err := op(ctx)
		if err != nil {
			return err
		}
		if err := g.store.Complete(ctx, composite, response); err != nil {
			return err
		}
		res = Result{Verdict: Created, Response: response}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	return res, nil
}

func Composite(merchant, operation, key string) string {
	return hash(merchant + ":" + operation + ":" + key)
}

func RequestHash(parts ...string) string {
	joined := ""
	for i, p := range parts {
		if i > 0 {
			joined += "\x1f"
		}
		joined += p
	}
	return hash(joined)
}

func hash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
