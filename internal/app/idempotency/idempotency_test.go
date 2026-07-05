package idempotency

import (
	"context"
	"errors"
	"testing"
)

type row struct {
	requestHash string
	status      string
	response    []byte
}

type fakeStore struct {
	rows map[string]*row
}

func newFakeStore() *fakeStore { return &fakeStore{rows: map[string]*row{}} }

func (s *fakeStore) Reserve(_ context.Context, composite, requestHash string) (bool, error) {
	if _, ok := s.rows[composite]; ok {
		return false, nil
	}
	s.rows[composite] = &row{requestHash: requestHash, status: "PROCESSING"}
	return true, nil
}

func (s *fakeStore) Lookup(_ context.Context, composite string) (bool, string, string, []byte, error) {
	r, ok := s.rows[composite]
	if !ok {
		return false, "", "", nil, nil
	}
	return true, r.requestHash, r.status, r.response, nil
}

func (s *fakeStore) Complete(_ context.Context, composite string, response []byte) error {
	if r, ok := s.rows[composite]; ok {
		r.status = "COMPLETED"
		r.response = response
	}
	return nil
}

// rollbackTx mimics a real transaction: if fn errors, any reservation the fn
// wrote is undone, so the store never strands a PROCESSING row.
type rollbackTx struct{ store *fakeStore }

func (t rollbackTx) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	before := make(map[string]*row, len(t.store.rows))
	for k, v := range t.store.rows {
		clone := *v
		before[k] = &clone
	}
	if err := fn(ctx); err != nil {
		t.store.rows = before
		return err
	}
	return nil
}

func newGuard() (*Guard, *fakeStore) {
	store := newFakeStore()
	return NewGuard(store, rollbackTx{store: store}), store
}

func TestGuard_CreatedThenReplayed(t *testing.T) {
	g, _ := newGuard()
	ctx := context.Background()
	composite := Composite("m", "op", "k")
	rh := RequestHash("a", "b")

	ran := 0
	res, err := g.Execute(ctx, composite, rh, func(context.Context) ([]byte, error) {
		ran++
		return []byte(`{"id":"x"}`), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != Created || string(res.Response) != `{"id":"x"}` {
		t.Fatalf("expected Created with response, got %v %s", res.Verdict, res.Response)
	}

	res, err = g.Execute(ctx, composite, rh, func(context.Context) ([]byte, error) {
		ran++
		return []byte(`{"id":"y"}`), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != Replayed || string(res.Response) != `{"id":"x"}` {
		t.Fatalf("replay must return the first response, got %v %s", res.Verdict, res.Response)
	}
	if ran != 1 {
		t.Errorf("op must run exactly once across a create + replay, ran %d", ran)
	}
}

func TestGuard_KeyReusedOnDifferentRequestHash(t *testing.T) {
	g, _ := newGuard()
	ctx := context.Background()
	composite := Composite("m", "op", "k")

	_, _ = g.Execute(ctx, composite, RequestHash("a"), func(context.Context) ([]byte, error) {
		return []byte(`{}`), nil
	})
	res, err := g.Execute(ctx, composite, RequestHash("different"), func(context.Context) ([]byte, error) {
		t.Fatal("op must not run on a key-reuse conflict")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != KeyReused {
		t.Errorf("expected KeyReused, got %v", res.Verdict)
	}
}

func TestGuard_InProgressWhenReservedNotCompleted(t *testing.T) {
	store := newFakeStore()
	g := NewGuard(store, rollbackTx{store: store})
	composite := Composite("m", "op", "k")
	// Peer reserved but has not completed yet.
	store.rows[composite] = &row{requestHash: RequestHash("a"), status: "PROCESSING"}

	res, err := g.Execute(context.Background(), composite, RequestHash("a"), func(context.Context) ([]byte, error) {
		t.Fatal("op must not run while another request is in progress")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict != InProgress {
		t.Errorf("expected InProgress, got %v", res.Verdict)
	}
}

func TestGuard_OpErrorRollsBackReservation(t *testing.T) {
	g, store := newGuard()
	ctx := context.Background()
	composite := Composite("m", "op", "k")
	boom := errors.New("boom")

	_, err := g.Execute(ctx, composite, RequestHash("a"), func(context.Context) ([]byte, error) {
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("expected op error to propagate, got %v", err)
	}
	if _, stranded := store.rows[composite]; stranded {
		t.Error("a failed op must roll back the reservation, not strand a PROCESSING row")
	}

	// The key is free again: a retry can now claim it.
	res, err := g.Execute(ctx, composite, RequestHash("a"), func(context.Context) ([]byte, error) {
		return []byte(`{}`), nil
	})
	if err != nil || res.Verdict != Created {
		t.Errorf("retry after rollback should claim fresh, got %v err=%v", res.Verdict, err)
	}
}
