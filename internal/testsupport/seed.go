package testsupport

import (
	"context"
	_ "embed"
	"testing"
)

//go:embed seed_stripe_card.sql
var seedStripeCardSQL string

func SeedStripeCardGateway(t *testing.T, pg *PG) {
	t.Helper()
	if _, err := pg.DB.Pool().Exec(context.Background(), seedStripeCardSQL); err != nil {
		t.Fatalf("testsupport: seed stripe gateway: %v", err)
	}
}
