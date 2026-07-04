package gateways

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"samarth/payment-service/internal/ports"
)

type stubAdapter struct{ ports.GatewayAdapter }

func (stubAdapter) Capabilities() ports.GatewayCapabilities { return ports.GatewayCapabilities{} }
func (stubAdapter) InitiatePayment(context.Context, ports.GatewayPaymentRequest) (*ports.GatewayPaymentResponse, error) {
	return nil, nil
}
func (stubAdapter) CheckStatus(context.Context, ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	return nil, nil
}
func (stubAdapter) Refund(context.Context, ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	return nil, nil
}
func (stubAdapter) Cancel(context.Context, ports.GatewayCancelRequest) (*ports.GatewayCancelResponse, error) {
	return nil, nil
}

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	r.Register("stripe", stubAdapter{})

	got, err := r.Get("stripe")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Error("expected registered adapter")
	}
}

func TestRegistry_GetUnknown(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Get("missing"); err == nil {
		t.Error("expected error for unregistered gateway")
	}
}

func TestRegistry_IDs(t *testing.T) {
	r := NewRegistry()
	r.Register("stripe", stubAdapter{})
	r.Register("razorpay", stubAdapter{})
	if len(r.IDs()) != 2 {
		t.Errorf("expected 2 registered IDs, got %d", len(r.IDs()))
	}
}

func TestRegistry_ConcurrentAccess(t *testing.T) {
	r := NewRegistry()
	r.Register("stripe", stubAdapter{})

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			r.Register(fmt.Sprintf("gw-%d", i), stubAdapter{})
		}(i)
		go func() {
			defer wg.Done()
			_, _ = r.Get("stripe")
			_, _ = r.WebhookParser("stripe")
			_ = r.IDs()
		}()
	}
	wg.Wait()
}
