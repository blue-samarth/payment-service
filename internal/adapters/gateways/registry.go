package gateways

import (
	"fmt"
	"sync"

	"samarth/payment-service/internal/ports"
)

type Registry struct {
	mu       sync.RWMutex
	adapters map[string]ports.GatewayAdapter
}

func NewRegistry() *Registry {
	return &Registry{adapters: make(map[string]ports.GatewayAdapter)}
}

func (r *Registry) Register(gatewayID string, adapter ports.GatewayAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[gatewayID] = adapter
}

func (r *Registry) Get(gatewayID string) (ports.GatewayAdapter, error) {
	r.mu.RLock()
	adapter, ok := r.adapters[gatewayID]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("gateways: no adapter registered for %q", gatewayID)
	}
	return adapter, nil
}

func (r *Registry) WebhookParser(gatewayID string) (ports.GatewayWebhookParser, bool) {
	r.mu.RLock()
	adapter, ok := r.adapters[gatewayID]
	r.mu.RUnlock()
	if !ok {
		return nil, false
	}
	parser, ok := adapter.(ports.GatewayWebhookParser)
	return parser, ok
}

func (r *Registry) IDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.adapters))
	for id := range r.adapters {
		ids = append(ids, id)
	}
	return ids
}
