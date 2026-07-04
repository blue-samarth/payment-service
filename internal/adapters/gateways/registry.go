package gateways

import (
	"fmt"

	"samarth/payment-service/internal/ports"
)

type Registry struct {
	adapters map[string]ports.GatewayAdapter
}

func NewRegistry() *Registry {
	return &Registry{adapters: make(map[string]ports.GatewayAdapter)}
}

func (r *Registry) Register(gatewayID string, adapter ports.GatewayAdapter) {
	r.adapters[gatewayID] = adapter
}

func (r *Registry) Get(gatewayID string) (ports.GatewayAdapter, error) {
	adapter, ok := r.adapters[gatewayID]
	if !ok {
		return nil, fmt.Errorf("gateways: no adapter registered for %q", gatewayID)
	}
	return adapter, nil
}

func (r *Registry) WebhookParser(gatewayID string) (ports.GatewayWebhookParser, bool) {
	adapter, ok := r.adapters[gatewayID]
	if !ok {
		return nil, false
	}
	parser, ok := adapter.(ports.GatewayWebhookParser)
	return parser, ok
}

func (r *Registry) IDs() []string {
	ids := make([]string, 0, len(r.adapters))
	for id := range r.adapters {
		ids = append(ids, id)
	}
	return ids
}
