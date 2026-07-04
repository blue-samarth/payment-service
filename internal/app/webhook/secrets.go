package webhook

import "context"

type StaticSecretProvider struct{ secrets map[string]string }

func NewStaticSecretProvider(secrets map[string]string) *StaticSecretProvider {
	return &StaticSecretProvider{secrets: secrets}
}
func (p *StaticSecretProvider) WebhookSecret(_ context.Context, gatewayID string) (string, error) {
	return p.secrets[gatewayID], nil
}
