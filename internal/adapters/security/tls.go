package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"samarth/payment-service/internal/ports"
)

const certExpiryWarnWindow = 14 * 24 * time.Hour

type Config struct {
	Name            string
	CertFile        string
	KeyFile         string
	CAFile          string
	OptionalMTLS    bool
	RefreshInterval time.Duration
	MinVersion      uint16
}

var tls12CipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
}

type Manager struct {
	cfg     Config
	log     ports.Logger
	metrics ports.MetricRecorder

	mu       sync.RWMutex
	cert     *tls.Certificate
	notAfter time.Time
	caPool   *x509.CertPool
}

func NewManager(cfg Config, log ports.Logger, metrics ports.MetricRecorder) (*Manager, error) {
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, fmt.Errorf("security: TLS cert and key files are required")
	}
	switch cfg.MinVersion {
	case 0:
		cfg.MinVersion = tls.VersionTLS13
	case tls.VersionTLS12, tls.VersionTLS13:
	default:
		return nil, fmt.Errorf("security: minimum TLS version must be 1.2 or 1.3")
	}
	m := &Manager{cfg: cfg, log: log, metrics: metrics}
	if err := m.reload(); err != nil {
		return nil, err
	}
	if m.caPool != nil && cfg.OptionalMTLS {
		m.log.Warn(ports.LogEventTLSMTLSOptional, map[string]any{
			"warning": "client-certificate verification is OPTIONAL; clients without a certificate will be admitted",
			"ca_file": cfg.CAFile,
		})
	}
	return m, nil
}

func (m *Manager) TLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:         m.cfg.MinVersion,
		CipherSuites:       m.cipherSuites(),
		GetConfigForClient: m.configForClient,
	}
}

func (m *Manager) configForClient(*tls.ClientHelloInfo) (*tls.Config, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &tls.Config{
		MinVersion:   m.cfg.MinVersion,
		CipherSuites: m.cipherSuites(),
		Certificates: []tls.Certificate{*m.cert},
		ClientCAs:    m.caPool,
		ClientAuth:   m.clientAuthType(),
	}, nil
}

func (m *Manager) cipherSuites() []uint16 {
	if m.cfg.MinVersion == tls.VersionTLS12 {
		return tls12CipherSuites
	}
	return nil
}

func (m *Manager) clientAuthType() tls.ClientAuthType {
	if m.caPool == nil {
		return tls.NoClientCert
	}
	if m.cfg.OptionalMTLS {
		return tls.VerifyClientCertIfGiven
	}
	return tls.RequireAndVerifyClientCert
}

func (m *Manager) StartRefresh(ctx context.Context) {
	if m.cfg.RefreshInterval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(m.cfg.RefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := m.reload(); err != nil {
					if m.metrics != nil {
						m.metrics.Increment(ports.MetricTLSCertReloadFailure, m.tags())
					}
					m.log.Error(ports.LogEventTLSCertReloadFailed, map[string]any{
						ports.FieldErrorCode:     "tls_cert_reload_failed",
						ports.FieldTraceID:       "",
						ports.FieldTransactionID: "",
						"listener":               m.cfg.Name,
					}, err)
					continue
				}
				m.log.Info(ports.LogEventTLSCertReloaded, map[string]any{"listener": m.cfg.Name})
			}
		}
	}()
}

func (m *Manager) reload() error {
	cert, err := tls.LoadX509KeyPair(m.cfg.CertFile, m.cfg.KeyFile)
	if err != nil {
		return fmt.Errorf("security: load key pair: %w", err)
	}

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("security: parse leaf certificate: %w", err)
	}
	if err := m.checkExpiry(leaf); err != nil {
		return err
	}

	var pool *x509.CertPool
	if m.cfg.CAFile != "" {
		pool, err = loadCAPool(m.cfg.CAFile)
		if err != nil {
			return err
		}
	}

	m.mu.Lock()
	m.cert = &cert
	m.notAfter = leaf.NotAfter
	m.caPool = pool
	m.mu.Unlock()
	return nil
}

func (m *Manager) Name() string {
	return m.cfg.Name
}

func (m *Manager) NotAfter() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.notAfter
}

func (m *Manager) checkExpiry(leaf *x509.Certificate) error {
	remaining := time.Until(leaf.NotAfter)
	if m.metrics != nil {
		m.metrics.Gauge(ports.MetricTLSCertExpirySeconds, remaining.Seconds(), m.tags())
	}
	if remaining <= 0 {
		return fmt.Errorf("security: server certificate expired at %s", leaf.NotAfter.UTC().Format(time.RFC3339))
	}
	if remaining < certExpiryWarnWindow {
		m.log.Warn(ports.LogEventTLSCertExpiring, map[string]any{
			"expires_at":      leaf.NotAfter.UTC().Format(time.RFC3339),
			"remaining_hours": int(remaining.Hours()),
			"listener":        m.cfg.Name,
		})
	}
	return nil
}

func (m *Manager) tags() map[string]string {
	if m.cfg.Name == "" {
		return nil
	}
	return map[string]string{"listener": m.cfg.Name}
}

func loadCAPool(caFile string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("security: read CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("security: no valid certificates in CA file %s", caFile)
	}
	return pool, nil
}
