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

type Config struct {
	CertFile        string
	KeyFile         string
	CAFile          string
	StrictMTLS      bool
	RefreshInterval time.Duration
}

type Manager struct {
	cfg Config
	log ports.Logger

	mu     sync.RWMutex
	cert   *tls.Certificate
	caPool *x509.CertPool
}

func NewManager(cfg Config, log ports.Logger) (*Manager, error) {
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return nil, fmt.Errorf("security: TLS cert and key files are required")
	}
	m := &Manager{cfg: cfg, log: log}
	if err := m.reload(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Manager) TLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		GetConfigForClient: m.configForClient,
	}
}

func (m *Manager) configForClient(*tls.ClientHelloInfo) (*tls.Config, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{*m.cert},
		ClientCAs:    m.caPool,
		ClientAuth:   m.clientAuthType(),
	}, nil
}

func (m *Manager) clientAuthType() tls.ClientAuthType {
	if m.caPool == nil {
		return tls.NoClientCert
	}
	if m.cfg.StrictMTLS {
		return tls.RequireAndVerifyClientCert
	}
	return tls.VerifyClientCertIfGiven
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
					m.log.Error(ports.LogEventTLSCertReloadFailed, map[string]any{
						ports.FieldErrorCode:     "tls_cert_reload_failed",
						ports.FieldTraceID:       "",
						ports.FieldTransactionID: "",
					}, err)
					continue
				}
				m.log.Info(ports.LogEventTLSCertReloaded, nil)
			}
		}
	}()
}

func (m *Manager) reload() error {
	cert, err := tls.LoadX509KeyPair(m.cfg.CertFile, m.cfg.KeyFile)
	if err != nil {
		return fmt.Errorf("security: load key pair: %w", err)
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
	m.caPool = pool
	m.mu.Unlock()
	return nil
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
