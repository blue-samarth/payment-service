package security

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"samarth/payment-service/internal/ports"
)

type noopLogger struct{}

func (noopLogger) Info(string, map[string]any)         {}
func (noopLogger) Warn(string, map[string]any)         {}
func (noopLogger) Error(string, map[string]any, error) {}
func (noopLogger) Debug(string, map[string]any)        {}
func (noopLogger) Trace(string, map[string]any)        {}
func (l noopLogger) With(map[string]any) ports.Logger  { return l }

type noopMetrics struct{}

func (noopMetrics) Increment(string, map[string]string)          {}
func (noopMetrics) Histogram(string, float64, map[string]string) {}
func (noopMetrics) Gauge(string, float64, map[string]string)     {}

// capturingLogger records which events were emitted, keyed by event name.
type capturingLogger struct {
	mu     sync.Mutex
	events map[string]int
}

func newCapturingLogger() *capturingLogger { return &capturingLogger{events: map[string]int{}} }

func (l *capturingLogger) note(event string) {
	l.mu.Lock()
	l.events[event]++
	l.mu.Unlock()
}
func (l *capturingLogger) count(event string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.events[event]
}
func (l *capturingLogger) Info(e string, _ map[string]any)           { l.note(e) }
func (l *capturingLogger) Warn(e string, _ map[string]any)           { l.note(e) }
func (l *capturingLogger) Error(e string, _ map[string]any, _ error) { l.note(e) }
func (l *capturingLogger) Debug(string, map[string]any)              {}
func (l *capturingLogger) Trace(string, map[string]any)              {}
func (l *capturingLogger) With(map[string]any) ports.Logger          { return l }

// countingMetrics tracks Increment calls per metric name.
type countingMetrics struct {
	mu      sync.Mutex
	counter map[string]int
}

func newCountingMetrics() *countingMetrics { return &countingMetrics{counter: map[string]int{}} }

func (m *countingMetrics) Increment(name string, _ map[string]string) {
	m.mu.Lock()
	m.counter[name]++
	m.mu.Unlock()
}
func (m *countingMetrics) Histogram(string, float64, map[string]string) {}
func (m *countingMetrics) Gauge(string, float64, map[string]string)     {}
func (m *countingMetrics) count(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counter[name]
}

type certAuthority struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newCA(t *testing.T) *certAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &certAuthority{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

func (ca *certAuthority) issue(t *testing.T, cn string, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func clientCert(t *testing.T, certPEM, keyPEM []byte) tls.Certificate {
	t.Helper()
	c, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func poolOf(pems ...[]byte) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, p := range pems {
		pool.AppendCertsFromPEM(p)
	}
	return pool
}

// managerFor writes the server cert/key and CA to disk and builds a Manager,
// mirroring how cmd/api loads them from the configured file paths.
func managerFor(t *testing.T, ca *certAuthority, strict bool) *Manager {
	t.Helper()
	serverCert, serverKey := ca.issue(t, "localhost", x509.ExtKeyUsageServerAuth)
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	caFile := filepath.Join(dir, "ca.crt")
	writeFile(t, certFile, serverCert)
	writeFile(t, keyFile, serverKey)
	writeFile(t, caFile, ca.certPEM)

	mgr, err := NewManager(Config{
		CertFile:     certFile,
		KeyFile:      keyFile,
		CAFile:       caFile,
		OptionalMTLS: !strict,
	}, noopLogger{}, noopMetrics{})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return mgr
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// handshake drives a full TLS handshake over an in-memory pipe and returns the
// server-side and client-side results.
func handshake(t *testing.T, serverCfg, clientCfg *tls.Config) (serverErr, clientErr error) {
	t.Helper()
	cConn, sConn := net.Pipe()
	deadline := time.Now().Add(5 * time.Second)
	_ = cConn.SetDeadline(deadline)
	_ = sConn.SetDeadline(deadline)

	server := tls.Server(sConn, serverCfg)
	client := tls.Client(cConn, clientCfg)

	// Close each conn the moment its handshake returns so a rejection on one
	// side unblocks the peer immediately instead of stalling to the deadline.
	sDone := make(chan error, 1)
	cDone := make(chan error, 1)
	go func() { e := server.Handshake(); sConn.Close(); sDone <- e }()
	go func() { e := client.Handshake(); cConn.Close(); cDone <- e }()
	serverErr = <-sDone
	clientErr = <-cDone
	return serverErr, clientErr
}

func TestManager_StrictMTLS_AcceptsValidClientCert(t *testing.T) {
	ca := newCA(t)
	mgr := managerFor(t, ca, true)
	cCertPEM, cKeyPEM := ca.issue(t, "payment-client", x509.ExtKeyUsageClientAuth)

	clientCfg := &tls.Config{
		ServerName:   "localhost",
		RootCAs:      poolOf(ca.certPEM),
		Certificates: []tls.Certificate{clientCert(t, cCertPEM, cKeyPEM)},
	}

	if serverErr, clientErr := handshake(t, mgr.TLSConfig(), clientCfg); serverErr != nil || clientErr != nil {
		t.Fatalf("valid client cert should complete mTLS handshake, got serverErr=%v clientErr=%v", serverErr, clientErr)
	}
}

func TestManager_StrictMTLS_RejectsMissingClientCert(t *testing.T) {
	ca := newCA(t)
	mgr := managerFor(t, ca, true)

	clientCfg := &tls.Config{ServerName: "localhost", RootCAs: poolOf(ca.certPEM)}

	serverErr, _ := handshake(t, mgr.TLSConfig(), clientCfg)
	if serverErr == nil {
		t.Fatal("strict mTLS must reject a client presenting no certificate")
	}
}

func TestManager_StrictMTLS_RejectsUntrustedClientCert(t *testing.T) {
	ca := newCA(t)
	mgr := managerFor(t, ca, true)

	rogueCA := newCA(t)
	rCertPEM, rKeyPEM := rogueCA.issue(t, "rogue-client", x509.ExtKeyUsageClientAuth)
	clientCfg := &tls.Config{
		ServerName:   "localhost",
		RootCAs:      poolOf(ca.certPEM),
		Certificates: []tls.Certificate{clientCert(t, rCertPEM, rKeyPEM)},
	}

	serverErr, _ := handshake(t, mgr.TLSConfig(), clientCfg)
	if serverErr == nil {
		t.Fatal("strict mTLS must reject a client cert signed by an untrusted CA")
	}
}

func TestManager_NonStrict_AllowsMissingClientCert(t *testing.T) {
	ca := newCA(t)
	mgr := managerFor(t, ca, false)

	clientCfg := &tls.Config{ServerName: "localhost", RootCAs: poolOf(ca.certPEM)}

	if serverErr, clientErr := handshake(t, mgr.TLSConfig(), clientCfg); serverErr != nil || clientErr != nil {
		t.Fatalf("non-strict TLS should allow a client with no cert, got serverErr=%v clientErr=%v", serverErr, clientErr)
	}
}

func TestManager_Reload_PicksUpRotatedCert(t *testing.T) {
	ca := newCA(t)
	serverCert, serverKey := ca.issue(t, "localhost", x509.ExtKeyUsageServerAuth)
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	caFile := filepath.Join(dir, "ca.crt")
	writeFile(t, certFile, serverCert)
	writeFile(t, keyFile, serverKey)
	writeFile(t, caFile, ca.certPEM)

	mgr, err := NewManager(Config{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}, noopLogger{}, noopMetrics{})
	if err != nil {
		t.Fatal(err)
	}

	firstLeaf := mgr.cert.Certificate[0]

	// Rotate the server cert on disk under the same CA, then reload.
	rotatedCert, rotatedKey := ca.issue(t, "localhost", x509.ExtKeyUsageServerAuth)
	writeFile(t, certFile, rotatedCert)
	writeFile(t, keyFile, rotatedKey)
	if err := mgr.reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}

	if string(mgr.cert.Certificate[0]) == string(firstLeaf) {
		t.Error("reload should have swapped in the rotated server certificate")
	}

	// The rotated cert must still serve a successful mTLS handshake.
	cCertPEM, cKeyPEM := ca.issue(t, "client", x509.ExtKeyUsageClientAuth)
	clientCfg := &tls.Config{
		ServerName:   "localhost",
		RootCAs:      poolOf(ca.certPEM),
		Certificates: []tls.Certificate{clientCert(t, cCertPEM, cKeyPEM)},
	}
	if serverErr, clientErr := handshake(t, mgr.TLSConfig(), clientCfg); serverErr != nil || clientErr != nil {
		t.Fatalf("handshake after rotation failed: serverErr=%v clientErr=%v", serverErr, clientErr)
	}
}

func TestNewManager_RequiresCertAndKey(t *testing.T) {
	if _, err := NewManager(Config{CAFile: "x"}, noopLogger{}, noopMetrics{}); err == nil {
		t.Fatal("expected error when cert/key files are absent")
	}
}

func TestManager_StartRefreshStopsWithContext(t *testing.T) {
	ca := newCA(t)
	mgr := managerFor(t, ca, true)
	log := newCapturingLogger()
	mgr.log = log
	mgr.cfg.RefreshInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	mgr.StartRefresh(ctx)

	// Let a few reload ticks happen, then cancel and confirm the count freezes.
	time.Sleep(40 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
	afterCancel := log.count(ports.LogEventTLSCertReloaded)
	if afterCancel == 0 {
		t.Fatal("expected the refresh goroutine to have reloaded at least once before cancel")
	}
	time.Sleep(30 * time.Millisecond)
	if grew := log.count(ports.LogEventTLSCertReloaded); grew != afterCancel {
		t.Errorf("refresh goroutine kept running after cancel: %d -> %d reloads", afterCancel, grew)
	}
}

func TestManager_OptionalMTLS_WarnsAtStartup(t *testing.T) {
	ca := newCA(t)
	serverCert, serverKey := ca.issue(t, "localhost", x509.ExtKeyUsageServerAuth)
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	caFile := filepath.Join(dir, "ca.crt")
	writeFile(t, certFile, serverCert)
	writeFile(t, keyFile, serverKey)
	writeFile(t, caFile, ca.certPEM)

	log := newCapturingLogger()
	if _, err := NewManager(Config{CertFile: certFile, KeyFile: keyFile, CAFile: caFile, OptionalMTLS: true}, log, noopMetrics{}); err != nil {
		t.Fatal(err)
	}
	if log.count(ports.LogEventTLSMTLSOptional) != 1 {
		t.Error("optional-mTLS-with-CA must emit a loud startup warning that clients without a cert are admitted")
	}
}

func TestManager_EnforcedByDefault(t *testing.T) {
	ca := newCA(t)
	log := newCapturingLogger()
	mgr := managerFor(t, ca, true) // OptionalMTLS false — the fail-safe default
	mgr.log = log
	if got := mgr.clientAuthType(); got != tls.RequireAndVerifyClientCert {
		t.Errorf("a CA pool with OptionalMTLS unset must enforce client certs, got auth type %d", got)
	}
	if log.count(ports.LogEventTLSMTLSOptional) != 0 {
		t.Error("enforced mode must not emit the optional-mTLS warning")
	}
}

func issueExpiring(t *testing.T, ca *certAuthority, notBefore, notAfter time.Time) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func TestManager_RejectsTLS12Client(t *testing.T) {
	ca := newCA(t)
	mgr := managerFor(t, ca, true)
	cCertPEM, cKeyPEM := ca.issue(t, "payment-client", x509.ExtKeyUsageClientAuth)

	// A fully-valid client cert, but capped at TLS 1.2 — must be rejected by the 1.3 floor.
	clientCfg := &tls.Config{
		ServerName:   "localhost",
		RootCAs:      poolOf(ca.certPEM),
		Certificates: []tls.Certificate{clientCert(t, cCertPEM, cKeyPEM)},
		MaxVersion:   tls.VersionTLS12,
	}

	serverErr, _ := handshake(t, mgr.TLSConfig(), clientCfg)
	if serverErr == nil {
		t.Fatal("a TLS 1.2 client must be rejected by the TLS 1.3 floor")
	}
}

func TestManager_ExpiredCert_Refused(t *testing.T) {
	ca := newCA(t)
	expiredCert, expiredKey := issueExpiring(t, ca, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	writeFile(t, certFile, expiredCert)
	writeFile(t, keyFile, expiredKey)

	if _, err := NewManager(Config{CertFile: certFile, KeyFile: keyFile}, noopLogger{}, noopMetrics{}); err == nil {
		t.Fatal("startup must refuse an already-expired server certificate")
	}
}

func TestManager_ExpiringCert_Warns(t *testing.T) {
	ca := newCA(t)
	soonCert, soonKey := issueExpiring(t, ca, time.Now().Add(-time.Hour), time.Now().Add(6*time.Hour))
	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	writeFile(t, certFile, soonCert)
	writeFile(t, keyFile, soonKey)

	log := newCapturingLogger()
	if _, err := NewManager(Config{CertFile: certFile, KeyFile: keyFile}, log, noopMetrics{}); err != nil {
		t.Fatal(err)
	}
	if log.count(ports.LogEventTLSCertExpiring) != 1 {
		t.Error("a cert inside the renewal window must warn at load")
	}
}

func TestManager_Reload_ExpiredKeepsLastGoodAndCountsMetric(t *testing.T) {
	ca := newCA(t)
	mgr := managerFor(t, ca, true)
	metrics := newCountingMetrics()
	mgr.metrics = metrics
	goodLeaf := mgr.cert.Certificate[0]

	// Replace the on-disk cert with an expired one and reload directly.
	expiredCert, expiredKey := issueExpiring(t, ca, time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	writeFile(t, mgr.cfg.CertFile, expiredCert)
	writeFile(t, mgr.cfg.KeyFile, expiredKey)

	if err := mgr.reload(); err == nil {
		t.Fatal("reload must reject an expired certificate")
	}
	if string(mgr.cert.Certificate[0]) != string(goodLeaf) {
		t.Error("a failed reload must keep the last-known-good certificate serving")
	}

	// The background refresh path increments the failure counter.
	mgr.cfg.RefreshInterval = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	mgr.StartRefresh(ctx)
	time.Sleep(30 * time.Millisecond)
	cancel()
	if metrics.count(ports.MetricTLSCertReloadFailure) == 0 {
		t.Error("a failing background reload must increment the reload-failure metric for alerting")
	}
}
