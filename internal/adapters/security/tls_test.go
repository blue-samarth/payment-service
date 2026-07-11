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
		CertFile:   certFile,
		KeyFile:    keyFile,
		CAFile:     caFile,
		StrictMTLS: strict,
	}, noopLogger{})
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

	mgr, err := NewManager(Config{CertFile: certFile, KeyFile: keyFile, CAFile: caFile, StrictMTLS: true}, noopLogger{})
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
	if _, err := NewManager(Config{CAFile: "x"}, noopLogger{}); err == nil {
		t.Fatal("expected error when cert/key files are absent")
	}
}

func TestManager_StartRefreshStopsWithContext(t *testing.T) {
	ca := newCA(t)
	mgr := managerFor(t, ca, true)
	ctx, cancel := context.WithCancel(context.Background())
	mgr.cfg.RefreshInterval = 10 * time.Millisecond
	mgr.StartRefresh(ctx)
	cancel()
	// No assertion beyond "does not panic / leak past cancel"; give the goroutine a tick to observe cancellation.
	time.Sleep(20 * time.Millisecond)
}
