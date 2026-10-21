package server_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"samarth/payment-service/internal/adapters/security"
	"samarth/payment-service/internal/ports"
	"samarth/payment-service/internal/server"
)

type noopLog struct{}

func (noopLog) Info(string, map[string]any)         {}
func (noopLog) Warn(string, map[string]any)         {}
func (noopLog) Error(string, map[string]any, error) {}
func (noopLog) Debug(string, map[string]any)        {}
func (noopLog) Trace(string, map[string]any)        {}
func (l noopLog) With(map[string]any) ports.Logger  { return l }

type noopMetrics struct{}

func (noopMetrics) Increment(string, map[string]string)          {}
func (noopMetrics) Histogram(string, float64, map[string]string) {}
func (noopMetrics) Gauge(string, float64, map[string]string)     {}

type run struct {
	addrs  map[string]net.Addr
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func start(t *testing.T, listeners []server.Listener, opts server.Options) *run {
	t.Helper()
	ready := make(chan map[string]net.Addr, 1)
	opts.Logger = noopLog{}
	userReady := opts.OnReady
	opts.OnReady = func(a map[string]net.Addr) {
		if userReady != nil {
			userReady(a)
		}
		ready <- a
	}
	if opts.ShutdownTimeout == 0 {
		opts.ShutdownTimeout = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &run{cancel: cancel, done: make(chan struct{})}
	go func() {
		r.err = server.Run(ctx, listeners, opts)
		close(r.done)
	}()

	select {
	case r.addrs = <-ready:
	case <-r.done:
		cancel()
		t.Fatalf("Run returned before serving: %v", r.err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("listeners did not become ready")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.done:
		case <-time.After(10 * time.Second):
		}
	})
	return r
}

func (r *run) wait(t *testing.T, within time.Duration) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(within):
		t.Fatalf("Run did not return within %s", within)
		return nil
	}
}

func (r *run) url(name, path string) string {
	return "http://" + r.addrs[name].String() + path
}

func ok(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }

func slow(d time.Duration, started chan<- struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if started != nil {
			started <- struct{}{}
		}
		time.Sleep(d)
		_, _ = w.Write([]byte("done"))
	}
}

func get(url string) (int, error) {
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func TestRun_BindFailureReleasesEarlierListeners(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()

	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	freeAddr := free.Addr().String()
	free.Close()

	err = server.Run(context.Background(), []server.Listener{
		{Name: "merchant", Addr: freeAddr, Handler: http.HandlerFunc(ok)},
		{Name: "webhook", Addr: taken.Addr().String(), Handler: http.HandlerFunc(ok)},
	}, server.Options{ShutdownTimeout: time.Second, Logger: noopLog{}})
	if err == nil || !strings.Contains(err.Error(), "webhook listener: bind") {
		t.Fatalf("a port already in use must fail startup naming the listener, got: %v", err)
	}

	again, err := net.Listen("tcp", freeAddr)
	if err != nil {
		t.Fatalf("the listener bound before the failure must be released, got: %v", err)
	}
	again.Close()
}

func TestRun_OnReadyAfterEveryListenerIsBound(t *testing.T) {
	var readyCalls atomic.Int32
	r := start(t, []server.Listener{
		{Name: "merchant", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok)},
		{Name: "webhook", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok)},
		{Name: "health", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok), DrainLast: true},
	}, server.Options{OnReady: func(map[string]net.Addr) { readyCalls.Add(1) }})

	if readyCalls.Load() != 1 || len(r.addrs) != 3 {
		t.Fatalf("OnReady must run once with every address, calls=%d addrs=%v", readyCalls.Load(), r.addrs)
	}
	for _, name := range []string{"merchant", "webhook", "health"} {
		if code, err := get(r.url(name, "/")); err != nil || code != http.StatusOK {
			t.Errorf("%s listener not serving: %d %v", name, code, err)
		}
	}
}

func TestRun_StopsAcceptingOnEveryListenerAtOnce(t *testing.T) {
	started := make(chan struct{}, 1)
	r := start(t, []server.Listener{
		{Name: "merchant", Addr: "127.0.0.1:0", Handler: slow(800*time.Millisecond, started)},
		{Name: "webhook", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok)},
	}, server.Options{})

	inflight := make(chan int, 1)
	go func() {
		code, _ := get(r.url("merchant", "/"))
		inflight <- code
	}()
	<-started

	r.cancel()
	deadline := time.Now().Add(400 * time.Millisecond)
	for {
		if _, err := get(r.url("webhook", "/")); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the webhook listener kept accepting while the merchant listener drained: shutdown is not concurrent")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if code := <-inflight; code != http.StatusOK {
		t.Fatalf("the in-flight request must complete during graceful shutdown, got %d", code)
	}
	if err := r.wait(t, 3*time.Second); err != nil {
		t.Fatalf("a graceful shutdown must return nil, got: %v", err)
	}
}

func TestRun_SlowListenerDoesNotConsumeAnotherListenersBudget(t *testing.T) {
	startedA, startedB := make(chan struct{}, 1), make(chan struct{}, 1)
	r := start(t, []server.Listener{
		{Name: "merchant", Addr: "127.0.0.1:0", Handler: slow(500*time.Millisecond, startedA)},
		{Name: "webhook", Addr: "127.0.0.1:0", Handler: slow(500*time.Millisecond, startedB)},
	}, server.Options{ShutdownTimeout: 900 * time.Millisecond})

	codes := make(chan int, 2)
	for _, name := range []string{"merchant", "webhook"} {
		go func() {
			code, _ := get(r.url(name, "/"))
			codes <- code
		}()
	}
	<-startedA
	<-startedB

	begin := time.Now()
	r.cancel()
	if err := r.wait(t, 3*time.Second); err != nil {
		t.Fatalf("both listeners fit the budget when drained concurrently, got: %v", err)
	}
	if elapsed := time.Since(begin); elapsed > 800*time.Millisecond {
		t.Errorf("shutdown took %s; listeners were drained one after another", elapsed)
	}
	for range 2 {
		if code := <-codes; code != http.StatusOK {
			t.Errorf("in-flight request lost: %d", code)
		}
	}
}

func TestRun_ForcesCloseWhenBudgetExpires(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	r := start(t, []server.Listener{
		{Name: "merchant", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			started <- struct{}{}
			<-release
		})},
	}, server.Options{ShutdownTimeout: 200 * time.Millisecond})

	reqErr := make(chan error, 1)
	go func() {
		_, err := get(r.url("merchant", "/"))
		reqErr <- err
	}()
	<-started

	r.cancel()
	err := r.wait(t, 2*time.Second)
	if err == nil || !strings.Contains(err.Error(), "merchant listener: graceful shutdown") || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("an expired budget must be reported, got: %v", err)
	}
	select {
	case err := <-reqErr:
		if err == nil {
			t.Fatal("the hung request must be cut off by Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cut off the hung connection")
	}
}

func TestRun_DrainingKeepsServingThroughTheDelay(t *testing.T) {
	draining := make(chan struct{})
	r := start(t, []server.Listener{
		{Name: "merchant", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok)},
		{Name: "health", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok), DrainLast: true},
	}, server.Options{
		DrainDelay: 400 * time.Millisecond,
		OnDraining: func() { close(draining) },
	})

	r.cancel()
	select {
	case <-draining:
	case <-time.After(time.Second):
		t.Fatal("OnDraining was not called")
	}
	for _, name := range []string{"merchant", "health"} {
		if code, err := get(r.url(name, "/")); err != nil || code != http.StatusOK {
			t.Errorf("%s must keep serving during the drain delay so the load balancer can see readiness fail, got %d %v", name, code, err)
		}
	}
	if err := r.wait(t, 3*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestRun_HealthListenerStopsLast(t *testing.T) {
	started := make(chan struct{}, 1)
	r := start(t, []server.Listener{
		{Name: "merchant", Addr: "127.0.0.1:0", Handler: slow(600*time.Millisecond, started)},
		{Name: "health", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok), DrainLast: true},
	}, server.Options{})

	go func() { _, _ = get(r.url("merchant", "/")) }()
	<-started

	r.cancel()
	time.Sleep(100 * time.Millisecond)
	if code, err := get(r.url("health", "/")); err != nil || code != http.StatusOK {
		t.Fatalf("health must answer while the other listeners drain, got %d %v", code, err)
	}
	if err := r.wait(t, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := get(r.url("health", "/")); err == nil {
		t.Fatal("health must be closed once Run returns")
	}
}

func TestRun_HealthKeepsItsOwnDeadlineWhenOthersUseTheBudget(t *testing.T) {
	merchantStarted, healthStarted := make(chan struct{}, 1), make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	r := start(t, []server.Listener{
		{Name: "merchant", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			merchantStarted <- struct{}{}
			<-release
		})},
		{Name: "health", Addr: "127.0.0.1:0", Handler: slow(600*time.Millisecond, healthStarted), DrainLast: true},
	}, server.Options{ShutdownTimeout: 200 * time.Millisecond})

	go func() { _, _ = get(r.url("merchant", "/")) }()
	<-merchantStarted
	healthCode := make(chan int, 1)
	go func() {
		code, _ := get(r.url("health", "/"))
		healthCode <- code
	}()
	<-healthStarted

	r.cancel()
	err := r.wait(t, 3*time.Second)
	if err == nil || !strings.Contains(err.Error(), "merchant listener") || strings.Contains(err.Error(), "health listener") {
		t.Fatalf("only the hung merchant listener should be force-closed, got: %v", err)
	}
	if code := <-healthCode; code != http.StatusOK {
		t.Fatalf("an in-flight probe must finish even after the others used the whole budget, got %d", code)
	}
}

func TestRun_ServeErrorStopsTheOthers(t *testing.T) {
	r := start(t, []server.Listener{
		{Name: "merchant", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok), TLS: &tls.Config{}},
		{Name: "webhook", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok)},
	}, server.Options{})

	err := r.wait(t, 3*time.Second)
	if err == nil || !strings.Contains(err.Error(), "merchant listener") {
		t.Fatalf("a listener that cannot serve must stop the process, got: %v", err)
	}
	if _, err := get(r.url("webhook", "/")); err == nil {
		t.Fatal("the healthy listener must be shut down too")
	}
}

func TestRun_ReadHeaderTimeoutDropsSlowClients(t *testing.T) {
	r := start(t, []server.Listener{
		{Name: "webhook", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok), Timeouts: server.Timeouts{ReadHeader: 150 * time.Millisecond}},
	}, server.Options{})

	conn, err := net.Dial("tcp", r.addrs["webhook"].String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("POST /webhooks/gateway/stripe HTTP/1.1\r\nHost: x\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = bufio.NewReader(conn).ReadByte()
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatal("a client that never finishes its headers must be dropped by the server")
	}
}

func TestRun_WriteTimeoutBoundsSlowHandlers(t *testing.T) {
	r := start(t, []server.Listener{
		{Name: "merchant", Addr: "127.0.0.1:0", Handler: slow(500*time.Millisecond, nil), Timeouts: server.Timeouts{Write: 100 * time.Millisecond}},
	}, server.Options{})

	if _, err := get(r.url("merchant", "/")); err == nil {
		t.Fatal("a response written after WriteTimeout must not reach the client")
	}
}

type pki struct {
	dir     string
	caCert  *x509.Certificate
	caKey   *ecdsa.PrivateKey
	caPEM   []byte
	serial  int64
	rootsCA *x509.CertPool
}

func newPKI(t *testing.T) *pki {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	p := &pki{dir: t.TempDir(), caCert: cert, caKey: key, serial: 1, rootsCA: x509.NewCertPool()}
	p.caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	p.rootsCA.AddCert(cert)
	return p
}

func (p *pki) issue(t *testing.T, name string, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	p.serial++
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(p.serial),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
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

func (p *pki) write(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(p.dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (p *pki) manager(t *testing.T, name string, withCA bool) *security.Manager {
	t.Helper()
	certPEM, keyPEM := p.issue(t, "server", x509.ExtKeyUsageServerAuth)
	cfg := security.Config{
		Name:     name,
		CertFile: p.write(t, name+".crt", certPEM),
		KeyFile:  p.write(t, name+".key", keyPEM),
	}
	if withCA {
		cfg.CAFile = p.write(t, "ca.crt", p.caPEM)
	}
	m, err := security.NewManager(cfg, noopLog{}, noopMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (p *pki) client(t *testing.T, cert *tls.Certificate) *http.Client {
	cfg := &tls.Config{RootCAs: p.rootsCA, MinVersion: tls.VersionTLS13}
	if cert != nil {
		cfg.Certificates = []tls.Certificate{*cert}
	}
	return &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: cfg, DisableKeepAlives: true}}
}

func TestRun_TLSListeners(t *testing.T) {
	p := newPKI(t)
	merchant := p.manager(t, "merchant", true)
	webhook := p.manager(t, "webhook", false)

	r := start(t, []server.Listener{
		{Name: "merchant", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok), TLS: merchant.TLSConfig()},
		{Name: "webhook", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok), TLS: webhook.TLSConfig()},
		{Name: "health", Addr: "127.0.0.1:0", Handler: http.HandlerFunc(ok), DrainLast: true},
	}, server.Options{})

	https := func(name string) string { return "https://" + r.addrs[name].String() + "/" }

	clientPEM, clientKey := p.issue(t, "merchant-client", x509.ExtKeyUsageClientAuth)
	clientCert, err := tls.X509KeyPair(clientPEM, clientKey)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("merchant admits a client certificate from the CA", func(t *testing.T) {
		resp, err := p.client(t, &clientCert).Get(https("merchant"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("got %d", resp.StatusCode)
		}
	})

	t.Run("merchant refuses a client without a certificate", func(t *testing.T) {
		if resp, err := p.client(t, nil).Get(https("merchant")); err == nil {
			resp.Body.Close()
			t.Fatal("strict mTLS must refuse the handshake without a client certificate")
		}
	})

	t.Run("webhook admits a client without a certificate", func(t *testing.T) {
		resp, err := p.client(t, nil).Get(https("webhook"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("got %d", resp.StatusCode)
		}
	})

	t.Run("TLS listeners refuse plaintext", func(t *testing.T) {
		for _, name := range []string{"merchant", "webhook"} {
			if code, err := get(r.url(name, "/")); err == nil && code == http.StatusOK {
				t.Errorf("%s served a plaintext request", name)
			}
		}
	})

	t.Run("health is plain HTTP", func(t *testing.T) {
		if code, err := get(r.url("health", "/")); err != nil || code != http.StatusOK {
			t.Fatalf("got %d %v", code, err)
		}
	})
}
