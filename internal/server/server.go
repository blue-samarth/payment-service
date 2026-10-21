package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"samarth/payment-service/internal/ports"
)

const drainLastTimeout = 5 * time.Second

type Timeouts struct {
	ReadHeader time.Duration
	Read       time.Duration
	Write      time.Duration
	Idle       time.Duration
}

type Listener struct {
	Name           string
	Addr           string
	Handler        http.Handler
	TLS            *tls.Config
	Transport      string
	Timeouts       Timeouts
	MaxHeaderBytes int
	DrainLast      bool
}

type Options struct {
	DrainDelay      time.Duration
	ShutdownTimeout time.Duration
	OnReady         func(addrs map[string]net.Addr)
	OnDraining      func()
	Logger          ports.Logger
}

type running struct {
	Listener
	srv *http.Server
	ln  net.Listener
}

func Run(ctx context.Context, listeners []Listener, opts Options) error {
	bound, err := bind(listeners)
	if err != nil {
		return err
	}

	serveErr := make(chan error, len(bound))
	var wg sync.WaitGroup
	addrs := make(map[string]net.Addr, len(bound))
	for _, r := range bound {
		addrs[r.Name] = r.ln.Addr()
		opts.Logger.Info(ports.LogEventHTTPServerStarting, map[string]any{
			"listener":  r.Name,
			"addr":      r.ln.Addr().String(),
			"transport": r.Transport,
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if r.TLS != nil {
				err = r.srv.ServeTLS(r.ln, "", "")
			} else {
				err = r.srv.Serve(r.ln)
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				serveErr <- fmt.Errorf("%s listener: %w", r.Name, err)
			}
		}()
	}

	if opts.OnReady != nil {
		opts.OnReady(addrs)
	}

	var runErr error
	select {
	case runErr = <-serveErr:
	case <-ctx.Done():
	}

	opts.Logger.Info(ports.LogEventHTTPServerDraining, map[string]any{"drain_delay": opts.DrainDelay.String()})
	if opts.OnDraining != nil {
		opts.OnDraining()
	}
	if runErr == nil && opts.DrainDelay > 0 {
		timer := time.NewTimer(opts.DrainDelay)
		select {
		case <-timer.C:
		case runErr = <-serveErr:
			timer.Stop()
		}
	}

	var first, last []running
	for _, r := range bound {
		if r.DrainLast {
			last = append(last, r)
		} else {
			first = append(first, r)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownTimeout)
	defer cancel()
	runErr = errors.Join(runErr, shutdownAll(shutdownCtx, first, opts.Logger))

	lastCtx, cancelLast := context.WithTimeout(context.Background(), drainLastTimeout)
	defer cancelLast()
	runErr = errors.Join(runErr, shutdownAll(lastCtx, last, opts.Logger))

	wg.Wait()
	close(serveErr)
	for err := range serveErr {
		runErr = errors.Join(runErr, err)
	}
	return runErr
}

func bind(listeners []Listener) ([]running, error) {
	bound := make([]running, 0, len(listeners))
	for _, l := range listeners {
		ln, err := net.Listen("tcp", l.Addr)
		if err != nil {
			for _, b := range bound {
				_ = b.ln.Close()
			}
			return nil, fmt.Errorf("%s listener: bind %s: %w", l.Name, l.Addr, err)
		}
		bound = append(bound, running{
			Listener: l,
			ln:       ln,
			srv: &http.Server{
				Handler:           l.Handler,
				TLSConfig:         l.TLS,
				ReadHeaderTimeout: l.Timeouts.ReadHeader,
				ReadTimeout:       l.Timeouts.Read,
				WriteTimeout:      l.Timeouts.Write,
				IdleTimeout:       l.Timeouts.Idle,
				MaxHeaderBytes:    l.MaxHeaderBytes,
			},
		})
	}
	return bound, nil
}

func shutdownAll(ctx context.Context, servers []running, log ports.Logger) error {
	errs := make([]error, len(servers))
	var wg sync.WaitGroup
	for i, r := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.srv.Shutdown(ctx); err != nil {
				log.Warn(ports.LogEventHTTPServerForcedClosed, map[string]any{"listener": r.Name, "error": err.Error()})
				errs[i] = errors.Join(
					fmt.Errorf("%s listener: graceful shutdown: %w", r.Name, err),
					r.srv.Close(),
				)
				return
			}
			log.Info(ports.LogEventHTTPServerStopped, map[string]any{"listener": r.Name})
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}
