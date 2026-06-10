// Package server implements the H02 TCP and UDP listeners.
package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"h02-server/pkg/protocol"
	"h02-server/server/internal/config"
	"h02-server/server/internal/forwarder"
	"h02-server/server/internal/handler"
	"h02-server/server/internal/metrics"
	"h02-server/server/internal/session"
	"h02-server/server/internal/store"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// TCPServer runs the H02 TCP listener and HTTP observability endpoint.
type TCPServer struct {
	cfg     *config.Config
	reg     *session.Registry
	handler *handler.Handler
	metrics *metrics.TCPMetrics
	log     *zap.Logger
}

func NewTCP(
	cfg *config.Config,
	reg *session.Registry,
	fwd *forwarder.Stream,
	devices *store.DeviceStore,
	m *metrics.TCPMetrics,
	log *zap.Logger,
) *TCPServer {
	h := handler.New(cfg, fwd, devices, reg, log)
	return &TCPServer{cfg: cfg, reg: reg, handler: h, metrics: m, log: log}
}

// Run starts the TCP listener and HTTP server, blocking until ctx is cancelled.
func (s *TCPServer) Run(ctx context.Context) error {
	lc := net.ListenConfig{KeepAlive: 60 * time.Second}
	ln, err := lc.Listen(ctx, "tcp", s.cfg.TCPAddr)
	if err != nil {
		return err
	}
	tcpLn := ln.(*net.TCPListener)
	s.log.Info("H02 TCP listening", zap.String("addr", s.cfg.TCPAddr))

	httpSrv := s.startHTTP(ctx)
	go s.pruneLoop(ctx)

	go func() {
		<-ctx.Done()
		tcpLn.Close()
	}()

	for {
		conn, err := tcpLn.AcceptTCP()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			s.log.Error("accept error", zap.Error(err))
			time.Sleep(50 * time.Millisecond)
			continue
		}

		conn.SetKeepAlive(true)                       //nolint:errcheck
		conn.SetKeepAlivePeriod(60 * time.Second)     //nolint:errcheck
		s.metrics.ConnectionsTotal.Inc()
		s.metrics.ConnectionsActive.Inc()
		go s.handleConn(ctx, conn)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	httpSrv.Shutdown(shutCtx) //nolint:errcheck
	s.log.Info("H02 TCP server stopped")
	return nil
}

func (s *TCPServer) handleConn(ctx context.Context, conn *net.TCPConn) {
	defer func() {
		conn.Close() //nolint:errcheck
		s.metrics.ConnectionsActive.Dec()
	}()

	sess := session.NewSession(conn)
	defer s.reg.Unregister(ctx, sess)

	s.log.Info("H02 TCP device connected", zap.String("addr", sess.RemoteAddr()))

	// Close unauthenticated connections after AUTH_TIMEOUT.
	authTimer := time.AfterFunc(s.cfg.AuthTimeout, func() {
		if sess.State() < session.StateLoggedIn {
			s.log.Warn("H02 TCP auth timeout — closing",
				zap.String("addr", sess.RemoteAddr()))
			sess.Close()
		}
	})
	defer authTimer.Stop()

	// bufio.Scanner with custom split function for H02 '#'-terminated frames.
	scanner := bufio.NewScanner(conn)
	scanner.Split(protocol.SplitOnHash)
	scanner.Buffer(make([]byte, 512), 512)

	for {
		conn.SetReadDeadline(time.Now().Add(s.cfg.HeartbeatTimeout)) //nolint:errcheck

		if !scanner.Scan() {
			err := scanner.Err()
			if ctx.Err() != nil {
				return
			}
			if err == nil {
				// Clean EOF — device disconnected.
				s.log.Info("H02 TCP device disconnected",
					zap.String("imei", sess.IMEI),
					zap.String("addr", sess.RemoteAddr()),
				)
			} else if isTimeout(err) {
				s.log.Info("H02 TCP device heartbeat timeout",
					zap.String("imei", sess.IMEI),
					zap.String("addr", sess.RemoteAddr()),
				)
			} else {
				s.log.Debug("H02 TCP connection closed",
					zap.String("imei", sess.IMEI),
					zap.String("addr", sess.RemoteAddr()),
					zap.Error(err),
				)
			}
			return
		}

		raw := scanner.Text()
		f, err := protocol.ParseFrame(raw)
		if err != nil {
			s.log.Debug("H02 TCP frame parse error",
				zap.String("addr", sess.RemoteAddr()),
				zap.String("raw", raw[:min(len(raw), 80)]),
				zap.Error(err),
			)
			s.metrics.DecodeErrors.Inc()
			continue
		}

		s.handler.DispatchTCP(ctx, sess, f, s.metrics)
	}
}

func (s *TCPServer) startHTTP(ctx context.Context) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok")) //nolint:errcheck
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready")) //nolint:errcheck
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"service":"h02-tcp","tcp_port":7020,"status":"ok","endpoints":["/healthz","/readyz","/metrics"]}`) //nolint:errcheck
	})

	srv := &http.Server{Addr: s.cfg.TCPHTTPAddr, Handler: mux}
	go func() {
		s.log.Info("H02 TCP HTTP observability listening", zap.String("addr", s.cfg.TCPHTTPAddr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("H02 TCP HTTP server error", zap.Error(err))
		}
	}()
	return srv
}

func (s *TCPServer) pruneLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HeartbeatTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.reg.PruneStale(ctx, s.cfg.HeartbeatTimeout*2); err != nil {
				s.log.Warn("stale session prune error", zap.Error(err))
			}
		}
	}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
