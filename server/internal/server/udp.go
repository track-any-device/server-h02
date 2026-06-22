package server

import (
	"context"
	"errors"
	"fmt"
	"h02-server/pkg/protocol"
	"h02-server/server/internal/config"
	"h02-server/server/internal/forwarder"
	"h02-server/server/internal/handler"
	"h02-server/server/internal/metrics"
	"h02-server/server/internal/store"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const udpMaxMsgSize = 512

// approvedEntry is a cache slot in the UDP IMEI approval map.
type approvedEntry struct {
	approvedAt time.Time
	lastSeen   time.Time
}

// UDPServer runs the H02 UDP listener and HTTP observability endpoint.
type UDPServer struct {
	cfg     *config.Config
	fwd     *forwarder.Stream
	devices *store.DeviceStore
	handler *handler.Handler
	metrics *metrics.UDPMetrics
	log     *zap.Logger

	// approvedIMEIs is an in-memory cache of IMEI → last-seen time.
	// Entries are pruned every HeartbeatTimeout to avoid unbounded growth.
	approvedIMEIs sync.Map // map[string]*approvedEntry
}

func NewUDP(
	cfg *config.Config,
	fwd *forwarder.Stream,
	devices *store.DeviceStore,
	m *metrics.UDPMetrics,
	log *zap.Logger,
) *UDPServer {
	h := handler.New(cfg, fwd, devices, nil, log)
	return &UDPServer{cfg: cfg, fwd: fwd, devices: devices, handler: h, metrics: m, log: log}
}

// Run starts the UDP listener and HTTP server, blocking until ctx is cancelled.
func (s *UDPServer) Run(ctx context.Context) error {
	addr, err := net.ResolveUDPAddr("udp", s.cfg.UDPAddr)
	if err != nil {
		return fmt.Errorf("h02 udp: resolve %s: %w", s.cfg.UDPAddr, err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return fmt.Errorf("h02 udp: listen %s: %w", s.cfg.UDPAddr, err)
	}
	defer conn.Close() //nolint:errcheck

	s.log.Info("H02 UDP listening", zap.String("addr", s.cfg.UDPAddr))

	httpSrv := s.startHTTP(ctx)
	go s.pruneLoop(ctx)

	go func() {
		<-ctx.Done()
		conn.Close() //nolint:errcheck
	}()

	buf := make([]byte, udpMaxMsgSize)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			s.log.Debug("UDP read error", zap.Error(err))
			continue
		}

		s.metrics.DatagramsTotal.Inc()
		raw := string(buf[:n])
		go s.processDatagram(ctx, conn, remoteAddr, raw)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	httpSrv.Shutdown(shutCtx) //nolint:errcheck
	s.log.Info("H02 UDP server stopped")
	return nil
}

func (s *UDPServer) processDatagram(ctx context.Context, conn *net.UDPConn, remote *net.UDPAddr, raw string) {
	f, err := protocol.ParseFrame(raw)
	if err != nil {
		s.log.Debug("UDP parse error",
			zap.String("remote", remote.String()),
			zap.String("raw", raw[:min(len(raw), 80)]),
			zap.Error(err),
		)
		s.metrics.DecodeErrors.Inc()
		return
	}

	// Check approved-IMEI cache.
	imei := f.IMEI
	now := time.Now()

	if entry, ok := s.approvedIMEIs.Load(imei); ok {
		e := entry.(*approvedEntry)
		e.lastSeen = now
		// Device is cached as approved — dispatch directly.
		respond := func(data []byte) error {
			_, err := conn.WriteToUDP(data, remote)
			return err
		}
		s.handler.DispatchUDP(ctx, f, respond, s.metrics)
		// Update online sorted set for heartbeat-like presence tracking.
		s.fwd.UpdateOnlineUDP(ctx, imei, now) //nolint:errcheck
		return
	}

	// Cache miss: check MySQL (or auto-approve in DB-less mode).
	result, dbErr := s.devices.CheckOrCreate(ctx, imei, "H02")
	if dbErr != nil {
		s.log.Error("UDP device check failed — dropping datagram",
			zap.String("imei", imei),
			zap.String("remote", remote.String()),
			zap.Error(dbErr),
		)
		s.metrics.IMEIRejected.Inc()
		return
	}
	if result == store.CheckBlocked {
		s.log.Warn("UDP device blocked — dropping datagram",
			zap.String("broadcast_id", imei),
			zap.String("remote", remote.String()),
		)
		s.metrics.IMEIRejected.Inc()
		return
	}
	// CheckApproved + CheckAutoCreated (new pending) → allow; broadcasts flow and
	// package-h02 skips signal storage for pending devices until activated.

	// Add to approval cache.
	s.approvedIMEIs.Store(imei, &approvedEntry{approvedAt: now, lastSeen: now})
	s.metrics.IMEIApproved.Inc()
	s.log.Info("H02 UDP device approved", zap.String("imei", imei), zap.String("remote", remote.String()))

	respond := func(data []byte) error {
		_, err := conn.WriteToUDP(data, remote)
		return err
	}
	s.handler.DispatchUDP(ctx, f, respond, s.metrics)
	s.fwd.UpdateOnlineUDP(ctx, imei, now) //nolint:errcheck
}

func (s *UDPServer) pruneLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HeartbeatTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-s.cfg.HeartbeatTimeout * 2)
			pruned := 0
			s.approvedIMEIs.Range(func(key, value any) bool {
				e := value.(*approvedEntry)
				if e.lastSeen.Before(cutoff) {
					s.approvedIMEIs.Delete(key)
					s.log.Debug("pruned stale UDP IMEI", zap.String("imei", key.(string)))
					pruned++
				}
				return true
			})
			if pruned > 0 {
				s.log.Info("UDP IMEI cache pruned", zap.Int("count", pruned))
			}
		}
	}
}

func (s *UDPServer) startHTTP(ctx context.Context) *http.Server {
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
		fmt.Fprintf(w, `{"service":"h02-udp","udp_port":7021,"status":"ok","endpoints":["/healthz","/readyz","/metrics"]}`) //nolint:errcheck
	})

	srv := &http.Server{Addr: s.cfg.UDPHTTPAddr, Handler: mux}
	go func() {
		s.log.Info("H02 UDP HTTP observability listening", zap.String("addr", s.cfg.UDPHTTPAddr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("H02 UDP HTTP server error", zap.Error(err))
		}
	}()
	return srv
}
