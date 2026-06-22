// Package handler dispatches H02 frames to the correct message handler.
//
// TCP device lifecycle:
//
//  1. TCP connect → StateConnected; auth timer started
//  2. First valid frame → MySQL CheckOrCreate → if approved → StateLoggedIn
//  3. V1/V2 frames → decode → publish location event
//  4. HTBT → ACK + session heartbeat
//  5. Disconnect / timeout → goroutine cleanup
//
// UDP device lifecycle:
//
//  1. Datagram received → IMEI extracted
//  2. IMEI cache miss → MySQL CheckOrCreate
//  3. If approved → process frame → publish to stream
//  4. If rejected → drop datagram
package handler

import (
	"context"
	"h02-server/pkg/protocol"
	"h02-server/server/internal/config"
	"h02-server/server/internal/forwarder"
	"h02-server/server/internal/metrics"
	"h02-server/server/internal/session"
	"h02-server/server/internal/store"
	"time"

	"go.uber.org/zap"
)

// Handler dispatches H02 frames for both TCP and UDP transports.
type Handler struct {
	cfg     *config.Config
	fwd     *forwarder.Stream
	devices *store.DeviceStore // may be nil (DB-less mode)
	reg     *session.Registry  // non-nil for TCP; nil for UDP
	log     *zap.Logger
}

func New(
	cfg *config.Config,
	fwd *forwarder.Stream,
	devices *store.DeviceStore,
	reg *session.Registry,
	log *zap.Logger,
) *Handler {
	return &Handler{cfg: cfg, fwd: fwd, devices: devices, reg: reg, log: log}
}

// DispatchTCP handles an H02 frame received over a TCP connection.
// It manages per-session authentication state and sends responses when required.
func (h *Handler) DispatchTCP(ctx context.Context, sess *session.Session, f *protocol.Frame, m *metrics.TCPMetrics) {
	m.FramesReceived.WithLabelValues(f.Cmd).Inc()

	// First frame from this connection → authenticate the IMEI.
	if sess.State() < session.StateLoggedIn {
		result, err := h.devices.CheckOrCreate(ctx, f.IMEI, "H02")
		if err != nil {
			h.log.Error("device check failed — rejecting TCP",
				zap.String("imei", f.IMEI),
				zap.String("addr", sess.RemoteAddr()),
				zap.Error(err),
			)
			m.LoginFailure.Inc()
			time.AfterFunc(200*time.Millisecond, sess.Close)
			return
		}
		if result == store.CheckBlocked {
			h.log.Warn("device blocked — closing TCP",
				zap.String("broadcast_id", f.IMEI),
				zap.String("addr", sess.RemoteAddr()),
			)
			m.LoginFailure.Inc()
			time.AfterFunc(200*time.Millisecond, sess.Close)
			return
		}
		sess.IMEI = f.IMEI
		sess.SetState(session.StateLoggedIn)
		h.reg.Register(ctx, sess)
		m.LoginSuccess.Inc()
		h.log.Info("H02 TCP device logged in",
			zap.String("imei", sess.IMEI),
			zap.String("addr", sess.RemoteAddr()),
		)
		go h.fwd.PublishEvent(ctx, "device.login", "tcp", sess.IMEI, map[string]any{
			"imei":     sess.IMEI,
			"addr":     sess.RemoteAddr(),
			"login_at": time.Now().UTC().Format(time.RFC3339),
		})
	}

	// For logged-in sessions, ignore any IMEI in the frame that differs from the
	// session IMEI — the IMEI is fixed at login and cannot change mid-connection.
	if sess.State() >= session.StateLoggedIn && f.IMEI != sess.IMEI {
		h.log.Warn("IMEI mismatch — using session IMEI",
			zap.String("session_imei", sess.IMEI),
			zap.String("frame_imei", f.IMEI),
		)
		f.IMEI = sess.IMEI
	}

	switch f.Cmd {
	case protocol.CmdV1, protocol.CmdV2:
		h.handleV1TCP(ctx, sess, f, m)
	case protocol.CmdHTBT:
		h.handleHTBTTCP(ctx, sess, f, m)
	case protocol.CmdNBR:
		h.handleNBRTCP(ctx, sess, f, m)
	case protocol.CmdLINK:
		h.handleLINKTCP(ctx, sess, f, m)
	case protocol.CmdSACK:
		h.handleSACKTCP(ctx, sess, f, m)
	default:
		h.log.Info("unhandled H02 cmd (TCP)",
			zap.String("imei", sess.IMEI),
			zap.String("cmd", f.Cmd),
		)
		m.DecodeErrors.Inc()
	}
}

// DispatchUDP handles an H02 frame received over a stateless UDP connection.
// The IMEI must have been pre-approved by the UDP server before calling this.
func (h *Handler) DispatchUDP(ctx context.Context, f *protocol.Frame, respond func([]byte) error, m *metrics.UDPMetrics) {
	switch f.Cmd {
	case protocol.CmdV1, protocol.CmdV2:
		h.handleV1UDP(ctx, f, m)
	case protocol.CmdHTBT:
		h.handleHTBTUDP(ctx, f, respond, m)
	case protocol.CmdNBR:
		h.handleNBRUDP(ctx, f, m)
	case protocol.CmdLINK:
		h.handleLINKUDP(ctx, f, m)
	default:
		h.log.Debug("unhandled H02 cmd (UDP)",
			zap.String("imei", f.IMEI),
			zap.String("cmd", f.Cmd),
		)
	}
}

// ── TCP handlers ─────────────────────────────────────────────────────────────

func (h *Handler) handleV1TCP(ctx context.Context, sess *session.Session, f *protocol.Frame, m *metrics.TCPMetrics) {
	loc, err := protocol.DecodeV1(f)
	if err != nil {
		h.log.Warn("V1 decode error (TCP)", zap.String("imei", sess.IMEI), zap.Error(err))
		m.DecodeErrors.Inc()
		return
	}
	sess.TouchLocation()
	m.LocationReports.Inc()
	go h.fwd.PublishLocation(ctx, "tcp", loc)
	h.log.Debug("V1 location (TCP)",
		zap.String("imei", sess.IMEI),
		zap.Bool("gps_fixed", loc.GPSFixed),
		zap.Float64("lat", loc.Latitude),
		zap.Float64("lon", loc.Longitude),
	)
}

func (h *Handler) handleHTBTTCP(ctx context.Context, sess *session.Session, f *protocol.Frame, m *metrics.TCPMetrics) {
	h.reg.Heartbeat(ctx, sess)
	resp := protocol.HTBTResponse(sess.IMEI)
	if err := sess.Send(resp, h.cfg.WriteTimeout); err != nil {
		h.log.Debug("HTBT ACK error (TCP)", zap.String("imei", sess.IMEI), zap.Error(err))
	}
	m.Heartbeats.Inc()
	h.devices.RecordHeartbeat(ctx, sess.IMEI)
	h.log.Debug("HTBT heartbeat (TCP)", zap.String("imei", sess.IMEI))
}

func (h *Handler) handleNBRTCP(ctx context.Context, sess *session.Session, f *protocol.Frame, m *metrics.TCPMetrics) {
	loc, err := protocol.DecodeNBR(f)
	if err != nil {
		h.log.Warn("NBR decode error (TCP)", zap.String("imei", sess.IMEI), zap.Error(err))
		m.DecodeErrors.Inc()
		return
	}
	m.LocationReports.Inc()
	go h.fwd.PublishLocation(ctx, "tcp", loc)
}

func (h *Handler) handleLINKTCP(ctx context.Context, sess *session.Session, f *protocol.Frame, m *metrics.TCPMetrics) {
	status, err := protocol.DecodeLINK(f)
	if err != nil {
		h.log.Warn("LINK decode error (TCP)", zap.String("imei", sess.IMEI), zap.Error(err))
		m.DecodeErrors.Inc()
		return
	}
	go h.fwd.PublishStatus(ctx, "tcp", status)
}

func (h *Handler) handleSACKTCP(ctx context.Context, sess *session.Session, f *protocol.Frame, m *metrics.TCPMetrics) {
	// Device requests a server ACK. Serial is in f.Fields[0] if present.
	serial := ""
	if len(f.Fields) > 0 {
		serial = f.Fields[0]
	}
	resp := protocol.SACKResponse(sess.IMEI, serial)
	if err := sess.Send(resp, h.cfg.WriteTimeout); err != nil {
		h.log.Debug("SACK response error (TCP)", zap.String("imei", sess.IMEI), zap.Error(err))
	}
}

// ── UDP handlers ─────────────────────────────────────────────────────────────

func (h *Handler) handleV1UDP(ctx context.Context, f *protocol.Frame, m *metrics.UDPMetrics) {
	loc, err := protocol.DecodeV1(f)
	if err != nil {
		h.log.Warn("V1 decode error (UDP)", zap.String("imei", f.IMEI), zap.Error(err))
		m.DecodeErrors.Inc()
		return
	}
	m.LocationReports.Inc()
	go h.fwd.PublishLocation(ctx, "udp", loc)
	h.log.Debug("V1 location (UDP)",
		zap.String("imei", f.IMEI),
		zap.Bool("gps_fixed", loc.GPSFixed),
		zap.Float64("lat", loc.Latitude),
		zap.Float64("lon", loc.Longitude),
	)
}

func (h *Handler) handleHTBTUDP(ctx context.Context, f *protocol.Frame, respond func([]byte) error, m *metrics.UDPMetrics) {
	m.Heartbeats.Inc()
	h.devices.RecordHeartbeat(ctx, f.IMEI)
	h.log.Debug("HTBT heartbeat (UDP)", zap.String("imei", f.IMEI))
	// Online sorted-set update is done by the UDP server for every datagram, not just HTBT.
	if respond != nil {
		resp := protocol.HTBTResponse(f.IMEI)
		respond(resp) //nolint:errcheck
	}
}

func (h *Handler) handleNBRUDP(ctx context.Context, f *protocol.Frame, m *metrics.UDPMetrics) {
	loc, err := protocol.DecodeNBR(f)
	if err != nil {
		h.log.Warn("NBR decode error (UDP)", zap.String("imei", f.IMEI), zap.Error(err))
		m.DecodeErrors.Inc()
		return
	}
	m.LocationReports.Inc()
	go h.fwd.PublishLocation(ctx, "udp", loc)
}

func (h *Handler) handleLINKUDP(ctx context.Context, f *protocol.Frame, m *metrics.UDPMetrics) {
	status, err := protocol.DecodeLINK(f)
	if err != nil {
		h.log.Warn("LINK decode error (UDP)", zap.String("imei", f.IMEI), zap.Error(err))
		return
	}
	go h.fwd.PublishStatus(ctx, "udp", status)
}
