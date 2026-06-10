package session

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// State tracks the H02 TCP device connection lifecycle.
// H02 has no dedicated login packet — the IMEI arrives in every frame.
// The first frame from an approved IMEI moves the session from StateConnected
// to StateLoggedIn.
type State int32

const (
	StateConnected State = iota
	StateLoggedIn
	StateClosing
)

// Session represents one active TCP connection from an H02 device.
type Session struct {
	conn    net.Conn
	writeMu sync.Mutex

	IMEI string

	state atomic.Int32

	ConnectedAt   time.Time
	LastHeartbeat atomic.Int64
	LastLocation  atomic.Int64
}

func NewSession(conn net.Conn) *Session {
	s := &Session{
		conn:        conn,
		ConnectedAt: time.Now(),
	}
	s.state.Store(int32(StateConnected))
	s.LastHeartbeat.Store(time.Now().UnixNano())
	return s
}

func (s *Session) State() State {
	return State(s.state.Load())
}

func (s *Session) SetState(st State) {
	s.state.Store(int32(st))
}

// Send writes raw bytes to the TCP connection with a write deadline.
func (s *Session) Send(data []byte, deadline time.Duration) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.conn.SetWriteDeadline(time.Now().Add(deadline)) //nolint:errcheck
	_, err := s.conn.Write(data)
	return err
}

func (s *Session) Touch() {
	s.LastHeartbeat.Store(time.Now().UnixNano())
}

func (s *Session) TouchLocation() {
	s.LastLocation.Store(time.Now().UnixNano())
}

func (s *Session) IsStale(ttl time.Duration) bool {
	last := time.Unix(0, s.LastHeartbeat.Load())
	return time.Since(last) > ttl
}

func (s *Session) RemoteAddr() string {
	return s.conn.RemoteAddr().String()
}

func (s *Session) SetReadDeadline(d time.Time) error {
	return s.conn.SetReadDeadline(d)
}

func (s *Session) Close() {
	s.state.Store(int32(StateClosing))
	s.conn.Close() //nolint:errcheck
}
