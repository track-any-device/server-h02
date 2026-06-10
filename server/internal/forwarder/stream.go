// Package forwarder publishes decoded H02 telemetry to a Redis Stream.
//
// Laravel integration:
//
//	H02 Go server (TCP or UDP)
//	  └── XADD h02:telemetry * event location transport tcp imei lat lon speed …
//	                │
//	                ▼ Redis Stream
//	                │
//	  Laravel Queue Worker  (package-h02: h02:consume)
//	    └── SignalService::record()
package forwarder

import (
	"context"
	"encoding/json"
	"h02-server/pkg/protocol"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Stream publishes telemetry to a Redis Stream.
type Stream struct {
	rdb        *redis.Client
	streamKey  string
	maxLen     int64
	onlineZKey string
	log        *zap.Logger
}

func NewStream(rdb *redis.Client, streamKey string, maxLen int64, onlineZKey string, log *zap.Logger) *Stream {
	return &Stream{rdb: rdb, streamKey: streamKey, maxLen: maxLen, onlineZKey: onlineZKey, log: log}
}

// PublishLocation publishes a decoded GPS location to the stream.
// The transport field ("tcp" or "udp") distinguishes which binary published the event.
func (s *Stream) PublishLocation(ctx context.Context, transport string, loc *protocol.LocationReport) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	values := map[string]any{
		"event":        "location",
		"transport":    transport,
		"imei":         loc.IMEI,
		"timestamp":    loc.Timestamp.Unix(),
		"speed":        loc.Speed,
		"course":       loc.Course,
		"gps_fixed":    boolInt(loc.GPSFixed),
		"acc_on":       boolInt(loc.ACCOn),
		"alarm_type":   loc.AlarmType,
		"raw_flags":    loc.RawFlags,
		"published_at": time.Now().UnixMilli(),
	}

	if loc.GPSFixed {
		values["latitude"] = loc.Latitude
		values["longitude"] = loc.Longitude
	}

	args := &redis.XAddArgs{
		Stream: s.streamKey,
		MaxLen: s.maxLen,
		Approx: true,
		Values: values,
	}

	if id, err := s.rdb.XAdd(ctx, args).Result(); err != nil {
		s.log.Warn("stream publish failed", zap.String("imei", loc.IMEI), zap.Error(err))
	} else {
		s.log.Info("location published",
			zap.String("imei", loc.IMEI),
			zap.String("transport", transport),
			zap.String("stream_id", id),
			zap.Bool("gps_fixed", loc.GPSFixed),
			zap.Float64("lat", loc.Latitude),
			zap.Float64("lon", loc.Longitude),
			zap.Float64("speed_kmh", loc.Speed),
		)
	}
}

// PublishStatus publishes a device status update (GSM signal, satellite count).
func (s *Stream) PublishStatus(ctx context.Context, transport string, status *protocol.StatusReport) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	args := &redis.XAddArgs{
		Stream: s.streamKey,
		MaxLen: s.maxLen,
		Approx: true,
		Values: map[string]any{
			"event":        "status",
			"transport":    transport,
			"imei":         status.IMEI,
			"gsm_signal":   status.GSMSignal,
			"satellites":   status.Satellites,
			"published_at": time.Now().UnixMilli(),
		},
	}

	if _, err := s.rdb.XAdd(ctx, args).Result(); err != nil {
		s.log.Warn("status publish failed", zap.String("imei", status.IMEI), zap.Error(err))
	}
}

// PublishEvent publishes a generic lifecycle event (device.login, device.created, etc.).
func (s *Stream) PublishEvent(ctx context.Context, event, transport, imei string, payload map[string]any) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	payloadJSON, _ := json.Marshal(payload)
	args := &redis.XAddArgs{
		Stream: s.streamKey,
		MaxLen: s.maxLen,
		Approx: true,
		Values: map[string]any{
			"event":        event,
			"transport":    transport,
			"imei":         imei,
			"payload":      string(payloadJSON),
			"published_at": time.Now().UnixMilli(),
		},
	}

	if _, err := s.rdb.XAdd(ctx, args).Result(); err != nil {
		s.log.Warn("event publish failed",
			zap.String("event", event),
			zap.String("imei", imei),
			zap.Error(err),
		)
	}
}

// UpdateOnlineUDP updates the h02:online sorted set for a UDP device.
// Called on each datagram so the online-detection logic in package-h02 sees
// the device as active even without a persistent TCP session.
func (s *Stream) UpdateOnlineUDP(ctx context.Context, imei string, t time.Time) error {
	return s.rdb.ZAdd(ctx, s.onlineZKey, redis.Z{
		Score:  float64(t.UnixNano()),
		Member: imei,
	}).Err()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
