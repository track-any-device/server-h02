// Package metrics exposes Prometheus instrumentation for the H02 server.
//
// TCP scrape endpoint: GET /metrics  (on H02_TCP_HTTP_ADDR, default :9092)
// UDP scrape endpoint: GET /metrics  (on H02_UDP_HTTP_ADDR, default :9093)
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TCPMetrics groups all TCP binary Prometheus instruments.
type TCPMetrics struct {
	ConnectionsTotal  prometheus.Counter
	ConnectionsActive prometheus.Gauge

	FramesReceived *prometheus.CounterVec

	LoginSuccess prometheus.Counter
	LoginFailure prometheus.Counter

	Heartbeats      prometheus.Counter
	LocationReports prometheus.Counter
	DecodeErrors    prometheus.Counter

	StreamPublishDuration prometheus.Histogram
}

func NewTCP() *TCPMetrics {
	return &TCPMetrics{
		ConnectionsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_tcp_connections_total",
			Help: "Total TCP connections accepted since startup.",
		}),
		ConnectionsActive: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "h02_tcp_connections_active",
			Help: "Currently connected H02 TCP devices.",
		}),
		FramesReceived: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "h02_tcp_frames_received_total",
			Help: "Total H02 TCP frames received, by CMD type.",
		}, []string{"cmd"}),
		LoginSuccess: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_tcp_login_success_total",
			Help: "First-frame IMEI approved and session registered.",
		}),
		LoginFailure: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_tcp_login_failure_total",
			Help: "First-frame IMEI rejected (not approved or DB error).",
		}),
		Heartbeats: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_tcp_heartbeats_total",
			Help: "Total HTBT frames received over TCP.",
		}),
		LocationReports: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_tcp_location_reports_total",
			Help: "V1/V2 location frames published to Redis Stream over TCP.",
		}),
		DecodeErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_tcp_decode_errors_total",
			Help: "Frames dropped due to parse error or missing '#' terminator (TCP).",
		}),
		StreamPublishDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "h02_tcp_stream_publish_seconds",
			Help:    "Redis Stream XADD latency in seconds (TCP path).",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
		}),
	}
}
