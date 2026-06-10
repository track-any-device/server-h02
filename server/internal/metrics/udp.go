package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// UDPMetrics groups all UDP binary Prometheus instruments.
type UDPMetrics struct {
	DatagramsTotal  prometheus.Counter
	LocationReports prometheus.Counter
	Heartbeats      prometheus.Counter
	IMEIApproved    prometheus.Counter
	IMEIRejected    prometheus.Counter
	DecodeErrors    prometheus.Counter

	StreamPublishDuration prometheus.Histogram
}

func NewUDP() *UDPMetrics {
	return &UDPMetrics{
		DatagramsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_udp_datagrams_total",
			Help: "Total UDP datagrams received.",
		}),
		LocationReports: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_udp_location_reports_total",
			Help: "V1/V2 location frames published to Redis Stream over UDP.",
		}),
		Heartbeats: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_udp_heartbeats_total",
			Help: "Total HTBT datagrams received over UDP.",
		}),
		IMEIApproved: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_udp_imei_approved_total",
			Help: "New IMEIs approved on first seen (UDP).",
		}),
		IMEIRejected: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_udp_imei_rejected_total",
			Help: "New IMEIs rejected on first seen (UDP).",
		}),
		DecodeErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "h02_udp_decode_errors_total",
			Help: "Datagrams dropped due to parse error or missing '#' terminator (UDP).",
		}),
		StreamPublishDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "h02_udp_stream_publish_seconds",
			Help:    "Redis Stream XADD latency in seconds (UDP path).",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
		}),
	}
}
