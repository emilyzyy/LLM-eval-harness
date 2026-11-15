// Package metrics defines the gateway's Prometheus metrics.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every collector. Each gateway has its own registry, so tests
// can create as many gateways as they like without name clashes.
type Metrics struct {
	registry *prometheus.Registry

	Requests         *prometheus.CounterVec
	TotalLatency     prometheus.Histogram
	UpstreamLatency  prometheus.Histogram
	ProxyOverhead    prometheus.Histogram
	InFlight         prometheus.Gauge
	RateLimited      *prometheus.CounterVec
	CaptureEnqueued  prometheus.Counter
	CaptureDropped   prometheus.Counter
	CaptureSinkError prometheus.Counter
	KafkaProduceErr  prometheus.Counter
}

// New creates the collectors and registers them.
func New() *Metrics {
	// Request and upstream latency are dominated by the model: milliseconds
	// to a minute or more.
	latencyBuckets := []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120}
	// The gateway's own overhead is far smaller: 50 microseconds to a second.
	overheadBuckets := []float64{.00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, 1}

	m := &Metrics{
		registry: prometheus.NewRegistry(),
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_requests_total",
			Help: "Chat completion requests by HTTP status sent to the client. 499 means the client disconnected first.",
		}, []string{"status"}),
		TotalLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_request_duration_seconds",
			Help:    "Time from receiving a request to writing the last response byte.",
			Buckets: latencyBuckets,
		}),
		UpstreamLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_upstream_duration_seconds",
			Help:    "Time spent waiting on the upstream, including retries and reading the body.",
			Buckets: latencyBuckets,
		}),
		ProxyOverhead: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "gateway_proxy_overhead_seconds",
			Help:    "Request duration minus upstream duration: the time the gateway itself added.",
			Buckets: overheadBuckets,
		}),
		InFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gateway_in_flight_requests",
			Help: "Chat completion requests currently being handled.",
		}),
		RateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gateway_rate_limited_total",
			Help: "Requests rejected with 429 by the gateway's own rate limiter.",
		}, []string{"upstream"}),
		CaptureEnqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_capture_enqueued_total",
			Help: "Capture records accepted into the capture queue.",
		}),
		CaptureDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_capture_dropped_total",
			Help: "Capture records dropped because the capture queue was full.",
		}),
		CaptureSinkError: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_capture_sink_errors_total",
			Help: "Capture records the sink rejected when the background writer handed them over.",
		}),
		KafkaProduceErr: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "gateway_kafka_produce_errors_total",
			Help: "Capture records Kafka did not acknowledge (broker down, timeout, or producer buffer full).",
		}),
	}

	m.registry.MustRegister(
		m.Requests, m.TotalLatency, m.UpstreamLatency, m.ProxyOverhead, m.InFlight,
		m.RateLimited, m.CaptureEnqueued, m.CaptureDropped, m.CaptureSinkError, m.KafkaProduceErr,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// Handler serves the metrics in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// ObserveRequest records one finished request.
func (m *Metrics) ObserveRequest(status int, total time.Duration) {
	m.Requests.WithLabelValues(strconv.Itoa(status)).Inc()
	m.TotalLatency.Observe(total.Seconds())
}

// ObserveUpstream records the timing of a request that reached an upstream.
func (m *Metrics) ObserveUpstream(total, upstream time.Duration) {
	m.UpstreamLatency.Observe(upstream.Seconds())
	m.ProxyOverhead.Observe((total - upstream).Seconds())
}
