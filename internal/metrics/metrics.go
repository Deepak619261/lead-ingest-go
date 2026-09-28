// Package metrics exposes Prometheus metrics on /metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds every Prometheus collector.
type Metrics struct {
	reg *prometheus.Registry

	Rows               *prometheus.CounterVec // outcome = inserted|updated|skipped_existing|invalid|malformed|duplicate|failed
	ValidationFailures *prometheus.CounterVec // code
	SinkLatency        *prometheus.HistogramVec
	LookupLatency      prometheus.Histogram
	BatchSize          prometheus.Histogram
	Retries            *prometheus.CounterVec // reason
	ActiveWorkers      prometheus.Gauge
	QueueDepth         *prometheus.GaugeVec // queue = rows|results|shards
	ImportDuration     prometheus.Histogram
	ImportsInFlight    prometheus.Gauge
}

// New registers all collectors plus Go runtime and process metrics (GC, heap, CPU).
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { reg.MustRegister(c) }

	m := &Metrics{
		reg: reg,
		Rows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_rows_total", Help: "Rows by final outcome.",
		}, []string{"outcome"}),
		ValidationFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_validation_failures_total", Help: "Invalid rows by reason code.",
		}, []string{"code"}),
		SinkLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "ingest_sink_write_seconds", Help: "Sink batch write latency per attempt.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 12), // 5ms .. ~10s
		}, []string{"result"}),
		LookupLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "ingest_lookup_seconds", Help: "Single-row lookup latency (legacy mode).",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 12),
		}),
		BatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "ingest_batch_size", Help: "Leads per written batch.",
			Buckets: []float64{1, 10, 50, 100, 250, 500, 1000, 2000, 5000},
		}),
		Retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ingest_retries_total", Help: "Sink retries by reason.",
		}, []string{"reason"}),
		ActiveWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ingest_active_workers", Help: "Workers currently processing a row.",
		}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ingest_queue_depth", Help: "Items waiting in each channel (sampled).",
		}, []string{"queue"}),
		ImportDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "ingest_import_duration_seconds", Help: "Wall time per import.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 12), // 1s .. ~34min
		}),
		ImportsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ingest_imports_in_flight", Help: "Imports currently running.",
		}),
	}
	for _, c := range []prometheus.Collector{m.Rows, m.ValidationFailures, m.SinkLatency, m.LookupLatency,
		m.BatchSize, m.Retries, m.ActiveWorkers, m.QueueDepth, m.ImportDuration, m.ImportsInFlight} {
		f(c)
	}
	return m
}

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
