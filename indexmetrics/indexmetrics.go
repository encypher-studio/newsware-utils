// Package indexmetrics holds the Prometheus metrics specific to services that
// index documents (the watchers). The generic, metric-agnostic registry and
// HTTP serving live in nwmetrics; call nwmetrics.Init(service) once in main,
// then indexmetrics.Register() to register these metrics with the service label.
package indexmetrics

import (
	"github.com/encypher-studio/newsware-utils/nwmetrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	MetricServiceRestarts      *prometheus.CounterVec
	MetricDocumentsIndexed     *prometheus.CounterVec
	MetricLastIndexedTimestamp *prometheus.GaugeVec
)

// The index metrics are constructed at package load so they are never nil for
// library consumers (e.g. the indexer package) that record to them regardless
// of whether Register has run. They are only registered — and thus scraped —
// once Register wires them into the service-labeled registry.
func init() {
	MetricServiceRestarts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "service_restarts",
			Help: "Service restarts",
		},
		[]string{"timestamp"},
	)

	MetricDocumentsIndexed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "documents_indexed_total",
			Help: "Number of documents indexed, by source",
		},
		[]string{"source"},
	)

	MetricLastIndexedTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "last_indexed_timestamp_seconds",
			Help: "Unix timestamp of the last successfully indexed document, by source",
		},
		[]string{"source"},
	)
}

// Register registers the index metrics through the service-labeled registry so
// each carries the "service" label. nwmetrics.Init must have been called first.
// Call it once from main.
func Register() {
	nwmetrics.Register(
		MetricServiceRestarts,
		MetricDocumentsIndexed,
		MetricLastIndexedTimestamp,
	)
}
