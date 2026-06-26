package indexmetrics

import (
	"fmt"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// DefaultPort is the standard port the metrics endpoint is served on across all
// watchers. It can be overridden with the METRICS_PORT environment variable.
const DefaultPort = "8080"

var (
	MetricServiceRestarts      *prometheus.CounterVec
	MetricDocumentsIndexed     *prometheus.CounterVec
	MetricLastIndexedTimestamp *prometheus.GaugeVec
)

func init() {
	defaultRegistry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = defaultRegistry
	prometheus.DefaultGatherer = defaultRegistry

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

	prometheus.MustRegister(
		MetricServiceRestarts,
		MetricDocumentsIndexed,
		MetricLastIndexedTimestamp,
	)
}

type zerologPromhttpLogger struct{ log zerolog.Logger }

func (l zerologPromhttpLogger) Println(v ...interface{}) {
	l.log.Error().Msg(fmt.Sprint(v...))
}

// Handle returns an http.Handler that serves the Prometheus metrics.
func Handle(log zerolog.Logger) http.Handler {
	return promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{ErrorLog: zerologPromhttpLogger{log}})
}

// Serve serves the Prometheus metrics endpoint at /metrics. It listens on the
// port from the METRICS_PORT environment variable, defaulting to DefaultPort.
// It blocks and retries on error, so callers should run it in a goroutine:
//
//	go indexmetrics.Serve(logger)
func Serve(log zerolog.Logger) {
	port := os.Getenv("METRICS_PORT")
	if port == "" {
		port = DefaultPort
	}

	for {
		mux := http.NewServeMux()
		mux.Handle("/metrics", Handle(log))

		log.Info().Str("port", port).Msg("serving metrics")

		if err := http.ListenAndServe(":"+port, mux); err != nil {
			log.Error().Err(err).Msg("failed to serve metrics")
		}
	}
}
