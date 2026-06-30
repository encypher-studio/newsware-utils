// Package nwmetrics provides a centralized way to expose Prometheus metrics
// that always carry a constant "service" label. Init is called once from main
// with the service name; every collector registered afterwards through Register
// inherits that label. It is metric-agnostic — packages with their own metrics
// (e.g. indexmetrics) build their collectors with the standard prometheus
// constructors and register them here.
package nwmetrics

import (
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// DefaultPort is the standard port the metrics endpoint is served on. It can be
// overridden with the METRICS_PORT environment variable.
const DefaultPort = "8080"

// registerer is the service-labeled registerer set up by Init. Collectors
// registered through it inherit the constant "service" label.
var registerer prometheus.Registerer

// Init wires Prometheus so that every metric registered through Register carries
// a constant "service" label. It must be called once from main, before any
// metric is registered. service must be non-empty.
func Init(service string) error {
	if service == "" {
		return errors.New("nwmetrics: service must not be empty")
	}

	// Gathering reads from the unwrapped registry; registration goes through the
	// wrapper so collectors pick up the service label.
	registry := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = registry
	prometheus.DefaultGatherer = registry
	registerer = prometheus.WrapRegistererWith(prometheus.Labels{"service": service}, registry)
	return nil
}

// Register adds collectors to the service-labeled registry. Build the metric
// with the standard prometheus.New*Vec constructors, then register it here so
// it inherits the service label. Init must have been called first.
func Register(cs ...prometheus.Collector) {
	if registerer == nil {
		panic("nwmetrics: Register called before Init")
	}
	registerer.MustRegister(cs...)
}

type zerologPromhttpLogger struct{ log zerolog.Logger }

func (l zerologPromhttpLogger) Println(v ...any) {
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
//	go nwmetrics.Serve(logger)
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
