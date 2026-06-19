package indexmetrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

var (
	MetricServiceRestarts  *prometheus.CounterVec
	MetricDocumentsIndexed *prometheus.CounterVec
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
			Name: "documents_indexed",
			Help: "Number of documents indexed",
		},
		[]string{},
	)

	err := prometheus.Register(MetricServiceRestarts)
	if err != nil {
		panic(err)
	}
	err = prometheus.Register(MetricDocumentsIndexed)
	if err != nil {
		panic(err)
	}
}

type zerologPromhttpLogger struct{ log zerolog.Logger }

func (l zerologPromhttpLogger) Println(v ...interface{}) {
	l.log.Error().Msg(fmt.Sprint(v...))
}

func Handle(log zerolog.Logger) http.Handler {
	return promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{ErrorLog: zerologPromhttpLogger{log}})
}
