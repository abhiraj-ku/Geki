package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ActiveClients = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "active_client_connections_total",
		Help: "The total no of connected clients waiting for or executing queries ",
	})

	QueriesRouted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "queries_routed_total",
		Help: " Totoal number of queries routed by the proxy",
	}, []string{"type"}) // "type" will be read or write
)
