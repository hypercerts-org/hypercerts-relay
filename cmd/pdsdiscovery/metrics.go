package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	candidates = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pdsdiscovery_candidates_total",
		Help: "Observed retained-collection records by PDS discovery outcome.",
	}, []string{"outcome"})
	quotas = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pdsdiscovery_account_limit_checks_total",
		Help: "Account limit checks for PDSes added by this process, by outcome.",
	}, []string{"outcome"})
	upstreamReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pdsdiscovery_upstream_reconnects_total",
		Help: "Upstream Jetstream disconnections followed by a reconnect.",
	})
	pollFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pdsdiscovery_admin_poll_failures_total",
		Help: "Failed administration discovery state polls.",
	})
	lastPoll = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pdsdiscovery_admin_last_poll_success_timestamp_seconds",
		Help: "Unix time of the last successful administration discovery state poll.",
	})
)
