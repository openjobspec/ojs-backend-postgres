// Package metrics provides Prometheus instrumentation for the OJS server.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsEnqueued counts total jobs enqueued.
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_enqueued_total",
		Help:      "Total number of jobs enqueued.",
	}, []string{"queue", "type"})

	// JobsFetched counts total jobs fetched by workers.
	JobsFetched = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_fetched_total",
		Help:      "Total number of jobs fetched.",
	}, []string{"queue"})

	// JobsCompleted counts total jobs acknowledged (completed).
	JobsCompleted = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_completed_total",
		Help:      "Total number of jobs completed.",
	})

	// JobsFailed counts total jobs that failed (nacked).
	JobsFailed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_failed_total",
		Help:      "Total number of jobs failed.",
	})

	// JobsDiscarded counts total jobs discarded (exhausted retries).
	JobsDiscarded = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "jobs_discarded_total",
		Help:      "Total number of jobs discarded.",
	})

	// FetchDuration tracks job fetch latency.
	FetchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ojs",
		Name:      "fetch_duration_seconds",
		Help:      "Duration of fetch operations in seconds.",
		Buckets:   prometheus.DefBuckets,
	})

	// ActiveJobs tracks currently active (in-flight) jobs per queue.
	ActiveJobs = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ojs",
		Name:      "active_jobs",
		Help:      "Number of currently active jobs.",
	}, []string{"queue"})

	// HTTPRequestsTotal counts HTTP requests by method, path, and status code.
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ojs",
		Name:      "http_requests_total",
		Help:      "Total number of HTTP requests.",
	}, []string{"method", "path", "status"})

	// HTTPRequestDuration tracks HTTP request latency.
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ojs",
		Name:      "http_request_duration_seconds",
		Help:      "Duration of HTTP requests in seconds.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"method", "path"})
)
