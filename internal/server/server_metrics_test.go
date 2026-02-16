package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/openjobspec/ojs-backend-postgres/internal/metrics"
)

func TestMetricsMiddleware_UsesRoutePatternForLabels(t *testing.T) {
	router := chi.NewRouter()
	router.Use(metricsMiddleware)
	router.Get("/ojs/v1/jobs/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	patternCounter := metrics.HTTPRequestsTotal.WithLabelValues(http.MethodGet, "/ojs/v1/jobs/{id}", "204")
	rawPathCounter := metrics.HTTPRequestsTotal.WithLabelValues(http.MethodGet, "/ojs/v1/jobs/123", "204")

	patternBefore := counterValue(t, patternCounter)
	rawPathBefore := counterValue(t, rawPathCounter)

	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/123", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}

	patternAfter := counterValue(t, patternCounter)
	rawPathAfter := counterValue(t, rawPathCounter)

	if got, want := patternAfter-patternBefore, float64(1); got != want {
		t.Fatalf("pattern-labeled counter increment = %.0f, want %.0f", got, want)
	}
	if got := rawPathAfter - rawPathBefore; got != 0 {
		t.Fatalf("raw-path-labeled counter increment = %.0f, want 0", got)
	}
}

func counterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()

	m := &dto.Metric{}
	if err := counter.Write(m); err != nil {
		t.Fatalf("write counter metric: %v", err)
	}
	if m.Counter == nil {
		t.Fatal("counter metric is nil")
	}

	return m.Counter.GetValue()
}
