package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

func TestRequestToJob_Defaults(t *testing.T) {
	req := &core.EnqueueRequest{
		Type: "email.send",
		Args: json.RawMessage(`["user@example.com"]`),
		UnknownFields: map[string]json.RawMessage{
			"custom": json.RawMessage(`{"k":"v"}`),
		},
	}

	job := requestToJob(req)

	if job.Type != req.Type {
		t.Fatalf("Type = %q, want %q", job.Type, req.Type)
	}
	if job.Queue != "default" {
		t.Fatalf("Queue = %q, want default", job.Queue)
	}
	if job.MaxAttempts == nil || *job.MaxAttempts != core.DefaultRetryPolicy().MaxAttempts {
		t.Fatal("default retry max attempts was not applied")
	}
	if _, ok := job.UnknownFields["custom"]; !ok {
		t.Fatal("unknown fields were not preserved")
	}
}

func TestRequestToJob_OptionsAndUnknownErrorHandling(t *testing.T) {
	priority := 10
	timeoutMs := 1500
	visMs := 9000
	retry := &core.RetryPolicy{MaxAttempts: 7}
	unknown := map[string]json.RawMessage{
		"error": json.RawMessage(`{"message":"boom"}`),
		"x":     json.RawMessage(`1`),
	}

	req := &core.EnqueueRequest{
		ID:            "job-123",
		Type:          "report.generate",
		Args:          json.RawMessage(`[42]`),
		Meta:          json.RawMessage(`{"original":true}`),
		UnknownFields: unknown,
		Options: &core.EnqueueOptions{
			Queue:               "reports",
			Priority:            &priority,
			TimeoutMs:           &timeoutMs,
			Tags:                []string{"nightly"},
			Retry:               retry,
			Unique:              &core.UniquePolicy{OnConflict: "reject"},
			ScheduledAt:         "+PT1S",
			ExpiresAt:           "+PT1M",
			RateLimit:           &core.RateLimitPolicy{MaxPerSecond: 3},
			Metadata:            json.RawMessage(`{"override":true}`),
			VisibilityTimeoutMs: &visMs,
		},
	}

	job := requestToJob(req)

	if job.ID != "job-123" {
		t.Fatalf("ID = %q, want job-123", job.ID)
	}
	if job.Queue != "reports" {
		t.Fatalf("Queue = %q, want reports", job.Queue)
	}
	if job.Priority == nil || *job.Priority != priority {
		t.Fatal("priority not mapped")
	}
	if job.TimeoutMs == nil || *job.TimeoutMs != timeoutMs {
		t.Fatal("timeout not mapped")
	}
	if job.Retry == nil || job.Retry.MaxAttempts != retry.MaxAttempts {
		t.Fatal("retry policy not mapped")
	}
	if job.MaxAttempts == nil || *job.MaxAttempts != retry.MaxAttempts {
		t.Fatal("max attempts not derived from retry policy")
	}
	if job.Unique == nil || job.Unique.OnConflict != "reject" {
		t.Fatal("unique policy not mapped")
	}
	if _, err := time.Parse(time.RFC3339, job.ScheduledAt); err != nil {
		t.Fatalf("scheduled_at is not RFC3339: %q", job.ScheduledAt)
	}
	if _, err := time.Parse(time.RFC3339, job.ExpiresAt); err != nil {
		t.Fatalf("expires_at is not RFC3339: %q", job.ExpiresAt)
	}
	if string(job.Meta) != `{"override":true}` {
		t.Fatalf("meta = %s, want options.metadata override", string(job.Meta))
	}
	if job.VisibilityTimeoutMs == nil || *job.VisibilityTimeoutMs != visMs {
		t.Fatal("visibility_timeout_ms not mapped")
	}
	if len(job.Error) == 0 {
		t.Fatal("error field was not moved from unknown fields")
	}
	if _, ok := job.UnknownFields["error"]; ok {
		t.Fatal("error field should be removed from unknown fields")
	}
	if _, ok := job.UnknownFields["x"]; !ok {
		t.Fatal("non-error unknown fields should be preserved")
	}
}

func TestRequestToJob_VisibilityTimeoutISO8601(t *testing.T) {
	req := &core.EnqueueRequest{
		Type: "task",
		Args: json.RawMessage(`[]`),
		Options: &core.EnqueueOptions{
			VisibilityTimeout: "PT5S",
		},
	}

	job := requestToJob(req)
	if job.VisibilityTimeoutMs == nil || *job.VisibilityTimeoutMs != 5000 {
		t.Fatalf("visibility_timeout_ms = %v, want 5000", job.VisibilityTimeoutMs)
	}
}

func TestResolveRelativeTime(t *testing.T) {
	absolute := "2026-01-01T00:00:00Z"
	if got := resolveRelativeTime(absolute); got != absolute {
		t.Fatalf("absolute time changed: got %q want %q", got, absolute)
	}

	start := time.Now()
	got := resolveRelativeTime("+PT2S")
	parsed, err := time.Parse(time.RFC3339, got)
	if err != nil {
		t.Fatalf("relative time did not resolve to RFC3339: %q", got)
	}
	end := time.Now()
	minExpected := start.Add(1 * time.Second)
	maxExpected := end.Add(3 * time.Second)
	if parsed.Before(minExpected) || parsed.After(maxExpected) {
		t.Fatalf("resolved time %s outside expected range [%s, %s]", parsed, minExpected, maxExpected)
	}

	if gotInvalid := resolveRelativeTime("+PTbad"); gotInvalid != "+PTbad" {
		t.Fatalf("invalid relative duration should be returned unchanged, got %q", gotInvalid)
	}
}

func TestDecodeBody(t *testing.T) {
	t.Run("valid json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"a":1}`))
		var payload map[string]int
		if err := decodeBody(req, &payload); err != nil {
			t.Fatalf("decodeBody returned error: %v", err)
		}
		if payload["a"] != 1 {
			t.Fatalf("payload[a] = %d, want 1", payload["a"])
		}
	})

	t.Run("invalid json", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"a":`))
		var payload map[string]int
		if err := decodeBody(req, &payload); err == nil {
			t.Fatal("expected decodeBody error for invalid JSON")
		}
	})
}

func TestOJSHeaders(t *testing.T) {
	handler := OJSHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	t.Run("echoes request id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Request-Id", "req-123")
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Header().Get("X-Request-Id") != "req-123" {
			t.Fatalf("X-Request-Id = %q, want req-123", rr.Header().Get("X-Request-Id"))
		}
		if rr.Header().Get("OJS-Version") != core.OJSVersion {
			t.Fatalf("OJS-Version = %q, want %q", rr.Header().Get("OJS-Version"), core.OJSVersion)
		}
		if rr.Header().Get("Content-Type") != core.OJSMediaType {
			t.Fatalf("Content-Type = %q, want %q", rr.Header().Get("Content-Type"), core.OJSMediaType)
		}
	})

	t.Run("generates request id when missing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Header().Get("X-Request-Id") == "" {
			t.Fatal("expected generated X-Request-Id")
		}
	})
}

func TestValidateContentType(t *testing.T) {
	baseHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := ValidateContentType(baseHandler)

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("application/json with params should pass, got %d", rr.Code)
	}

	reqBad := httptest.NewRequest(http.MethodPost, "/", nil)
	reqBad.Header.Set("Content-Type", "text/plain")
	rrBad := httptest.NewRecorder()
	handler.ServeHTTP(rrBad, reqBad)
	if rrBad.Code != http.StatusBadRequest {
		t.Fatalf("invalid content-type should fail, got %d", rrBad.Code)
	}

	reqGet := httptest.NewRequest(http.MethodGet, "/", nil)
	reqGet.Header.Set("Content-Type", "text/plain")
	rrGet := httptest.NewRecorder()
	handler.ServeHTTP(rrGet, reqGet)
	if rrGet.Code != http.StatusNoContent {
		t.Fatalf("GET should not enforce content-type, got %d", rrGet.Code)
	}
}

func TestLimitRequestBody(t *testing.T) {
	handler := LimitRequestBody(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	tooLarge := strings.Repeat("a", MaxBodySize+1)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tooLarge))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected %d for oversized body, got %d", http.StatusRequestEntityTooLarge, rr.Code)
	}
}

