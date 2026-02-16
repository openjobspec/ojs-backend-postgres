package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// ---------------------------------------------------------------------------
// WriteJSON tests
// ---------------------------------------------------------------------------

func TestWriteJSON_200_Struct(t *testing.T) {
	rr := httptest.NewRecorder()
	data := struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}{Name: "test", Value: 42}

	WriteJSON(rr, http.StatusOK, data)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if ct := rr.Header().Get("Content-Type"); ct != core.OJSMediaType {
		t.Fatalf("Content-Type = %q, want %q", ct, core.OJSMediaType)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp["name"] != "test" {
		t.Fatalf("name = %v, want test", resp["name"])
	}
}

func TestWriteJSON_201_Map(t *testing.T) {
	rr := httptest.NewRecorder()
	data := map[string]any{"id": "abc", "created": true}

	WriteJSON(rr, http.StatusCreated, data)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusCreated)
	}
	if ct := rr.Header().Get("Content-Type"); ct != core.OJSMediaType {
		t.Fatalf("Content-Type = %q, want %q", ct, core.OJSMediaType)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp["id"] != "abc" {
		t.Fatalf("id = %v, want abc", resp["id"])
	}
}

func TestWriteJSON_200_Slice(t *testing.T) {
	rr := httptest.NewRecorder()
	data := []string{"a", "b", "c"}

	WriteJSON(rr, http.StatusOK, data)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp []string
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(resp) != 3 {
		t.Fatalf("len = %d, want 3", len(resp))
	}
}

// ---------------------------------------------------------------------------
// WriteError tests
// ---------------------------------------------------------------------------

func TestWriteError_400_InvalidRequest(t *testing.T) {
	rr := httptest.NewRecorder()
	ojsErr := core.NewInvalidRequestError("missing required field", nil)

	WriteError(rr, http.StatusBadRequest, ojsErr)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
	if ct := rr.Header().Get("Content-Type"); ct != core.OJSMediaType {
		t.Fatalf("Content-Type = %q, want %q", ct, core.OJSMediaType)
	}

	var resp ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Error.Code != core.ErrCodeInvalidRequest {
		t.Fatalf("code = %q, want %q", resp.Error.Code, core.ErrCodeInvalidRequest)
	}
	if resp.Error.Retryable {
		t.Fatal("invalid_request should not be retryable")
	}
}

func TestWriteError_404_NotFound(t *testing.T) {
	rr := httptest.NewRecorder()
	ojsErr := core.NewNotFoundError("Job", "job-123")

	WriteError(rr, http.StatusNotFound, ojsErr)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}

	var resp ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Error.Code != core.ErrCodeNotFound {
		t.Fatalf("code = %q, want %q", resp.Error.Code, core.ErrCodeNotFound)
	}
	if resp.Error.Retryable {
		t.Fatal("not_found should not be retryable")
	}
}

func TestWriteError_500_InternalWithRetryable(t *testing.T) {
	rr := httptest.NewRecorder()
	ojsErr := core.NewInternalError("database connection lost")

	WriteError(rr, http.StatusInternalServerError, ojsErr)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}

	var resp ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Error.Code != core.ErrCodeInternalError {
		t.Fatalf("code = %q, want %q", resp.Error.Code, core.ErrCodeInternalError)
	}
	if !resp.Error.Retryable {
		t.Fatal("internal_error should be retryable")
	}
}

func TestWriteError_IncludesRequestID(t *testing.T) {
	rr := httptest.NewRecorder()
	rr.Header().Set("X-Request-Id", "req-abc-123")
	ojsErr := core.NewInvalidRequestError("bad input", nil)

	WriteError(rr, http.StatusBadRequest, ojsErr)

	var resp ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Error.RequestID != "req-abc-123" {
		t.Fatalf("request_id = %q, want %q", resp.Error.RequestID, "req-abc-123")
	}
}
