package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// ---------------------------------------------------------------------------
// Job handler tests
// ---------------------------------------------------------------------------

func TestJobCreate_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"email.send","args":["user@example.com"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusCreated)
	}
	if ct := rr.Header().Get("Content-Type"); ct != core.OJSMediaType {
		t.Fatalf("Content-Type = %q, want %q", ct, core.OJSMediaType)
	}
}

func TestJobCreate_MissingType(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"args":["x"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest && rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 400 or 422", rr.Code)
	}
}

func TestJobCreate_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{invalid`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestJobCreate_DuplicateConflict(t *testing.T) {
	backend := &mockBackend{
		pushFunc: func(_ context.Context, _ *core.Job) (*core.Job, error) {
			return nil, &core.OJSError{Code: core.ErrCodeDuplicate, Message: "duplicate job"}
		},
	}
	router := newTestRouter(backend)
	body := `{"type":"email.send","args":["x"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusConflict)
	}
}

func TestJobGet_Found(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/test-id", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if _, ok := resp["job"]; !ok {
		t.Fatal("response missing 'job' key")
	}
}

func TestJobGet_NotFound(t *testing.T) {
	backend := &mockBackend{
		infoFunc: func(_ context.Context, id string) (*core.Job, error) {
			return nil, core.NewNotFoundError("Job", id)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/missing-id", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestJobCancel_NotFound(t *testing.T) {
	backend := &mockBackend{
		cancelFunc: func(_ context.Context, id string) (*core.Job, error) {
			return nil, core.NewNotFoundError("Job", id)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/jobs/missing-id", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestJobCancel_Conflict(t *testing.T) {
	backend := &mockBackend{
		cancelFunc: func(_ context.Context, _ string) (*core.Job, error) {
			return nil, core.NewConflictError("job already completed", nil)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/jobs/completed-id", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusConflict)
	}
}

// ---------------------------------------------------------------------------
// Worker handler tests
// ---------------------------------------------------------------------------

func TestWorkerFetch_MissingQueues(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"worker_id":"w-1"}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkerFetch_Success(t *testing.T) {
	backend := &mockBackend{
		fetchFunc: func(_ context.Context, _ []string, _ int, _ string, _ int) ([]*core.Job, error) {
			return []*core.Job{{ID: "job-1", Type: "test", State: "active", Queue: "default"}}, nil
		},
	}
	router := newTestRouter(backend)
	body := `{"queues":["default"],"worker_id":"w-1"}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if ct := rr.Header().Get("Content-Type"); ct != core.OJSMediaType {
		t.Fatalf("Content-Type = %q, want %q", ct, core.OJSMediaType)
	}
}

func TestWorkerFetch_Empty(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"queues":["default"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]json.RawMessage
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	var jobs []json.RawMessage
	if err := json.Unmarshal(resp["jobs"], &jobs); err != nil {
		t.Fatalf("invalid jobs array: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("jobs count = %d, want 0", len(jobs))
	}
}

func TestWorkerAck_MissingJobID(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"result":{"ok":true}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkerAck_NotFound(t *testing.T) {
	backend := &mockBackend{
		ackFunc: func(_ context.Context, id string, _ []byte) (*core.AckResponse, error) {
			return nil, core.NewNotFoundError("Job", id)
		},
	}
	router := newTestRouter(backend)
	body := `{"job_id":"missing"}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestWorkerAck_Conflict(t *testing.T) {
	backend := &mockBackend{
		ackFunc: func(_ context.Context, _ string, _ []byte) (*core.AckResponse, error) {
			return nil, core.NewConflictError("already acked", nil)
		},
	}
	router := newTestRouter(backend)
	body := `{"job_id":"j-1"}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusConflict)
	}
}

func TestWorkerNack_MissingJobID(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"error":{"message":"boom"}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/nack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkerHeartbeat_MissingWorkerID(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"active_jobs":["j-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/heartbeat", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkerHeartbeat_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"worker_id":"w-1","active_jobs":["j-1"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/heartbeat", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
}

// ---------------------------------------------------------------------------
// System handler tests
// ---------------------------------------------------------------------------

func TestSystemManifest(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/manifest", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp["specversion"] != core.OJSVersion {
		t.Fatalf("specversion = %v, want %s", resp["specversion"], core.OJSVersion)
	}
}

func TestSystemHealth_OK(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/health", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
}

func TestSystemHealth_Degraded(t *testing.T) {
	backend := &mockBackend{
		healthFunc: func(_ context.Context) (*core.HealthResponse, error) {
			return &core.HealthResponse{Status: "degraded", Version: core.OJSVersion}, nil
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/health", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}
}

// ---------------------------------------------------------------------------
// Queue handler tests
// ---------------------------------------------------------------------------

func TestQueueList(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/queues", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := resp["queues"]; !ok {
		t.Fatal("response missing 'queues' key")
	}
}

func TestQueueStats(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/queues/default/stats", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := resp["queue"]; !ok {
		t.Fatal("response missing 'queue' key")
	}
}

// ---------------------------------------------------------------------------
// Dead letter handler tests
// ---------------------------------------------------------------------------

func TestDeadLetterList(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/dead-letter", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := resp["jobs"]; !ok {
		t.Fatal("response missing 'jobs' key")
	}
	if _, ok := resp["pagination"]; !ok {
		t.Fatal("response missing 'pagination' key")
	}
}

// ---------------------------------------------------------------------------
// Cron handler tests
// ---------------------------------------------------------------------------

func TestCronList(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/cron", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := resp["crons"]; !ok {
		t.Fatal("response missing 'crons' key")
	}
}

func TestCronRegister(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"name":"daily-report","expression":"0 0 * * *","job_template":{"type":"report.generate","args":[]}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/cron", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusCreated)
	}
}

// ---------------------------------------------------------------------------
// Workflow handler tests
// ---------------------------------------------------------------------------

func TestWorkflowCreate(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"chain","steps":[{"name":"step1","type":"task.a","args":[]},{"name":"step2","type":"task.b","args":[]}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusCreated)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := resp["workflow"]; !ok {
		t.Fatal("response missing 'workflow' key")
	}
}

// ---------------------------------------------------------------------------
// Batch handler tests
// ---------------------------------------------------------------------------

func TestBatchCreate_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"jobs":[{"type":"email.send","args":["a@b.com"]},{"type":"email.send","args":["c@d.com"]}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusCreated)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, ok := resp["jobs"]; !ok {
		t.Fatal("response missing 'jobs' key")
	}
	count, ok := resp["count"].(float64)
	if !ok || int(count) != 2 {
		t.Fatalf("count = %v, want 2", resp["count"])
	}
}

// ---------------------------------------------------------------------------
// Additional job handler tests (parity with Redis backend)
// ---------------------------------------------------------------------------

func TestJobCreate_ExistingJob(t *testing.T) {
	backend := &mockBackend{
		pushFunc: func(_ context.Context, job *core.Job) (*core.Job, error) {
			job.ID = "existing-id"
			job.State = "available"
			job.IsExisting = true
			return job, nil
		},
	}
	router := newTestRouter(backend)
	body := `{"type":"email.send","args":["hello"]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for existing job, got %d", rr.Code)
	}
}

func TestJobCancel_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/jobs/job-123", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	job, _ := resp["job"].(map[string]any)
	if job["state"] != "cancelled" {
		t.Errorf("state = %v, want cancelled", job["state"])
	}
}

// ---------------------------------------------------------------------------
// Additional worker handler tests (parity with Redis backend)
// ---------------------------------------------------------------------------

func TestWorkerFetch_EmptyQueues(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"queues":[]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkerAck_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"job_id":"job-1","result":{"ok":true}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp["acknowledged"] != true {
		t.Errorf("acknowledged = %v, want true", resp["acknowledged"])
	}
}

func TestWorkerNack_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"job_id":"job-1","error":{"message":"timeout"}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/nack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
}

// ---------------------------------------------------------------------------
// Additional queue handler tests (parity with Redis backend)
// ---------------------------------------------------------------------------

func TestQueuePause_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/queues/default/pause", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	queue, _ := resp["queue"].(map[string]any)
	if queue["paused"] != true {
		t.Errorf("paused = %v, want true", queue["paused"])
	}
}

func TestQueueResume_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/queues/default/resume", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	queue, _ := resp["queue"].(map[string]any)
	if queue["paused"] != false {
		t.Errorf("paused = %v, want false", queue["paused"])
	}
}

// ---------------------------------------------------------------------------
// Additional dead letter handler tests (parity with Redis backend)
// ---------------------------------------------------------------------------

func TestDeadLetterList_LimitCap(t *testing.T) {
	var capturedLimit int
	backend := &mockBackend{
		listDeadLetterFunc: func(_ context.Context, limit, offset int) ([]*core.Job, int, error) {
			capturedLimit = limit
			return []*core.Job{}, 0, nil
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/dead-letter?limit=99999", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if capturedLimit > 1000 {
		t.Errorf("limit should be capped at 1000, got %d", capturedLimit)
	}
}

func TestDeadLetterRetry_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/dead-letter/dead-1/retry", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
}

func TestDeadLetterRetry_NotFound(t *testing.T) {
	backend := &mockBackend{
		retryDeadLetterFunc: func(_ context.Context, jobID string) (*core.Job, error) {
			return nil, core.NewNotFoundError("Job", jobID)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/dead-letter/nonexistent/retry", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestDeadLetterDelete_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/dead-letter/dead-1", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp["deleted"] != true {
		t.Errorf("deleted = %v, want true", resp["deleted"])
	}
}

// ---------------------------------------------------------------------------
// Additional cron handler tests (parity with Redis backend)
// ---------------------------------------------------------------------------

func TestCronList_WithData(t *testing.T) {
	backend := &mockBackend{
		listCronFunc: func(_ context.Context) ([]*core.CronJob, error) {
			return []*core.CronJob{{Name: "daily", Expression: "0 9 * * *", Enabled: true}}, nil
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/cron", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	crons, _ := resp["crons"].([]any)
	if len(crons) != 1 {
		t.Errorf("expected 1 cron, got %d", len(crons))
	}
}

func TestCronRegister_MissingName(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"expression":"0 9 * * *","job_template":{"type":"test"}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/cron", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestCronRegister_MissingExpression(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"name":"daily","job_template":{"type":"test"}}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/cron", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestCronRegister_MissingJobTemplate(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"name":"daily","expression":"0 9 * * *"}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/cron", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestCronDelete_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/cron/daily", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
}

func TestCronDelete_NotFound(t *testing.T) {
	backend := &mockBackend{
		deleteCronFunc: func(_ context.Context, name string) (*core.CronJob, error) {
			return nil, core.NewNotFoundError("Cron job", name)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/cron/nonexistent", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

// ---------------------------------------------------------------------------
// Additional workflow handler tests (parity with Redis backend)
// ---------------------------------------------------------------------------

func TestWorkflowCreate_Group(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"group","jobs":[{"type":"job1","args":[]},{"type":"job2","args":[]}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", rr.Code, http.StatusCreated, rr.Body.String())
	}
}

func TestWorkflowCreate_MissingType(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"steps":[{"type":"step1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkflowCreate_InvalidType(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"invalid","steps":[{"type":"step1"}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkflowCreate_ChainMissingSteps(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"chain","steps":[]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkflowGet_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/workflows/wf-123", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	wf, _ := resp["workflow"].(map[string]any)
	if wf["id"] != "wf-123" {
		t.Errorf("id = %v, want wf-123", wf["id"])
	}
}

func TestWorkflowGet_NotFound(t *testing.T) {
	backend := &mockBackend{
		getWorkflowFunc: func(_ context.Context, id string) (*core.Workflow, error) {
			return nil, core.NewNotFoundError("Workflow", id)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodGet, "/ojs/v1/workflows/nonexistent", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNotFound)
	}
}

func TestWorkflowCancel_Success(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/workflows/wf-123", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	wf, _ := resp["workflow"].(map[string]any)
	if wf["state"] != "cancelled" {
		t.Errorf("state = %v, want cancelled", wf["state"])
	}
}

func TestWorkflowCancel_Conflict(t *testing.T) {
	backend := &mockBackend{
		cancelWorkflowFunc: func(_ context.Context, id string) (*core.Workflow, error) {
			return nil, core.NewConflictError("already completed", nil)
		},
	}
	router := newTestRouter(backend)
	req := httptest.NewRequest(http.MethodDelete, "/ojs/v1/workflows/wf-123", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusConflict)
	}
}

// ---------------------------------------------------------------------------
// Additional batch handler tests (parity with Redis backend)
// ---------------------------------------------------------------------------

func TestBatchCreate_EmptyJobs(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"jobs":[]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestBatchCreate_InvalidJobInBatch(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	body := `{"jobs":[{"type":"ok","args":[]},{"args":[]}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest && rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 400 or 422: %s", rr.Code, rr.Body.String())
	}
}

func TestBatchCreate_BackendError(t *testing.T) {
	backend := &mockBackend{
		pushBatchFunc: func(_ context.Context, jobs []*core.Job) ([]*core.Job, error) {
			return nil, fmt.Errorf("database connection lost")
		},
	}
	router := newTestRouter(backend)
	body := `{"jobs":[{"type":"email.send","args":["a"]}]}`
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
}

// ---------------------------------------------------------------------------
// Invalid JSON body tests (parity with Redis backend)
// ---------------------------------------------------------------------------

func TestWorkerFetch_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkerAck_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkerNack_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/nack", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkerHeartbeat_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/heartbeat", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestCronRegister_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/cron", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestWorkflowCreate_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workflows", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestBatchCreate_InvalidJSON(t *testing.T) {
	router := newTestRouter(&mockBackend{})
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs/batch", strings.NewReader("{bad"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}
