package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// mockBackend implements core.Backend for benchmark testing.
type mockBackend struct {
	pushFunc             func(ctx context.Context, job *core.Job) (*core.Job, error)
	fetchFunc            func(ctx context.Context, queues []string, count int, workerID string, visMs int) ([]*core.Job, error)
	ackFunc              func(ctx context.Context, jobID string, result []byte) (*core.AckResponse, error)
	nackFunc             func(ctx context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error)
	infoFunc             func(ctx context.Context, jobID string) (*core.Job, error)
	cancelFunc           func(ctx context.Context, jobID string) (*core.Job, error)
	listQueuesFunc       func(ctx context.Context) ([]core.QueueInfo, error)
	healthFunc           func(ctx context.Context) (*core.HealthResponse, error)
	heartbeatFunc        func(ctx context.Context, workerID string, activeJobs []string, visMs int) (*core.HeartbeatResponse, error)
	listDeadLetterFunc   func(ctx context.Context, limit, offset int) ([]*core.Job, int, error)
	retryDeadLetterFunc  func(ctx context.Context, jobID string) (*core.Job, error)
	deleteDeadLetterFunc func(ctx context.Context, jobID string) error
	registerCronFunc     func(ctx context.Context, cron *core.CronJob) (*core.CronJob, error)
	listCronFunc         func(ctx context.Context) ([]*core.CronJob, error)
	deleteCronFunc       func(ctx context.Context, name string) (*core.CronJob, error)
	createWorkflowFunc   func(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error)
	getWorkflowFunc      func(ctx context.Context, id string) (*core.Workflow, error)
	cancelWorkflowFunc   func(ctx context.Context, id string) (*core.Workflow, error)
	advanceWorkflowFunc  func(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error
	pushBatchFunc        func(ctx context.Context, jobs []*core.Job) ([]*core.Job, error)
	queueStatsFunc       func(ctx context.Context, name string) (*core.QueueStats, error)
	pauseQueueFunc       func(ctx context.Context, name string) error
	resumeQueueFunc      func(ctx context.Context, name string) error
	setWorkerStateFunc   func(ctx context.Context, workerID string, state string) error
}

func (m *mockBackend) Push(ctx context.Context, job *core.Job) (*core.Job, error) {
	if m.pushFunc != nil {
		return m.pushFunc(ctx, job)
	}
	job.ID = "test-id"
	job.State = "available"
	job.CreatedAt = "2025-01-01T00:00:00.000Z"
	job.EnqueuedAt = "2025-01-01T00:00:00.000Z"
	return job, nil
}

func (m *mockBackend) Fetch(ctx context.Context, queues []string, count int, workerID string, visMs int) ([]*core.Job, error) {
	if m.fetchFunc != nil {
		return m.fetchFunc(ctx, queues, count, workerID, visMs)
	}
	return []*core.Job{}, nil
}

func (m *mockBackend) Ack(ctx context.Context, jobID string, result []byte) (*core.AckResponse, error) {
	if m.ackFunc != nil {
		return m.ackFunc(ctx, jobID, result)
	}
	return &core.AckResponse{Acknowledged: true, JobID: jobID, State: "completed"}, nil
}

func (m *mockBackend) Nack(ctx context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error) {
	if m.nackFunc != nil {
		return m.nackFunc(ctx, jobID, jobErr, requeue)
	}
	return &core.NackResponse{JobID: jobID, State: "retryable", Attempt: 1, MaxAttempts: 3}, nil
}

func (m *mockBackend) Info(ctx context.Context, jobID string) (*core.Job, error) {
	if m.infoFunc != nil {
		return m.infoFunc(ctx, jobID)
	}
	return &core.Job{ID: jobID, Type: "test", State: "available", Queue: "default"}, nil
}

func (m *mockBackend) Cancel(ctx context.Context, jobID string) (*core.Job, error) {
	if m.cancelFunc != nil {
		return m.cancelFunc(ctx, jobID)
	}
	return &core.Job{ID: jobID, Type: "test", State: "cancelled", Queue: "default"}, nil
}

func (m *mockBackend) ListQueues(ctx context.Context) ([]core.QueueInfo, error) {
	if m.listQueuesFunc != nil {
		return m.listQueuesFunc(ctx)
	}
	return []core.QueueInfo{{Name: "default", Status: "active"}}, nil
}

func (m *mockBackend) Health(ctx context.Context) (*core.HealthResponse, error) {
	if m.healthFunc != nil {
		return m.healthFunc(ctx)
	}
	return &core.HealthResponse{Status: "ok", Version: "1.0"}, nil
}

func (m *mockBackend) Heartbeat(ctx context.Context, workerID string, activeJobs []string, visMs int) (*core.HeartbeatResponse, error) {
	if m.heartbeatFunc != nil {
		return m.heartbeatFunc(ctx, workerID, activeJobs, visMs)
	}
	return &core.HeartbeatResponse{State: "active", Directive: "continue"}, nil
}

func (m *mockBackend) ListDeadLetter(ctx context.Context, limit, offset int) ([]*core.Job, int, error) {
	if m.listDeadLetterFunc != nil {
		return m.listDeadLetterFunc(ctx, limit, offset)
	}
	return []*core.Job{}, 0, nil
}

func (m *mockBackend) RetryDeadLetter(ctx context.Context, jobID string) (*core.Job, error) {
	if m.retryDeadLetterFunc != nil {
		return m.retryDeadLetterFunc(ctx, jobID)
	}
	return &core.Job{ID: jobID, Type: "test", State: "available", Queue: "default"}, nil
}

func (m *mockBackend) DeleteDeadLetter(ctx context.Context, jobID string) error {
	if m.deleteDeadLetterFunc != nil {
		return m.deleteDeadLetterFunc(ctx, jobID)
	}
	return nil
}

func (m *mockBackend) RegisterCron(ctx context.Context, cron *core.CronJob) (*core.CronJob, error) {
	if m.registerCronFunc != nil {
		return m.registerCronFunc(ctx, cron)
	}
	cron.CreatedAt = "2025-01-01T00:00:00.000Z"
	cron.Enabled = true
	return cron, nil
}

func (m *mockBackend) ListCron(ctx context.Context) ([]*core.CronJob, error) {
	if m.listCronFunc != nil {
		return m.listCronFunc(ctx)
	}
	return []*core.CronJob{}, nil
}

func (m *mockBackend) DeleteCron(ctx context.Context, name string) (*core.CronJob, error) {
	if m.deleteCronFunc != nil {
		return m.deleteCronFunc(ctx, name)
	}
	return &core.CronJob{Name: name}, nil
}

func (m *mockBackend) CreateWorkflow(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error) {
	if m.createWorkflowFunc != nil {
		return m.createWorkflowFunc(ctx, req)
	}
	total := len(req.Steps)
	if req.Type != "chain" {
		total = len(req.Jobs)
	}
	zero := 0
	wf := &core.Workflow{ID: "wf-test", Type: req.Type, State: "running", CreatedAt: "2025-01-01T00:00:00.000Z"}
	if req.Type == "chain" {
		wf.StepsTotal = &total
		wf.StepsCompleted = &zero
	} else {
		wf.JobsTotal = &total
		wf.JobsCompleted = &zero
	}
	return wf, nil
}

func (m *mockBackend) GetWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	if m.getWorkflowFunc != nil {
		return m.getWorkflowFunc(ctx, id)
	}
	total := 3
	completed := 1
	return &core.Workflow{ID: id, Type: "chain", State: "running", StepsTotal: &total, StepsCompleted: &completed}, nil
}

func (m *mockBackend) CancelWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	if m.cancelWorkflowFunc != nil {
		return m.cancelWorkflowFunc(ctx, id)
	}
	return &core.Workflow{ID: id, Type: "chain", State: "cancelled"}, nil
}

func (m *mockBackend) AdvanceWorkflow(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error {
	if m.advanceWorkflowFunc != nil {
		return m.advanceWorkflowFunc(ctx, workflowID, jobID, result, failed)
	}
	return nil
}

func (m *mockBackend) PushBatch(ctx context.Context, jobs []*core.Job) ([]*core.Job, error) {
	if m.pushBatchFunc != nil {
		return m.pushBatchFunc(ctx, jobs)
	}
	for i, j := range jobs {
		j.ID = fmt.Sprintf("batch-%d", i)
		j.State = "available"
		j.CreatedAt = "2025-01-01T00:00:00.000Z"
	}
	return jobs, nil
}

func (m *mockBackend) QueueStats(ctx context.Context, name string) (*core.QueueStats, error) {
	if m.queueStatsFunc != nil {
		return m.queueStatsFunc(ctx, name)
	}
	return &core.QueueStats{Queue: name, Status: "active", Stats: core.Stats{Available: 5, Active: 2}}, nil
}

func (m *mockBackend) PauseQueue(ctx context.Context, name string) error {
	if m.pauseQueueFunc != nil {
		return m.pauseQueueFunc(ctx, name)
	}
	return nil
}

func (m *mockBackend) ResumeQueue(ctx context.Context, name string) error {
	if m.resumeQueueFunc != nil {
		return m.resumeQueueFunc(ctx, name)
	}
	return nil
}

func (m *mockBackend) SetWorkerState(ctx context.Context, workerID string, state string) error {
	if m.setWorkerStateFunc != nil {
		return m.setWorkerStateFunc(ctx, workerID, state)
	}
	return nil
}

func (m *mockBackend) Subscribe(queues []string) (<-chan string, func()) {
	ch := make(chan string)
	return ch, func() { close(ch) }
}

func (m *mockBackend) Close() error { return nil }

func (m *mockBackend) ListJobs(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error) {
	return nil, 0, nil
}

func (m *mockBackend) ListWorkers(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error) {
	return nil, core.WorkerSummary{}, nil
}

// newTestRouter creates a chi router wired to the given mock backend.
func newTestRouter(backend *mockBackend) *chi.Mux {
	r := chi.NewRouter()

	jobHandler := NewJobHandler(backend)
	workerHandler := NewWorkerHandler(backend)
	systemHandler := NewSystemHandler(backend)
	queueHandler := NewQueueHandler(backend)
	deadLetterHandler := NewDeadLetterHandler(backend)
	cronHandler := NewCronHandler(backend)
	workflowHandler := NewWorkflowHandler(backend)
	batchHandler := NewBatchHandler(backend)

	r.Get("/ojs/manifest", systemHandler.Manifest)
	r.Get("/ojs/v1/health", systemHandler.Health)
	r.Post("/ojs/v1/jobs", jobHandler.Create)
	r.Get("/ojs/v1/jobs/{id}", jobHandler.Get)
	r.Delete("/ojs/v1/jobs/{id}", jobHandler.Cancel)
	r.Post("/ojs/v1/jobs/batch", batchHandler.Create)
	r.Post("/ojs/v1/workers/fetch", workerHandler.Fetch)
	r.Post("/ojs/v1/workers/ack", workerHandler.Ack)
	r.Post("/ojs/v1/workers/nack", workerHandler.Nack)
	r.Post("/ojs/v1/workers/heartbeat", workerHandler.Heartbeat)
	r.Get("/ojs/v1/queues", queueHandler.List)
	r.Get("/ojs/v1/queues/{name}/stats", queueHandler.Stats)
	r.Post("/ojs/v1/queues/{name}/pause", queueHandler.Pause)
	r.Post("/ojs/v1/queues/{name}/resume", queueHandler.Resume)
	r.Get("/ojs/v1/dead-letter", deadLetterHandler.List)
	r.Post("/ojs/v1/dead-letter/{id}/retry", deadLetterHandler.Retry)
	r.Delete("/ojs/v1/dead-letter/{id}", deadLetterHandler.Delete)
	r.Get("/ojs/v1/cron", cronHandler.List)
	r.Post("/ojs/v1/cron", cronHandler.Register)
	r.Delete("/ojs/v1/cron/{name}", cronHandler.Delete)
	r.Post("/ojs/v1/workflows", workflowHandler.Create)
	r.Get("/ojs/v1/workflows/{id}", workflowHandler.Get)
	r.Delete("/ojs/v1/workflows/{id}", workflowHandler.Cancel)

	return r
}

// BenchmarkJobCreate benchmarks the job creation endpoint (POST /ojs/v1/jobs).
func BenchmarkJobCreate(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"type":"email.send","args":["user@example.com"],"options":{"queue":"default"}}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkJobGet benchmarks the job retrieval endpoint (GET /ojs/v1/jobs/{id}).
func BenchmarkJobGet(b *testing.B) {
	router := newTestRouter(&mockBackend{})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/test-id", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkWorkerFetch benchmarks the worker fetch endpoint (POST /ojs/v1/workers/fetch).
func BenchmarkWorkerFetch(b *testing.B) {
	backend := &mockBackend{
		fetchFunc: func(ctx context.Context, queues []string, count int, workerID string, visMs int) ([]*core.Job, error) {
			return []*core.Job{{ID: "job-1", Type: "test", State: "active", Queue: "default"}}, nil
		},
	}
	router := newTestRouter(backend)
	body := `{"queues":["default"],"worker_id":"w-1"}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

// BenchmarkWorkerAck benchmarks the worker ack endpoint (POST /ojs/v1/workers/ack).
func BenchmarkWorkerAck(b *testing.B) {
	router := newTestRouter(&mockBackend{})
	body := `{"job_id":"job-1","result":{"ok":true}}`

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/ack", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
	}
}

