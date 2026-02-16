//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

func setupTestBackend(t *testing.T) *Backend {
	t.Helper()
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("ojs_test"),
		postgres.WithUsername("ojs"),
		postgres.WithPassword("ojs"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	backend, err := New(connStr)
	if err != nil {
		t.Fatalf("failed to create backend: %v", err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	return backend
}

func TestIntegration_PushAndInfo(t *testing.T) {
	backend := setupTestBackend(t)
	ctx := context.Background()

	job := &core.Job{
		Type:  "email.send",
		Queue: "default",
		Args:  json.RawMessage(`["hello@example.com"]`),
	}

	created, err := backend.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	if created.ID == "" {
		t.Fatal("expected non-empty job ID")
	}
	if created.State != core.StateAvailable {
		t.Errorf("state = %q, want %q", created.State, core.StateAvailable)
	}
	if created.Type != "email.send" {
		t.Errorf("type = %q, want %q", created.Type, "email.send")
	}

	info, err := backend.Info(ctx, created.ID)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.ID != created.ID {
		t.Errorf("info.ID = %q, want %q", info.ID, created.ID)
	}
}

func TestIntegration_FetchAndAck(t *testing.T) {
	backend := setupTestBackend(t)
	ctx := context.Background()

	// Push a job
	job := &core.Job{
		Type:  "pdf.generate",
		Queue: "workers",
		Args:  json.RawMessage(`[1, 2, 3]`),
	}
	created, err := backend.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Fetch it
	fetched, err := backend.Fetch(ctx, []string{"workers"}, 1, "worker-1", 30000)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(fetched) != 1 {
		t.Fatalf("expected 1 fetched job, got %d", len(fetched))
	}
	if fetched[0].ID != created.ID {
		t.Errorf("fetched ID = %q, want %q", fetched[0].ID, created.ID)
	}
	if fetched[0].State != core.StateActive {
		t.Errorf("state = %q, want %q", fetched[0].State, core.StateActive)
	}

	// Ack it
	ackResp, err := backend.Ack(ctx, created.ID, json.RawMessage(`{"status":"done"}`))
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if !ackResp.Acknowledged {
		t.Error("expected acknowledged=true")
	}
	if ackResp.State != core.StateCompleted {
		t.Errorf("state = %q, want %q", ackResp.State, core.StateCompleted)
	}

	// Verify completed state
	info, err := backend.Info(ctx, created.ID)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if info.State != core.StateCompleted {
		t.Errorf("state = %q, want %q", info.State, core.StateCompleted)
	}
}

func TestIntegration_FetchAndNack(t *testing.T) {
	backend := setupTestBackend(t)
	ctx := context.Background()

	maxAttempts := 2
	job := &core.Job{
		Type:        "flaky.task",
		Queue:       "default",
		Args:        json.RawMessage(`[]`),
		MaxAttempts: &maxAttempts,
	}
	_, err := backend.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Fetch and nack (retry)
	fetched, err := backend.Fetch(ctx, []string{"default"}, 1, "worker-1", 30000)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(fetched) != 1 {
		t.Fatalf("expected 1 job, got %d", len(fetched))
	}

	nackResp, err := backend.Nack(ctx, fetched[0].ID, &core.JobError{
		Message: "connection timeout",
	}, false)
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if nackResp.State != core.StateRetryable {
		t.Errorf("state = %q, want %q", nackResp.State, core.StateRetryable)
	}
}

func TestIntegration_Cancel(t *testing.T) {
	backend := setupTestBackend(t)
	ctx := context.Background()

	job := &core.Job{
		Type:  "cancellable.task",
		Queue: "default",
		Args:  json.RawMessage(`[]`),
	}
	created, err := backend.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	cancelled, err := backend.Cancel(ctx, created.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if cancelled.State != "cancelled" {
		t.Errorf("state = %q, want cancelled", cancelled.State)
	}

	// Cancelling again should fail (terminal state)
	_, err = backend.Cancel(ctx, created.ID)
	if err == nil {
		t.Error("expected error on double cancel")
	}
}

func TestIntegration_PushBatch(t *testing.T) {
	backend := setupTestBackend(t)
	ctx := context.Background()

	jobs := []*core.Job{
		{Type: "batch.1", Queue: "batch-q", Args: json.RawMessage(`[1]`)},
		{Type: "batch.2", Queue: "batch-q", Args: json.RawMessage(`[2]`)},
		{Type: "batch.3", Queue: "batch-q", Args: json.RawMessage(`[3]`)},
	}

	created, err := backend.PushBatch(ctx, jobs)
	if err != nil {
		t.Fatalf("PushBatch: %v", err)
	}
	if len(created) != 3 {
		t.Fatalf("expected 3 created jobs, got %d", len(created))
	}

	// Fetch all 3
	fetched, err := backend.Fetch(ctx, []string{"batch-q"}, 10, "worker-1", 30000)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(fetched) != 3 {
		t.Errorf("expected 3 fetched jobs, got %d", len(fetched))
	}
}

func TestIntegration_UniqueJob_Reject(t *testing.T) {
	backend := setupTestBackend(t)
	ctx := context.Background()

	job := &core.Job{
		Type:  "unique.task",
		Queue: "default",
		Args:  json.RawMessage(`["arg1"]`),
		Unique: &core.UniquePolicy{
			OnConflict: "reject",
		},
	}

	_, err := backend.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push first: %v", err)
	}

	// Push duplicate - should be rejected
	dup := &core.Job{
		Type:  "unique.task",
		Queue: "default",
		Args:  json.RawMessage(`["arg1"]`),
		Unique: &core.UniquePolicy{
			OnConflict: "reject",
		},
	}
	_, err = backend.Push(ctx, dup)
	if err == nil {
		t.Fatal("expected error for duplicate unique job")
	}

	ojsErr, ok := err.(*core.OJSError)
	if !ok {
		t.Fatalf("expected OJSError, got %T", err)
	}
	if ojsErr.Code != core.ErrCodeDuplicate {
		t.Errorf("error code = %q, want %q", ojsErr.Code, core.ErrCodeDuplicate)
	}
}

func TestIntegration_QueuePauseResume(t *testing.T) {
	backend := setupTestBackend(t)
	ctx := context.Background()

	// Push a job to create the queue
	job := &core.Job{
		Type:  "pause.test",
		Queue: "pausable",
		Args:  json.RawMessage(`[]`),
	}
	_, err := backend.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Pause queue
	if err := backend.PauseQueue(ctx, "pausable"); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}

	// Fetch should return 0 jobs from paused queue
	fetched, err := backend.Fetch(ctx, []string{"pausable"}, 10, "worker-1", 30000)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(fetched) != 0 {
		t.Errorf("expected 0 jobs from paused queue, got %d", len(fetched))
	}

	// Resume queue
	if err := backend.ResumeQueue(ctx, "pausable"); err != nil {
		t.Fatalf("ResumeQueue: %v", err)
	}

	// Fetch should now return the job
	fetched, err = backend.Fetch(ctx, []string{"pausable"}, 10, "worker-1", 30000)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(fetched) != 1 {
		t.Errorf("expected 1 job after resume, got %d", len(fetched))
	}
}

func TestIntegration_Health(t *testing.T) {
	backend := setupTestBackend(t)
	ctx := context.Background()

	health, err := backend.Health(ctx)
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if health.Status != "ok" {
		t.Errorf("status = %q, want ok", health.Status)
	}
	if health.Backend.Type != "postgres" {
		t.Errorf("backend type = %q, want postgres", health.Backend.Type)
	}
	if health.Backend.Status != "connected" {
		t.Errorf("backend status = %q, want connected", health.Backend.Status)
	}
}

func TestIntegration_Subscribe(t *testing.T) {
	backend := setupTestBackend(t)
	ctx := context.Background()

	events, cancel := backend.Subscribe([]string{"notify-q"})
	defer cancel()

	// Push a job to trigger notification
	job := &core.Job{
		Type:  "notify.test",
		Queue: "notify-q",
		Args:  json.RawMessage(`[]`),
	}
	_, err := backend.Push(ctx, job)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Should receive notification within a reasonable time
	select {
	case queue := <-events:
		if queue != "notify-q" {
			t.Errorf("got notification for queue %q, want notify-q", queue)
		}
	case <-time.After(5 * time.Second):
		t.Error("timed out waiting for notification")
	}
}
