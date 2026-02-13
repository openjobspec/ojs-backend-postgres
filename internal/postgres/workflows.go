package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// CreateWorkflow creates and starts a workflow.
func (b *Backend) CreateWorkflow(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error) {
	now := time.Now()
	wfID := core.NewUUIDv7()

	jobs := req.Jobs
	if req.Type == "chain" {
		jobs = req.Steps
	}
	total := len(jobs)

	wf := &core.Workflow{
		ID:        wfID,
		Name:      req.Name,
		Type:      req.Type,
		State:     "running",
		CreatedAt: core.FormatTime(now),
	}

	if req.Type == "chain" {
		wf.StepsTotal = &total
		zero := 0
		wf.StepsCompleted = &zero
	} else {
		wf.JobsTotal = &total
		zero := 0
		wf.JobsCompleted = &zero
	}

	jobDefsJSON, _ := json.Marshal(jobs)
	var callbacksJSON []byte
	if req.Callbacks != nil {
		callbacksJSON, _ = json.Marshal(req.Callbacks)
	}

	// Insert the workflow row
	_, err := b.pool.Exec(ctx, `
		INSERT INTO ojs_workflows (id, name, type, state, total, completed, failed, job_defs, callbacks, created_at)
		VALUES ($1, $2, $3, $4, $5, 0, 0, $6, $7, $8)`,
		wfID, req.Name, req.Type, "running", total, jobDefsJSON, callbacksJSON, now)
	if err != nil {
		return nil, fmt.Errorf("insert workflow: %w", err)
	}

	// Push jobs and track their IDs.
	// If any push fails, clean up the workflow to avoid orphans.
	pushAndTrack := func(job *core.Job) error {
		created, err := b.Push(ctx, job)
		if err != nil {
			return err
		}
		if _, err := b.pool.Exec(ctx,
			"UPDATE ojs_workflows SET job_ids = array_append(job_ids, $1) WHERE id = $2",
			created.ID, wfID); err != nil {
			slog.Error("workflow: error tracking job", "job_id", created.ID, "workflow_id", wfID, "error", err)
		}
		return nil
	}

	var pushErr error
	if req.Type == "chain" {
		// Chain: only enqueue the first step
		pushErr = pushAndTrack(b.workflowStepToJob(jobs[0], wfID, 0))
	} else {
		// Group/Batch: enqueue all jobs
		for i, step := range jobs {
			if err := pushAndTrack(b.workflowStepToJob(step, wfID, i)); err != nil {
				pushErr = err
				break
			}
		}
	}

	if pushErr != nil {
		// Clean up: mark workflow as failed since we couldn't create all jobs
		b.logExec(ctx, "workflow-cleanup",
			"UPDATE ojs_workflows SET state = 'failed', completed_at = $1 WHERE id = $2",
			time.Now(), wfID)
		return nil, pushErr
	}

	return wf, nil
}

func (b *Backend) workflowStepToJob(step core.WorkflowJobRequest, wfID string, stepIdx int) *core.Job {
	queue := "default"
	if step.Options != nil && step.Options.Queue != "" {
		queue = step.Options.Queue
	}

	job := &core.Job{
		Type:         step.Type,
		Args:         step.Args,
		Queue:        queue,
		WorkflowID:   wfID,
		WorkflowStep: stepIdx,
	}
	if step.Options != nil && step.Options.RetryPolicy != nil {
		job.Retry = step.Options.RetryPolicy
		job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
	} else if step.Options != nil && step.Options.Retry != nil {
		job.Retry = step.Options.Retry
		job.MaxAttempts = &step.Options.Retry.MaxAttempts
	}
	return job
}

// GetWorkflow retrieves a workflow by ID.
func (b *Backend) GetWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	var (
		wfID, wfType, state string
		name                *string
		total, completed    int
		createdAt           time.Time
		completedAt         *time.Time
	)

	err := b.pool.QueryRow(ctx,
		"SELECT id, name, type, state, total, completed, created_at, completed_at FROM ojs_workflows WHERE id = $1",
		id).Scan(&wfID, &name, &wfType, &state, &total, &completed, &createdAt, &completedAt)
	if err != nil {
		return nil, core.NewNotFoundError("Workflow", id)
	}

	wf := &core.Workflow{
		ID:        wfID,
		Type:      wfType,
		State:     state,
		CreatedAt: core.FormatTime(createdAt),
	}
	if name != nil {
		wf.Name = *name
	}
	if completedAt != nil {
		wf.CompletedAt = core.FormatTime(*completedAt)
	}

	if wfType == "chain" {
		wf.StepsTotal = &total
		wf.StepsCompleted = &completed
	} else {
		wf.JobsTotal = &total
		wf.JobsCompleted = &completed
	}

	return wf, nil
}

// CancelWorkflow cancels a workflow and its active/pending jobs.
func (b *Backend) CancelWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
	wf, err := b.GetWorkflow(ctx, id)
	if err != nil {
		return nil, err
	}

	if wf.State == "completed" || wf.State == "failed" || wf.State == "cancelled" {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot cancel workflow in state '%s'.", wf.State),
			nil,
		)
	}

	// Cancel all non-terminal jobs in this workflow
	var jobIDs []string
	rows, err := b.pool.Query(ctx, "SELECT id FROM ojs_jobs WHERE workflow_id = $1", id)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var jobID string
			if err := rows.Scan(&jobID); err != nil {
				continue
			}
			jobIDs = append(jobIDs, jobID)
		}
	}

	for _, jobID := range jobIDs {
		var jobState string
		if b.pool.QueryRow(ctx, "SELECT state FROM ojs_jobs WHERE id = $1", jobID).Scan(&jobState) == nil {
			if !core.IsTerminalState(jobState) {
				_, _ = b.Cancel(ctx, jobID)
			}
		}
	}

	now := time.Now()
	wf.State = "cancelled"
	wf.CompletedAt = core.FormatTime(now)

	b.logExec(ctx, "workflow-cancel",
		"UPDATE ojs_workflows SET state = 'cancelled', completed_at = $1 WHERE id = $2",
		now, id)

	return wf, nil
}

// AdvanceWorkflow is called after ACK or NACK to update workflow state.
func (b *Backend) AdvanceWorkflow(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error {
	var wfType, state string
	var total, completed, failedCount int
	var jobDefsJSON, callbacksJSON []byte

	err := b.pool.QueryRow(ctx,
		"SELECT type, state, total, completed, failed, job_defs, callbacks FROM ojs_workflows WHERE id = $1 FOR UPDATE",
		workflowID).Scan(&wfType, &state, &total, &completed, &failedCount, &jobDefsJSON, &callbacksJSON)
	if err != nil {
		return nil
	}

	if state != "running" {
		return nil
	}

	// Get the job's workflow step
	var stepIdx int
	_ = b.pool.QueryRow(ctx, "SELECT workflow_step FROM ojs_jobs WHERE id = $1", jobID).Scan(&stepIdx)

	// Store result
	if len(result) > 0 {
		b.logExec(ctx, "workflow-results", `
			UPDATE ojs_workflows SET results = jsonb_set(COALESCE(results, '{}'::jsonb), ARRAY[$1], $2::jsonb)
			WHERE id = $3`,
			strconv.Itoa(stepIdx), string(result), workflowID)
	}

	if failed {
		failedCount++
	} else {
		completed++
	}
	totalFinished := completed + failedCount

	if wfType == "chain" {
		if failed {
			b.logExec(ctx, "workflow-state",
				"UPDATE ojs_workflows SET state = 'failed', completed = $1, failed = $2, completed_at = $3 WHERE id = $4",
				completed, failedCount, time.Now(), workflowID)
			return nil
		}

		if totalFinished >= total {
			b.logExec(ctx, "workflow-state",
				"UPDATE ojs_workflows SET state = 'completed', completed = $1, failed = $2, completed_at = $3 WHERE id = $4",
				completed, failedCount, time.Now(), workflowID)
			return nil
		}

		// Update counters and enqueue next step
		b.logExec(ctx, "workflow-state",
			"UPDATE ojs_workflows SET completed = $1, failed = $2 WHERE id = $3",
			completed, failedCount, workflowID)
		return b.enqueueChainStep(ctx, workflowID, jobDefsJSON, stepIdx+1)
	}

	// Group/Batch
	b.logExec(ctx, "workflow-state",
		"UPDATE ojs_workflows SET completed = $1, failed = $2 WHERE id = $3",
		completed, failedCount, workflowID)

	if totalFinished >= total {
		finalState := "completed"
		if failedCount > 0 {
			finalState = "failed"
		}
		b.logExec(ctx, "workflow-state",
			"UPDATE ojs_workflows SET state = $1, completed_at = $2 WHERE id = $3",
			finalState, time.Now(), workflowID)

		if wfType == "batch" && len(callbacksJSON) > 0 {
			b.fireBatchCallbacks(ctx, callbacksJSON, failedCount > 0)
		}
	}

	return nil
}

func (b *Backend) enqueueChainStep(ctx context.Context, workflowID string, jobDefsJSON []byte, stepIdx int) error {
	var jobDefs []core.WorkflowJobRequest
	if err := json.Unmarshal(jobDefsJSON, &jobDefs); err != nil {
		return err
	}

	if stepIdx >= len(jobDefs) {
		return nil
	}

	step := jobDefs[stepIdx]

	// Collect parent results
	var resultsJSON []byte
	if err := b.pool.QueryRow(ctx, "SELECT results FROM ojs_workflows WHERE id = $1", workflowID).Scan(&resultsJSON); err != nil {
		slog.Error("workflow: error fetching parent results", "workflow_id", workflowID, "error", err)
	}

	var parentResults []json.RawMessage
	if len(resultsJSON) > 0 {
		var resultsMap map[string]json.RawMessage
		if json.Unmarshal(resultsJSON, &resultsMap) == nil {
			for i := 0; i < stepIdx; i++ {
				if r, ok := resultsMap[strconv.Itoa(i)]; ok {
					parentResults = append(parentResults, r)
				}
			}
		}
	}

	job := b.workflowStepToJob(step, workflowID, stepIdx)
	job.ParentResults = parentResults

	created, err := b.Push(ctx, job)
	if err != nil {
		return err
	}

	if _, err := b.pool.Exec(ctx, "UPDATE ojs_workflows SET job_ids = array_append(job_ids, $1) WHERE id = $2",
		created.ID, workflowID); err != nil {
		slog.Error("workflow: error tracking job", "job_id", created.ID, "workflow_id", workflowID, "error", err)
	}
	return nil
}

func (b *Backend) fireBatchCallbacks(ctx context.Context, callbacksJSON []byte, hasFailure bool) {
	var callbacks core.WorkflowCallbacks
	if err := json.Unmarshal(callbacksJSON, &callbacks); err != nil {
		return
	}

	if callbacks.OnComplete != nil {
		b.fireCallback(ctx, callbacks.OnComplete)
	}
	if !hasFailure && callbacks.OnSuccess != nil {
		b.fireCallback(ctx, callbacks.OnSuccess)
	}
	if hasFailure && callbacks.OnFailure != nil {
		b.fireCallback(ctx, callbacks.OnFailure)
	}
}

func (b *Backend) fireCallback(ctx context.Context, cb *core.WorkflowCallback) {
	queue := "default"
	if cb.Options != nil && cb.Options.Queue != "" {
		queue = cb.Options.Queue
	}
	_, _ = b.Push(ctx, &core.Job{
		Type:  cb.Type,
		Args:  cb.Args,
		Queue: queue,
	})
}
