package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ojsotel "github.com/openjobspec/ojs-go-backend-common/otel"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
	"github.com/openjobspec/ojs-backend-postgres/internal/metrics"
)

// Ack acknowledges a job as completed.
func (b *Backend) Ack(ctx context.Context, jobID string, result []byte) (*core.AckResponse, error) {
	ctx, span := ojsotel.StartJobSpan(ctx, "ack", jobID, "", "")
	defer span.End()

	now := time.Now()

	var currentState, queue, jobType string
	err := b.pool.QueryRow(ctx, "SELECT state, queue, type FROM ojs_jobs WHERE id = $1", jobID).Scan(&currentState, &queue, &jobType)
	if err != nil {
		return nil, core.NewNotFoundError("Job", jobID)
	}

	if currentState != core.StateActive {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot acknowledge job not in 'active' state. Current state: '%s'.", currentState),
			map[string]any{
				"job_id":         jobID,
				"current_state":  currentState,
				"expected_state": "active",
			},
		)
	}

	var resultJSON []byte
	if len(result) > 0 {
		resultJSON = result
	}

	_, err = b.pool.Exec(ctx, `
		UPDATE ojs_jobs SET
			state = 'completed',
			completed_at = $1,
			result = $2,
			error = NULL,
			visibility_timeout = NULL,
			worker_id = NULL
		WHERE id = $3`,
		now, resultJSON, jobID)
	if err != nil {
		return nil, fmt.Errorf("ack job: %w", err)
	}

	// Increment queue completed count
	b.logExec(ctx, "queue-stats",
		"UPDATE ojs_queues SET completed_count = completed_count + 1 WHERE name = $1", queue)

	metrics.JobsCompleted.WithLabelValues(queue, jobType).Inc()
	metrics.ActiveJobs.WithLabelValues(queue).Dec()

	// Advance workflow if applicable
	b.advanceWorkflowForJob(ctx, jobID, core.StateCompleted, result)

	job, _ := b.Info(ctx, jobID)

	return &core.AckResponse{
		Acknowledged: true,
		JobID:        jobID,
		State:        core.StateCompleted,
		CompletedAt:  core.FormatTime(now),
		Job:          job,
	}, nil
}

// Nack reports a job failure.
func (b *Backend) Nack(ctx context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error) {
	ctx, span := ojsotel.StartJobSpan(ctx, "nack", jobID, "", "")
	defer span.End()

	now := time.Now()

	var currentState, queue, jobType string
	var attempt, maxAttempts int
	var retryPolicyJSON []byte
	var priority int

	err := b.pool.QueryRow(ctx,
		"SELECT state, queue, type, attempt, max_attempts, retry_policy, priority FROM ojs_jobs WHERE id = $1",
		jobID).Scan(&currentState, &queue, &jobType, &attempt, &maxAttempts, &retryPolicyJSON, &priority)
	if err != nil {
		return nil, core.NewNotFoundError("Job", jobID)
	}

	if currentState != core.StateActive {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot fail job not in 'active' state. Current state: '%s'.", currentState),
			map[string]any{
				"job_id":         jobID,
				"current_state":  currentState,
				"expected_state": "active",
			},
		)
	}

	metrics.JobsFailed.WithLabelValues(queue, jobType).Inc()
	metrics.ActiveJobs.WithLabelValues(queue).Dec()

	// Handle requeue
	if requeue {
		_, err = b.pool.Exec(ctx, `
			UPDATE ojs_jobs SET
				state = 'available',
				started_at = NULL,
				worker_id = NULL,
				visibility_timeout = NULL,
				enqueued_at = $1
			WHERE id = $2`, now, jobID)
		if err != nil {
			return nil, fmt.Errorf("requeue job: %w", err)
		}

		notifyJobAvailable(ctx, b.pool, queue)

		job, _ := b.Info(ctx, jobID)
		return &core.NackResponse{
			JobID:       jobID,
			State:       core.StateAvailable,
			Attempt:     attempt,
			MaxAttempts: maxAttempts,
			Job:         job,
		}, nil
	}

	newAttempt := attempt + 1

	// Build error JSON
	var errJSON []byte
	if jobErr != nil {
		errObj := map[string]any{
			"message": jobErr.Message,
			"attempt": attempt,
		}
		if jobErr.Code != "" {
			errObj["type"] = jobErr.Code
		}
		if jobErr.Type != "" {
			errObj["type"] = jobErr.Type
		}
		if jobErr.Retryable != nil {
			errObj["retryable"] = *jobErr.Retryable
		}
		if jobErr.Details != nil {
			errObj["details"] = jobErr.Details
		}
		errJSON, _ = json.Marshal(errObj)
	}

	// Check if error is non-retryable
	isNonRetryable := false
	if jobErr != nil && jobErr.Retryable != nil && !*jobErr.Retryable {
		isNonRetryable = true
	}

	var retryPolicy *core.RetryPolicy
	if len(retryPolicyJSON) > 0 {
		var rp core.RetryPolicy
		if json.Unmarshal(retryPolicyJSON, &rp) == nil {
			retryPolicy = &rp
		}
	}

	if !isNonRetryable && jobErr != nil && retryPolicy != nil {
		for _, pattern := range retryPolicy.NonRetryableErrors {
			errType := jobErr.Code
			if jobErr.Type != "" {
				errType = jobErr.Type
			}
			if matchesPattern(errType, pattern) || matchesPattern(jobErr.Message, pattern) {
				isNonRetryable = true
				break
			}
		}
	}

	onExhaustion := "discard"
	if retryPolicy != nil && retryPolicy.OnExhaustion != "" {
		onExhaustion = retryPolicy.OnExhaustion
	}

	// Exhausted or non-retryable => discard
	if isNonRetryable || newAttempt >= maxAttempts {
		discardedAt := now

		_, err = b.pool.Exec(ctx, `
			UPDATE ojs_jobs SET
				state = 'discarded',
				completed_at = $1,
				attempt = $2,
				error = $3,
				errors = array_append(errors, $3::jsonb),
				visibility_timeout = NULL,
				worker_id = NULL,
				started_at = NULL
			WHERE id = $4`,
			discardedAt, newAttempt, errJSON, jobID)
		if err != nil {
			return nil, fmt.Errorf("discard job: %w", err)
		}

		// If on_exhaustion is dead_letter, the job stays with state=discarded
		// and is queryable via ListDeadLetter (which queries state='discarded')
		_ = onExhaustion // dead_letter vs discard both result in 'discarded' state

		metrics.JobsDiscarded.Inc()

		b.advanceWorkflowForJob(ctx, jobID, core.StateDiscarded, nil)

		job, _ := b.Info(ctx, jobID)
		return &core.NackResponse{
			JobID:       jobID,
			State:       core.StateDiscarded,
			Attempt:     newAttempt,
			MaxAttempts: maxAttempts,
			DiscardedAt: core.FormatTime(discardedAt),
			Job:         job,
		}, nil
	}

	// Retry with backoff
	backoff := core.CalculateBackoff(retryPolicy, newAttempt)
	backoffMs := backoff.Milliseconds()
	nextAttemptAt := now.Add(backoff)

	_, err = b.pool.Exec(ctx, `
		UPDATE ojs_jobs SET
			state = 'retryable',
			attempt = $1,
			error = $2,
			errors = array_append(errors, $2::jsonb),
			retry_delay_ms = $3,
			scheduled_at = $4,
			visibility_timeout = NULL,
			worker_id = NULL,
			started_at = NULL
		WHERE id = $5`,
		newAttempt, errJSON, backoffMs, nextAttemptAt, jobID)
	if err != nil {
		return nil, fmt.Errorf("retry job: %w", err)
	}

	job, _ := b.Info(ctx, jobID)
	return &core.NackResponse{
		JobID:         jobID,
		State:         core.StateRetryable,
		Attempt:       newAttempt,
		MaxAttempts:   maxAttempts,
		NextAttemptAt: core.FormatTime(nextAttemptAt),
		Job:           job,
	}, nil
}

// advanceWorkflowForJob checks if a job belongs to a workflow and advances it.
func (b *Backend) advanceWorkflowForJob(ctx context.Context, jobID, state string, result []byte) {
	var workflowID *string
	err := b.pool.QueryRow(ctx, "SELECT workflow_id FROM ojs_jobs WHERE id = $1", jobID).Scan(&workflowID)
	if err != nil || workflowID == nil {
		return
	}

	failed := state == core.StateDiscarded || state == core.StateCancelled
	_ = b.AdvanceWorkflow(ctx, *workflowID, jobID, json.RawMessage(result), failed)
}
