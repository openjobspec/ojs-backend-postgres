package postgres

import (
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// scanJob scans a single row from a jobs query into a core.Job.
// The column order must match the SELECT used in queries.
func scanJob(rows pgx.Rows) (*core.Job, error) {
	var (
		id                 string
		jobType            string
		queue              string
		args               []byte
		meta               []byte
		state              string
		priority           int
		attempt            int
		maxAttempts        int
		retryPolicy        []byte
		uniqueKey          *string
		result             []byte
		jobError           []byte
		errors             [][]byte
		scheduledAt        *time.Time
		expiresAt          *time.Time
		createdAt          time.Time
		enqueuedAt         *time.Time
		startedAt          *time.Time
		completedAt        *time.Time
		cancelledAt        *time.Time
		discardedAt        *time.Time
		visibilityTimeout  *time.Time
		workerID           *string
		tags               []string
		timeoutMs          *int
		workflowID         *string
		workflowStep       *int
		parentResults      [][]byte
		retryDelayMs       *int64
		visibilityTimeoutMs *int
		unknownFields      []byte
	)

	err := rows.Scan(
		&id, &jobType, &queue, &args, &meta,
		&state, &priority, &attempt, &maxAttempts, &retryPolicy,
		&uniqueKey, &result, &jobError, &errors,
		&scheduledAt, &expiresAt, &createdAt, &enqueuedAt,
		&startedAt, &completedAt, &cancelledAt, &discardedAt,
		&visibilityTimeout, &workerID, &tags, &timeoutMs,
		&workflowID, &workflowStep, &parentResults, &retryDelayMs,
		&visibilityTimeoutMs, &unknownFields,
	)
	if err != nil {
		return nil, err
	}

	return buildJob(
		id, jobType, queue, args, meta, state, priority, attempt, maxAttempts,
		retryPolicy, uniqueKey, result, jobError, errors,
		scheduledAt, expiresAt, createdAt, enqueuedAt, startedAt,
		completedAt, cancelledAt, discardedAt, visibilityTimeout,
		workerID, tags, timeoutMs, workflowID, workflowStep,
		parentResults, retryDelayMs, visibilityTimeoutMs, unknownFields,
	), nil
}

// scanJobRow scans a single pgx.Row into a core.Job.
func scanJobRow(row pgx.Row) (*core.Job, error) {
	var (
		id                 string
		jobType            string
		queue              string
		args               []byte
		meta               []byte
		state              string
		priority           int
		attempt            int
		maxAttempts        int
		retryPolicy        []byte
		uniqueKey          *string
		result             []byte
		jobError           []byte
		errors             [][]byte
		scheduledAt        *time.Time
		expiresAt          *time.Time
		createdAt          time.Time
		enqueuedAt         *time.Time
		startedAt          *time.Time
		completedAt        *time.Time
		cancelledAt        *time.Time
		discardedAt        *time.Time
		visibilityTimeout  *time.Time
		workerID           *string
		tags               []string
		timeoutMs          *int
		workflowID         *string
		workflowStep       *int
		parentResults      [][]byte
		retryDelayMs       *int64
		visibilityTimeoutMs *int
		unknownFields      []byte
	)

	err := row.Scan(
		&id, &jobType, &queue, &args, &meta,
		&state, &priority, &attempt, &maxAttempts, &retryPolicy,
		&uniqueKey, &result, &jobError, &errors,
		&scheduledAt, &expiresAt, &createdAt, &enqueuedAt,
		&startedAt, &completedAt, &cancelledAt, &discardedAt,
		&visibilityTimeout, &workerID, &tags, &timeoutMs,
		&workflowID, &workflowStep, &parentResults, &retryDelayMs,
		&visibilityTimeoutMs, &unknownFields,
	)
	if err != nil {
		return nil, err
	}

	return buildJob(
		id, jobType, queue, args, meta, state, priority, attempt, maxAttempts,
		retryPolicy, uniqueKey, result, jobError, errors,
		scheduledAt, expiresAt, createdAt, enqueuedAt, startedAt,
		completedAt, cancelledAt, discardedAt, visibilityTimeout,
		workerID, tags, timeoutMs, workflowID, workflowStep,
		parentResults, retryDelayMs, visibilityTimeoutMs, unknownFields,
	), nil
}

func buildJob(
	id, jobType, queue string,
	args, meta []byte,
	state string,
	priority, attempt, maxAttempts int,
	retryPolicy []byte,
	uniqueKey *string,
	result, jobError []byte,
	errors [][]byte,
	scheduledAt, expiresAt *time.Time,
	createdAt time.Time,
	enqueuedAt, startedAt, completedAt, cancelledAt, discardedAt, visibilityTimeout *time.Time,
	workerID *string,
	tags []string,
	timeoutMs *int,
	workflowID *string,
	workflowStep *int,
	parentResults [][]byte,
	retryDelayMs *int64,
	visibilityTimeoutMs *int,
	unknownFields []byte,
) *core.Job {
	job := &core.Job{
		ID:       id,
		Type:     jobType,
		State:    state,
		Queue:    queue,
		Attempt:  attempt,
		Priority: &priority,
	}

	if priority == 0 {
		job.Priority = nil
	}

	job.MaxAttempts = &maxAttempts

	if len(args) > 0 {
		job.Args = json.RawMessage(args)
	}
	if len(meta) > 0 && string(meta) != "{}" {
		job.Meta = json.RawMessage(meta)
	}
	if len(result) > 0 {
		job.Result = json.RawMessage(result)
	}
	if len(jobError) > 0 {
		job.Error = json.RawMessage(jobError)
	}

	// Convert JSONB[] errors to []json.RawMessage
	if len(errors) > 0 {
		job.Errors = make([]json.RawMessage, len(errors))
		for i, e := range errors {
			job.Errors[i] = json.RawMessage(e)
		}
	}

	if len(retryPolicy) > 0 {
		var rp core.RetryPolicy
		if json.Unmarshal(retryPolicy, &rp) == nil {
			job.Retry = &rp
		}
	}

	job.CreatedAt = core.FormatTime(createdAt)

	if enqueuedAt != nil {
		job.EnqueuedAt = core.FormatTime(*enqueuedAt)
	}
	if startedAt != nil {
		job.StartedAt = core.FormatTime(*startedAt)
	}
	if completedAt != nil {
		job.CompletedAt = core.FormatTime(*completedAt)
	}
	if cancelledAt != nil {
		job.CancelledAt = core.FormatTime(*cancelledAt)
	}
	if scheduledAt != nil {
		job.ScheduledAt = core.FormatTime(*scheduledAt)
	}
	if expiresAt != nil {
		job.ExpiresAt = core.FormatTime(*expiresAt)
	}

	if tags != nil {
		job.Tags = tags
	}
	if timeoutMs != nil {
		job.TimeoutMs = timeoutMs
	}
	if retryDelayMs != nil {
		job.RetryDelayMs = retryDelayMs
	}
	if visibilityTimeoutMs != nil {
		job.VisibilityTimeoutMs = visibilityTimeoutMs
	}

	if workflowID != nil {
		job.WorkflowID = *workflowID
	}
	if workflowStep != nil {
		job.WorkflowStep = *workflowStep
	}

	// Convert JSONB[] parent_results to []json.RawMessage
	if len(parentResults) > 0 {
		job.ParentResults = make([]json.RawMessage, len(parentResults))
		for i, pr := range parentResults {
			job.ParentResults[i] = json.RawMessage(pr)
		}
	}

	// Restore unknown fields
	if len(unknownFields) > 0 && string(unknownFields) != "{}" && string(unknownFields) != "null" {
		var uf map[string]json.RawMessage
		if json.Unmarshal(unknownFields, &uf) == nil && len(uf) > 0 {
			job.UnknownFields = uf
		}
	}

	return job
}

// allJobColumns is the SELECT column list matching the scan order.
const allJobColumns = `id, type, queue, args, meta,
	state, priority, attempt, max_attempts, retry_policy,
	unique_key, result, error, errors,
	scheduled_at, expires_at, created_at, enqueued_at,
	started_at, completed_at, cancelled_at, discarded_at,
	visibility_timeout, worker_id, tags, timeout_ms,
	workflow_id, workflow_step, parent_results, retry_delay_ms,
	visibility_timeout_ms, unknown_fields`
