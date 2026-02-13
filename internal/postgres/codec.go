package postgres

import (
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// jobRow holds the raw scanned values from a database row.
// Field order must match allJobColumns.
type jobRow struct {
	ID                  string
	Type                string
	Queue               string
	Args                []byte
	Meta                []byte
	State               string
	Priority            int
	Attempt             int
	MaxAttempts         int
	RetryPolicy         []byte
	UniqueKey           *string
	Result              []byte
	Error               []byte
	Errors              [][]byte
	ScheduledAt         *time.Time
	ExpiresAt           *time.Time
	CreatedAt           time.Time
	EnqueuedAt          *time.Time
	StartedAt           *time.Time
	CompletedAt         *time.Time
	CancelledAt         *time.Time
	DiscardedAt         *time.Time
	VisibilityTimeout   *time.Time
	WorkerID            *string
	Tags                []string
	TimeoutMs           *int
	WorkflowID          *string
	WorkflowStep        *int
	ParentResults       [][]byte
	RetryDelayMs        *int64
	VisibilityTimeoutMs *int
	UnknownFields       []byte
}

// scanDest returns a slice of pointers for use with pgx Scan.
func (r *jobRow) scanDest() []any {
	return []any{
		&r.ID, &r.Type, &r.Queue, &r.Args, &r.Meta,
		&r.State, &r.Priority, &r.Attempt, &r.MaxAttempts, &r.RetryPolicy,
		&r.UniqueKey, &r.Result, &r.Error, &r.Errors,
		&r.ScheduledAt, &r.ExpiresAt, &r.CreatedAt, &r.EnqueuedAt,
		&r.StartedAt, &r.CompletedAt, &r.CancelledAt, &r.DiscardedAt,
		&r.VisibilityTimeout, &r.WorkerID, &r.Tags, &r.TimeoutMs,
		&r.WorkflowID, &r.WorkflowStep, &r.ParentResults, &r.RetryDelayMs,
		&r.VisibilityTimeoutMs, &r.UnknownFields,
	}
}

// toJob converts a scanned row into a core.Job.
func (r *jobRow) toJob() *core.Job {
	job := &core.Job{
		ID:      r.ID,
		Type:    r.Type,
		State:   r.State,
		Queue:   r.Queue,
		Attempt: r.Attempt,
	}

	if r.Priority != 0 {
		job.Priority = &r.Priority
	}

	job.MaxAttempts = &r.MaxAttempts

	if len(r.Args) > 0 {
		job.Args = json.RawMessage(r.Args)
	}
	if len(r.Meta) > 0 && string(r.Meta) != "{}" {
		job.Meta = json.RawMessage(r.Meta)
	}
	if len(r.Result) > 0 {
		job.Result = json.RawMessage(r.Result)
	}
	if len(r.Error) > 0 {
		job.Error = json.RawMessage(r.Error)
	}

	// Convert JSONB[] errors to []json.RawMessage
	if len(r.Errors) > 0 {
		job.Errors = make([]json.RawMessage, len(r.Errors))
		for i, e := range r.Errors {
			job.Errors[i] = json.RawMessage(e)
		}
	}

	if len(r.RetryPolicy) > 0 {
		var rp core.RetryPolicy
		if json.Unmarshal(r.RetryPolicy, &rp) == nil {
			job.Retry = &rp
		}
	}

	job.CreatedAt = core.FormatTime(r.CreatedAt)

	if r.EnqueuedAt != nil {
		job.EnqueuedAt = core.FormatTime(*r.EnqueuedAt)
	}
	if r.StartedAt != nil {
		job.StartedAt = core.FormatTime(*r.StartedAt)
	}
	if r.CompletedAt != nil {
		job.CompletedAt = core.FormatTime(*r.CompletedAt)
	}
	if r.CancelledAt != nil {
		job.CancelledAt = core.FormatTime(*r.CancelledAt)
	}
	if r.ScheduledAt != nil {
		job.ScheduledAt = core.FormatTime(*r.ScheduledAt)
	}
	if r.ExpiresAt != nil {
		job.ExpiresAt = core.FormatTime(*r.ExpiresAt)
	}

	if r.Tags != nil {
		job.Tags = r.Tags
	}
	if r.TimeoutMs != nil {
		job.TimeoutMs = r.TimeoutMs
	}
	if r.RetryDelayMs != nil {
		job.RetryDelayMs = r.RetryDelayMs
	}
	if r.VisibilityTimeoutMs != nil {
		job.VisibilityTimeoutMs = r.VisibilityTimeoutMs
	}

	if r.WorkflowID != nil {
		job.WorkflowID = *r.WorkflowID
	}
	if r.WorkflowStep != nil {
		job.WorkflowStep = *r.WorkflowStep
	}

	// Convert JSONB[] parent_results to []json.RawMessage
	if len(r.ParentResults) > 0 {
		job.ParentResults = make([]json.RawMessage, len(r.ParentResults))
		for i, pr := range r.ParentResults {
			job.ParentResults[i] = json.RawMessage(pr)
		}
	}

	// Restore unknown fields
	if len(r.UnknownFields) > 0 && string(r.UnknownFields) != "{}" && string(r.UnknownFields) != "null" {
		var uf map[string]json.RawMessage
		if json.Unmarshal(r.UnknownFields, &uf) == nil && len(uf) > 0 {
			job.UnknownFields = uf
		}
	}

	return job
}

// scanJob scans a single row from a jobs query (pgx.Rows) into a core.Job.
func scanJob(rows pgx.Rows) (*core.Job, error) {
	var r jobRow
	if err := rows.Scan(r.scanDest()...); err != nil {
		return nil, err
	}
	return r.toJob(), nil
}

// scanJobRow scans a single pgx.Row into a core.Job.
func scanJobRow(row pgx.Row) (*core.Job, error) {
	var r jobRow
	if err := row.Scan(r.scanDest()...); err != nil {
		return nil, err
	}
	return r.toJob(), nil
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
