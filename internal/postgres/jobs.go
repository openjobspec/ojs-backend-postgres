package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
	"github.com/openjobspec/ojs-backend-postgres/internal/metrics"
)

// Push enqueues a single job.
func (b *Backend) Push(ctx context.Context, job *core.Job) (*core.Job, error) {
	now := time.Now()

	if job.ID == "" {
		job.ID = core.NewUUIDv7()
	}

	job.CreatedAt = core.FormatTime(now)
	job.Attempt = 0

	// Handle unique jobs
	if job.Unique != nil {
		fingerprint := computeFingerprint(job)

		conflict := job.Unique.OnConflict
		if conflict == "" {
			conflict = "reject"
		}

		// Use a transaction to make the unique check + insert atomic
		tx, err := b.pool.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("begin unique job tx: %w", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// Check for existing job with same unique key in non-terminal states
		var existingID, existingState string
		query := `SELECT id, state FROM ojs_jobs WHERE unique_key = $1
			AND state NOT IN ('completed', 'cancelled', 'discarded') LIMIT 1 FOR UPDATE`

		var foundExisting bool
		if len(job.Unique.States) > 0 {
			query = `SELECT id, state FROM ojs_jobs WHERE unique_key = $1 AND state = ANY($2) LIMIT 1 FOR UPDATE`
			scanErr := tx.QueryRow(ctx, query, fingerprint, job.Unique.States).Scan(&existingID, &existingState)
			foundExisting = scanErr == nil && existingID != ""
		} else {
			scanErr := tx.QueryRow(ctx, query, fingerprint).Scan(&existingID, &existingState)
			foundExisting = scanErr == nil && existingID != ""
		}

		if foundExisting {
			conflictResult, conflictErr := b.handleUniqueConflict(ctx, conflict, fingerprint, existingID, job)
			if conflictResult != nil || conflictErr != nil {
				return conflictResult, conflictErr
			}
			// For "replace": conflict handler cancelled the existing job and returned (nil, nil)
			// to signal we should proceed with inserting the replacement below.
		}

		created, err := b.insertJob(ctx, job, now, &fingerprint)
		if err != nil {
			return nil, err
		}

		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit unique job tx: %w", err)
		}

		return created, nil
	}

	return b.insertJob(ctx, job, now, nil)
}

func (b *Backend) handleUniqueConflict(ctx context.Context, conflict, fingerprint, existingID string, job *core.Job) (*core.Job, error) {
	switch conflict {
	case "reject":
		return nil, &core.OJSError{
			Code:    core.ErrCodeDuplicate,
			Message: "A job with the same unique key already exists.",
			Details: map[string]any{
				"existing_job_id": existingID,
				"unique_key":      fingerprint,
			},
		}
	case "ignore":
		existing, err := b.Info(ctx, existingID)
		if err == nil {
			existing.IsExisting = true
			return existing, nil
		}
		return nil, err
	case "replace":
		_, _ = b.Cancel(ctx, existingID)
		return nil, nil // signal caller to proceed with insert below
	default:
		return nil, &core.OJSError{
			Code:    core.ErrCodeDuplicate,
			Message: "A job with the same unique key already exists.",
			Details: map[string]any{
				"existing_job_id": existingID,
				"unique_key":      fingerprint,
			},
		}
	}
}

func (b *Backend) insertJob(ctx context.Context, job *core.Job, now time.Time, uniqueKey *string) (*core.Job, error) {
	// Determine initial state
	var scheduledTime *time.Time
	if job.ScheduledAt != "" {
		if t, err := time.Parse(time.RFC3339, job.ScheduledAt); err == nil && t.After(now) {
			job.State = core.StateScheduled
			job.EnqueuedAt = core.FormatTime(now)
			scheduledTime = &t
		}
	}

	if job.State == "" {
		job.State = core.StateAvailable
		job.EnqueuedAt = core.FormatTime(now)
	}

	priority := 0
	if job.Priority != nil {
		priority = *job.Priority
	}
	maxAttempts := 3
	if job.MaxAttempts != nil {
		maxAttempts = *job.MaxAttempts
	}

	var retryPolicyJSON []byte
	if job.Retry != nil {
		retryPolicyJSON, _ = json.Marshal(job.Retry)
	}

	var expiresAt *time.Time
	if job.ExpiresAt != "" {
		if t, err := time.Parse(time.RFC3339, job.ExpiresAt); err == nil {
			expiresAt = &t
		}
	}

	var unknownFieldsJSON []byte
	if len(job.UnknownFields) > 0 {
		unknownFieldsJSON, _ = json.Marshal(job.UnknownFields)
	}

	var parentResultsBytes [][]byte
	for _, pr := range job.ParentResults {
		parentResultsBytes = append(parentResultsBytes, []byte(pr))
	}

	var workflowID *string
	var workflowStep *int
	if job.WorkflowID != "" {
		workflowID = &job.WorkflowID
		workflowStep = &job.WorkflowStep
	}

	var errorJSON []byte
	if len(job.Error) > 0 {
		errorJSON = []byte(job.Error)
	}

	var metaJSON []byte
	if len(job.Meta) > 0 {
		metaJSON = []byte(job.Meta)
	} else {
		metaJSON = []byte("{}")
	}

	var argsJSON []byte
	if job.Args != nil {
		argsJSON = []byte(job.Args)
	} else {
		argsJSON = []byte("[]")
	}

	enqueuedAt := now

	// Store rate limit config at queue level if specified
	if job.RateLimit != nil && job.RateLimit.MaxPerSecond > 0 {
		b.logExec(ctx, "rate-limit",
			`INSERT INTO ojs_queues (name, rate_limit) VALUES ($1, $2)
			 ON CONFLICT (name) DO UPDATE SET rate_limit = $2`,
			job.Queue, job.RateLimit.MaxPerSecond)
	}

	_, err := b.pool.Exec(ctx, `
		INSERT INTO ojs_jobs (
			id, type, queue, args, meta, state, priority, attempt, max_attempts,
			retry_policy, unique_key, error, scheduled_at, expires_at,
			created_at, enqueued_at, tags, timeout_ms, workflow_id, workflow_step,
			parent_results, visibility_timeout_ms, unknown_fields
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9,
			$10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19, $20,
			$21, $22, $23
		)`,
		job.ID, job.Type, job.Queue, argsJSON, metaJSON,
		job.State, priority, job.Attempt, maxAttempts,
		retryPolicyJSON, uniqueKey, errorJSON,
		scheduledTime, expiresAt,
		now, enqueuedAt,
		job.Tags, job.TimeoutMs, workflowID, workflowStep,
		parentResultsBytes, job.VisibilityTimeoutMs, unknownFieldsJSON,
	)
	if err != nil {
		return nil, fmt.Errorf("insert job: %w", err)
	}

	// Notify listeners of available job
	if job.State == core.StateAvailable {
		notifyJobAvailable(ctx, b.pool, job.Queue)
	}

	metrics.JobsEnqueued.WithLabelValues(job.Queue, job.Type).Inc()

	return b.Info(ctx, job.ID)
}

// Info retrieves job details.
func (b *Backend) Info(ctx context.Context, jobID string) (*core.Job, error) {
	query := fmt.Sprintf("SELECT %s FROM ojs_jobs WHERE id = $1", allJobColumns)
	row := b.pool.QueryRow(ctx, query, jobID)
	job, err := scanJobRow(row)
	if err != nil {
		return nil, core.NewNotFoundError("Job", jobID)
	}
	return job, nil
}

// Cancel cancels a job.
func (b *Backend) Cancel(ctx context.Context, jobID string) (*core.Job, error) {
	var currentState string
	err := b.pool.QueryRow(ctx, "SELECT state FROM ojs_jobs WHERE id = $1", jobID).Scan(&currentState)
	if err != nil {
		return nil, core.NewNotFoundError("Job", jobID)
	}

	if core.IsTerminalState(currentState) {
		return nil, core.NewConflictError(
			fmt.Sprintf("Cannot cancel job in terminal state '%s'.", currentState),
			map[string]any{
				"job_id":        jobID,
				"current_state": currentState,
			},
		)
	}

	now := time.Now()
	_, err = b.pool.Exec(ctx, `
		UPDATE ojs_jobs SET
			state = 'cancelled',
			cancelled_at = $1,
			visibility_timeout = NULL,
			worker_id = NULL
		WHERE id = $2`, now, jobID)
	if err != nil {
		return nil, fmt.Errorf("cancel job: %w", err)
	}

	return b.Info(ctx, jobID)
}

// PushBatch atomically enqueues multiple jobs.
func (b *Backend) PushBatch(ctx context.Context, jobs []*core.Job) ([]*core.Job, error) {
	now := time.Now()
	var created []*core.Job

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin batch tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, job := range jobs {
		if job.ID == "" {
			job.ID = core.NewUUIDv7()
		}
		job.State = core.StateAvailable
		job.Attempt = 0
		job.CreatedAt = core.FormatTime(now)
		job.EnqueuedAt = core.FormatTime(now)

		priority := 0
		if job.Priority != nil {
			priority = *job.Priority
		}
		maxAttempts := 3
		if job.MaxAttempts != nil {
			maxAttempts = *job.MaxAttempts
		}

		var retryPolicyJSON []byte
		if job.Retry != nil {
			retryPolicyJSON, _ = json.Marshal(job.Retry)
		}

		var argsJSON []byte
		if job.Args != nil {
			argsJSON = []byte(job.Args)
		} else {
			argsJSON = []byte("[]")
		}

		var metaJSON []byte
		if len(job.Meta) > 0 {
			metaJSON = []byte(job.Meta)
		} else {
			metaJSON = []byte("{}")
		}

		var unknownFieldsJSON []byte
		if len(job.UnknownFields) > 0 {
			unknownFieldsJSON, _ = json.Marshal(job.UnknownFields)
		}

		_, err := tx.Exec(ctx, `
			INSERT INTO ojs_jobs (
				id, type, queue, args, meta, state, priority, attempt, max_attempts,
				retry_policy, created_at, enqueued_at, tags, timeout_ms, unknown_fields
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
			job.ID, job.Type, job.Queue, argsJSON, metaJSON,
			job.State, priority, job.Attempt, maxAttempts,
			retryPolicyJSON, now, now, job.Tags, job.TimeoutMs, unknownFieldsJSON)
		if err != nil {
			return nil, fmt.Errorf("batch insert job: %w", err)
		}

		created = append(created, job)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit batch: %w", err)
	}

	// Notify for each unique queue and record metrics
	queues := make(map[string]bool)
	for _, job := range created {
		metrics.JobsEnqueued.WithLabelValues(job.Queue, job.Type).Inc()
		if !queues[job.Queue] {
			queues[job.Queue] = true
			notifyJobAvailable(ctx, b.pool, job.Queue)
		}
	}

	return created, nil
}

func computeFingerprint(job *core.Job) string {
	h := sha256.New()
	keys := job.Unique.Keys
	if len(keys) == 0 {
		keys = []string{"type", "args"}
	}
	sort.Strings(keys)
	for _, key := range keys {
		switch key {
		case "type":
			h.Write([]byte("type:"))
			h.Write([]byte(job.Type))
		case "args":
			h.Write([]byte("args:"))
			if job.Args != nil {
				h.Write(job.Args)
			}
		case "queue":
			h.Write([]byte("queue:"))
			h.Write([]byte(job.Queue))
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

var patternCache sync.Map // map[string]*regexp.Regexp

func matchesPattern(s, pattern string) bool {
	key := "^" + pattern + "$"
	if cached, ok := patternCache.Load(key); ok {
		re, _ := cached.(*regexp.Regexp)
		return re.MatchString(s)
	}
	re, err := regexp.Compile(key)
	if err != nil {
		return s == pattern
	}
	patternCache.Store(key, re)
	return re.MatchString(s)
}
