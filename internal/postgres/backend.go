package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// PostgresBackend implements core.Backend using PostgreSQL.
type PostgresBackend struct {
	pool      *pgxpool.Pool
	startTime time.Time
	cancel    context.CancelFunc
}

// New creates a new PostgresBackend, runs migrations, and starts LISTEN/NOTIFY.
func New(databaseURL string) (*PostgresBackend, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing database URL: %w", err)
	}

	config.MinConns = 2
	config.MaxConns = 20

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}

	// Run migrations
	if err := RunMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	listenCtx, cancel := context.WithCancel(ctx)

	b := &PostgresBackend{
		pool:      pool,
		startTime: time.Now(),
		cancel:    cancel,
	}

	// Start LISTEN/NOTIFY listener
	listenForNotifications(listenCtx, pool, func(queue string) {
		// Notification received - workers can be woken up here
		// For now this is a no-op since HTTP workers poll via Fetch
	})

	return b, nil
}

func (b *PostgresBackend) Close() error {
	b.cancel()
	b.pool.Close()
	return nil
}

// Push enqueues a single job.
func (b *PostgresBackend) Push(ctx context.Context, job *core.Job) (*core.Job, error) {
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

		// Check for existing job with same unique key in non-terminal states
		var existingID, existingState string
		query := `SELECT id, state FROM ojs_jobs WHERE unique_key = $1
			AND state NOT IN ('completed', 'cancelled', 'discarded') LIMIT 1`

		// If unique.states is specified, only check those states
		if len(job.Unique.States) > 0 {
			query = `SELECT id, state FROM ojs_jobs WHERE unique_key = $1 AND state = ANY($2) LIMIT 1`
			err := b.pool.QueryRow(ctx, query, fingerprint, job.Unique.States).Scan(&existingID, &existingState)
			if err == nil && existingID != "" {
				return b.handleUniqueConflict(ctx, conflict, fingerprint, existingID, job)
			}
		} else {
			err := b.pool.QueryRow(ctx, query, fingerprint).Scan(&existingID, &existingState)
			if err == nil && existingID != "" {
				return b.handleUniqueConflict(ctx, conflict, fingerprint, existingID, job)
			}
		}

		return b.insertJob(ctx, job, now, &fingerprint)
	}

	return b.insertJob(ctx, job, now, nil)
}

func (b *PostgresBackend) handleUniqueConflict(ctx context.Context, conflict, fingerprint, existingID string, job *core.Job) (*core.Job, error) {
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
		b.Cancel(ctx, existingID)
		return nil, nil // signal caller to proceed with insert
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

func (b *PostgresBackend) insertJob(ctx context.Context, job *core.Job, now time.Time, uniqueKey *string) (*core.Job, error) {
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
	if job.Error != nil && len(job.Error) > 0 {
		errorJSON = []byte(job.Error)
	}

	var metaJSON []byte
	if job.Meta != nil && len(job.Meta) > 0 {
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
		_, _ = b.pool.Exec(ctx,
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

	return b.Info(ctx, job.ID)
}

// Fetch claims jobs from the specified queues.
func (b *PostgresBackend) Fetch(ctx context.Context, queues []string, count int, workerID string, visibilityTimeoutMs int) ([]*core.Job, error) {
	now := time.Now()
	var allJobs []*core.Job

	for _, queue := range queues {
		if len(allJobs) >= count {
			break
		}

		// Check if queue is paused
		var status string
		err := b.pool.QueryRow(ctx, "SELECT status FROM ojs_queues WHERE name = $1", queue).Scan(&status)
		if err == nil && status == "paused" {
			continue
		}

		// Check rate limit
		var rateLimit *int
		_ = b.pool.QueryRow(ctx, "SELECT rate_limit FROM ojs_queues WHERE name = $1", queue).Scan(&rateLimit)

		remaining := count - len(allJobs)

		// Use SELECT ... FOR UPDATE SKIP LOCKED for non-blocking dequeue
		query := fmt.Sprintf(`
			UPDATE ojs_jobs SET
				state = 'active',
				started_at = $1,
				worker_id = $2,
				visibility_timeout = $3
			WHERE id IN (
				SELECT id FROM ojs_jobs
				WHERE queue = $4 AND state = 'available'
				AND (expires_at IS NULL OR expires_at > $1)
				ORDER BY priority DESC, id
				FOR UPDATE SKIP LOCKED
				LIMIT $5
			)
			RETURNING %s`, allJobColumns)

		effectiveVisTimeout := visibilityTimeoutMs
		if effectiveVisTimeout <= 0 {
			effectiveVisTimeout = 30000 // default 30s
		}
		visDeadline := now.Add(time.Duration(effectiveVisTimeout) * time.Millisecond)

		rows, err := b.pool.Query(ctx, query, now, workerID, visDeadline, queue, remaining)
		if err != nil {
			return nil, fmt.Errorf("fetch jobs: %w", err)
		}

		for rows.Next() {
			job, err := scanJob(rows)
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan fetched job: %w", err)
			}

			// If job has a per-job visibility_timeout_ms, update the deadline
			if job.VisibilityTimeoutMs != nil && *job.VisibilityTimeoutMs > 0 && visibilityTimeoutMs <= 0 {
				jobVisDeadline := now.Add(time.Duration(*job.VisibilityTimeoutMs) * time.Millisecond)
				_, _ = b.pool.Exec(ctx, "UPDATE ojs_jobs SET visibility_timeout = $1 WHERE id = $2", jobVisDeadline, job.ID)
			}

			allJobs = append(allJobs, job)
		}
		rows.Close()

		// Discard expired jobs that were skipped
		_, _ = b.pool.Exec(ctx, `
			UPDATE ojs_jobs SET state = 'discarded'
			WHERE queue = $1 AND state = 'available' AND expires_at IS NOT NULL AND expires_at <= $2`,
			queue, now)
	}

	return allJobs, nil
}

// Ack acknowledges a job as completed.
func (b *PostgresBackend) Ack(ctx context.Context, jobID string, result []byte) (*core.AckResponse, error) {
	now := time.Now()

	var currentState, queue string
	err := b.pool.QueryRow(ctx, "SELECT state, queue FROM ojs_jobs WHERE id = $1", jobID).Scan(&currentState, &queue)
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
	if result != nil && len(result) > 0 {
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
	_, _ = b.pool.Exec(ctx,
		"UPDATE ojs_queues SET completed_count = completed_count + 1 WHERE name = $1", queue)

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
func (b *PostgresBackend) Nack(ctx context.Context, jobID string, jobErr *core.JobError, requeue bool) (*core.NackResponse, error) {
	now := time.Now()

	var currentState, queue string
	var attempt, maxAttempts int
	var retryPolicyJSON []byte
	var priority int

	err := b.pool.QueryRow(ctx,
		"SELECT state, queue, attempt, max_attempts, retry_policy, priority FROM ojs_jobs WHERE id = $1",
		jobID).Scan(&currentState, &queue, &attempt, &maxAttempts, &retryPolicyJSON, &priority)
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

// Info retrieves job details.
func (b *PostgresBackend) Info(ctx context.Context, jobID string) (*core.Job, error) {
	query := fmt.Sprintf("SELECT %s FROM ojs_jobs WHERE id = $1", allJobColumns)
	row := b.pool.QueryRow(ctx, query, jobID)
	job, err := scanJobRow(row)
	if err != nil {
		return nil, core.NewNotFoundError("Job", jobID)
	}
	return job, nil
}

// Cancel cancels a job.
func (b *PostgresBackend) Cancel(ctx context.Context, jobID string) (*core.Job, error) {
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

// ListQueues returns all known queues.
func (b *PostgresBackend) ListQueues(ctx context.Context) ([]core.QueueInfo, error) {
	rows, err := b.pool.Query(ctx, "SELECT name, status FROM ojs_queues ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queues []core.QueueInfo
	for rows.Next() {
		var name, status string
		if err := rows.Scan(&name, &status); err != nil {
			return nil, err
		}
		queues = append(queues, core.QueueInfo{
			Name:   name,
			Status: status,
		})
	}
	return queues, nil
}

// Health returns the health status.
func (b *PostgresBackend) Health(ctx context.Context) (*core.HealthResponse, error) {
	start := time.Now()
	err := b.pool.Ping(ctx)
	latency := time.Since(start).Milliseconds()

	resp := &core.HealthResponse{
		Version:       core.OJSVersion,
		UptimeSeconds: int64(time.Since(b.startTime).Seconds()),
	}

	if err != nil {
		resp.Status = "degraded"
		resp.Backend = core.BackendHealth{
			Type:   "postgres",
			Status: "disconnected",
			Error:  err.Error(),
		}
		return resp, err
	}

	resp.Status = "ok"
	resp.Backend = core.BackendHealth{
		Type:      "postgres",
		Status:    "connected",
		LatencyMs: latency,
	}
	return resp, nil
}

// Heartbeat extends visibility timeout and reports worker state.
func (b *PostgresBackend) Heartbeat(ctx context.Context, workerID string, activeJobs []string, visibilityTimeoutMs int) (*core.HeartbeatResponse, error) {
	now := time.Now()

	// Upsert worker
	_, _ = b.pool.Exec(ctx, `
		INSERT INTO ojs_workers (worker_id, last_heartbeat, active_jobs_count)
		VALUES ($1, $2, $3)
		ON CONFLICT (worker_id) DO UPDATE SET
			last_heartbeat = $2,
			active_jobs_count = $3`,
		workerID, now, len(activeJobs))

	// Extend visibility for active jobs
	extended := make([]string, 0)
	timeout := time.Duration(visibilityTimeoutMs) * time.Millisecond
	visDeadline := now.Add(timeout)

	for _, jobID := range activeJobs {
		tag, err := b.pool.Exec(ctx,
			"UPDATE ojs_jobs SET visibility_timeout = $1 WHERE id = $2 AND state = 'active'",
			visDeadline, jobID)
		if err == nil && tag.RowsAffected() > 0 {
			extended = append(extended, jobID)
		}
	}

	// Get directive
	directive := "continue"
	var storedDirective *string
	err := b.pool.QueryRow(ctx, "SELECT directive FROM ojs_workers WHERE worker_id = $1", workerID).Scan(&storedDirective)
	if err == nil && storedDirective != nil && *storedDirective != "" {
		directive = *storedDirective
	}

	// Check job metadata for test_directive
	if directive == "continue" {
		for _, jobID := range activeJobs {
			var meta []byte
			err := b.pool.QueryRow(ctx, "SELECT meta FROM ojs_jobs WHERE id = $1", jobID).Scan(&meta)
			if err == nil && len(meta) > 0 {
				var metaObj map[string]any
				if json.Unmarshal(meta, &metaObj) == nil {
					if td, ok := metaObj["test_directive"]; ok {
						if tdStr, ok := td.(string); ok && tdStr != "" {
							directive = tdStr
							break
						}
					}
				}
			}
		}
	}

	return &core.HeartbeatResponse{
		State:        "active",
		Directive:    directive,
		JobsExtended: extended,
		ServerTime:   core.FormatTime(now),
	}, nil
}

// ListDeadLetter returns dead letter (discarded) jobs.
func (b *PostgresBackend) ListDeadLetter(ctx context.Context, limit, offset int) ([]*core.Job, int, error) {
	var total int
	err := b.pool.QueryRow(ctx, "SELECT COUNT(*) FROM ojs_jobs WHERE state = 'discarded'").Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	query := fmt.Sprintf(`SELECT %s FROM ojs_jobs WHERE state = 'discarded'
		ORDER BY completed_at DESC LIMIT $1 OFFSET $2`, allJobColumns)
	rows, err := b.pool.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var jobs []*core.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, 0, err
		}
		jobs = append(jobs, job)
	}

	return jobs, total, nil
}

// RetryDeadLetter retries a dead letter job.
func (b *PostgresBackend) RetryDeadLetter(ctx context.Context, jobID string) (*core.Job, error) {
	var state string
	err := b.pool.QueryRow(ctx, "SELECT state FROM ojs_jobs WHERE id = $1", jobID).Scan(&state)
	if err != nil || state != core.StateDiscarded {
		return nil, core.NewNotFoundError("Dead letter job", jobID)
	}

	now := time.Now()
	_, err = b.pool.Exec(ctx, `
		UPDATE ojs_jobs SET
			state = 'available',
			attempt = 0,
			enqueued_at = $1,
			error = NULL,
			errors = ARRAY[]::JSONB[],
			completed_at = NULL,
			retry_delay_ms = NULL,
			started_at = NULL,
			worker_id = NULL,
			visibility_timeout = NULL
		WHERE id = $2`, now, jobID)
	if err != nil {
		return nil, fmt.Errorf("retry dead letter: %w", err)
	}

	return b.Info(ctx, jobID)
}

// DeleteDeadLetter removes a dead letter job.
func (b *PostgresBackend) DeleteDeadLetter(ctx context.Context, jobID string) error {
	tag, err := b.pool.Exec(ctx, "DELETE FROM ojs_jobs WHERE id = $1 AND state = 'discarded'", jobID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return core.NewNotFoundError("Dead letter job", jobID)
	}
	return nil
}

// RegisterCron registers a cron job.
func (b *PostgresBackend) RegisterCron(ctx context.Context, cronJob *core.CronJob) (*core.CronJob, error) {
	expr := cronJob.Expression
	if expr == "" {
		expr = cronJob.Schedule
	}

	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	var schedule cron.Schedule
	var err error

	if cronJob.Timezone != "" {
		loc, locErr := time.LoadLocation(cronJob.Timezone)
		if locErr != nil {
			return nil, core.NewInvalidRequestError(
				fmt.Sprintf("Invalid timezone: %s", cronJob.Timezone),
				map[string]any{"timezone": cronJob.Timezone},
			)
		}
		schedule, err = parser.Parse("CRON_TZ=" + loc.String() + " " + expr)
		if err != nil {
			schedule, err = parser.Parse(expr)
		}
	} else {
		schedule, err = parser.Parse(expr)
	}

	if err != nil {
		return nil, core.NewInvalidRequestError(
			fmt.Sprintf("Invalid cron expression: %s", expr),
			map[string]any{"expression": expr, "error": err.Error()},
		)
	}

	now := time.Now()
	cronJob.CreatedAt = core.FormatTime(now)
	cronJob.NextRunAt = core.FormatTime(schedule.Next(now))
	cronJob.Schedule = expr
	cronJob.Expression = expr

	if cronJob.Queue == "" {
		cronJob.Queue = "default"
	}
	if cronJob.OverlapPolicy == "" {
		cronJob.OverlapPolicy = "allow"
	}
	cronJob.Enabled = true

	templateJSON, _ := json.Marshal(cronJob.JobTemplate)

	_, err = b.pool.Exec(ctx, `
		INSERT INTO ojs_cron_jobs (name, expression, timezone, overlap_policy, enabled, job_template, queue, created_at, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (name) DO UPDATE SET
			expression = $2, timezone = $3, overlap_policy = $4, enabled = $5,
			job_template = $6, queue = $7, next_run_at = $9`,
		cronJob.Name, expr, cronJob.Timezone, cronJob.OverlapPolicy, cronJob.Enabled,
		templateJSON, cronJob.Queue, now, schedule.Next(now))
	if err != nil {
		return nil, fmt.Errorf("register cron: %w", err)
	}

	return cronJob, nil
}

// ListCron lists all registered cron jobs.
func (b *PostgresBackend) ListCron(ctx context.Context) ([]*core.CronJob, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT name, expression, timezone, overlap_policy, enabled, job_template,
			queue, created_at, next_run_at, last_run_at
		FROM ojs_cron_jobs ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var crons []*core.CronJob
	for rows.Next() {
		var (
			name, expression, overlapPolicy, queue string
			timezone                               *string
			enabled                                bool
			templateJSON                           []byte
			createdAt                              time.Time
			nextRunAt, lastRunAt                   *time.Time
		)
		if err := rows.Scan(&name, &expression, &timezone, &overlapPolicy, &enabled,
			&templateJSON, &queue, &createdAt, &nextRunAt, &lastRunAt); err != nil {
			return nil, err
		}

		cj := &core.CronJob{
			Name:          name,
			Expression:    expression,
			OverlapPolicy: overlapPolicy,
			Enabled:       enabled,
			Queue:         queue,
			CreatedAt:     core.FormatTime(createdAt),
		}
		if timezone != nil {
			cj.Timezone = *timezone
		}
		if nextRunAt != nil {
			cj.NextRunAt = core.FormatTime(*nextRunAt)
		}
		if lastRunAt != nil {
			cj.LastRunAt = core.FormatTime(*lastRunAt)
		}
		if len(templateJSON) > 0 {
			var tmpl core.CronJobTemplate
			if json.Unmarshal(templateJSON, &tmpl) == nil {
				cj.JobTemplate = &tmpl
			}
		}

		crons = append(crons, cj)
	}
	return crons, nil
}

// DeleteCron removes a cron job.
func (b *PostgresBackend) DeleteCron(ctx context.Context, name string) (*core.CronJob, error) {
	// Get the cron job first
	crons, err := b.ListCron(ctx)
	if err != nil {
		return nil, err
	}

	var found *core.CronJob
	for _, c := range crons {
		if c.Name == name {
			found = c
			break
		}
	}
	if found == nil {
		return nil, core.NewNotFoundError("Cron job", name)
	}

	_, err = b.pool.Exec(ctx, "DELETE FROM ojs_cron_jobs WHERE name = $1", name)
	if err != nil {
		return nil, fmt.Errorf("delete cron: %w", err)
	}

	return found, nil
}

// CreateWorkflow creates and starts a workflow.
func (b *PostgresBackend) CreateWorkflow(ctx context.Context, req *core.WorkflowRequest) (*core.Workflow, error) {
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

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin workflow tx: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO ojs_workflows (id, name, type, state, total, completed, failed, job_defs, callbacks, created_at)
		VALUES ($1, $2, $3, $4, $5, 0, 0, $6, $7, $8)`,
		wfID, req.Name, req.Type, "running", total, jobDefsJSON, callbacksJSON, now)
	if err != nil {
		return nil, fmt.Errorf("insert workflow: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit workflow: %w", err)
	}

	if req.Type == "chain" {
		// Chain: only enqueue the first step
		step := jobs[0]
		queue := "default"
		if step.Options != nil && step.Options.Queue != "" {
			queue = step.Options.Queue
		}

		job := &core.Job{
			Type:         step.Type,
			Args:         step.Args,
			Queue:        queue,
			WorkflowID:   wfID,
			WorkflowStep: 0,
		}
		if step.Options != nil && step.Options.RetryPolicy != nil {
			job.Retry = step.Options.RetryPolicy
			job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
		} else if step.Options != nil && step.Options.Retry != nil {
			job.Retry = step.Options.Retry
			job.MaxAttempts = &step.Options.Retry.MaxAttempts
		}

		created, err := b.Push(ctx, job)
		if err != nil {
			return nil, err
		}

		_, _ = b.pool.Exec(ctx, "UPDATE ojs_workflows SET job_ids = array_append(job_ids, $1) WHERE id = $2",
			created.ID, wfID)
	} else {
		// Group/Batch: enqueue all jobs
		for i, step := range jobs {
			queue := "default"
			if step.Options != nil && step.Options.Queue != "" {
				queue = step.Options.Queue
			}

			job := &core.Job{
				Type:         step.Type,
				Args:         step.Args,
				Queue:        queue,
				WorkflowID:   wfID,
				WorkflowStep: i,
			}
			if step.Options != nil && step.Options.RetryPolicy != nil {
				job.Retry = step.Options.RetryPolicy
				job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
			} else if step.Options != nil && step.Options.Retry != nil {
				job.Retry = step.Options.Retry
				job.MaxAttempts = &step.Options.Retry.MaxAttempts
			}

			created, err := b.Push(ctx, job)
			if err != nil {
				return nil, err
			}

			_, _ = b.pool.Exec(ctx, "UPDATE ojs_workflows SET job_ids = array_append(job_ids, $1) WHERE id = $2",
				created.ID, wfID)
		}
	}

	return wf, nil
}

// GetWorkflow retrieves a workflow by ID.
func (b *PostgresBackend) GetWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
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
func (b *PostgresBackend) CancelWorkflow(ctx context.Context, id string) (*core.Workflow, error) {
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
		for rows.Next() {
			var jobID string
			rows.Scan(&jobID)
			jobIDs = append(jobIDs, jobID)
		}
		rows.Close()
	}

	for _, jobID := range jobIDs {
		var jobState string
		if b.pool.QueryRow(ctx, "SELECT state FROM ojs_jobs WHERE id = $1", jobID).Scan(&jobState) == nil {
			if !core.IsTerminalState(jobState) {
				b.Cancel(ctx, jobID)
			}
		}
	}

	now := time.Now()
	wf.State = "cancelled"
	wf.CompletedAt = core.FormatTime(now)

	_, _ = b.pool.Exec(ctx,
		"UPDATE ojs_workflows SET state = 'cancelled', completed_at = $1 WHERE id = $2",
		now, id)

	return wf, nil
}

// AdvanceWorkflow is called after ACK or NACK to update workflow state.
func (b *PostgresBackend) AdvanceWorkflow(ctx context.Context, workflowID string, jobID string, result json.RawMessage, failed bool) error {
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
	if result != nil && len(result) > 0 {
		_, _ = b.pool.Exec(ctx, `
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
			_, _ = b.pool.Exec(ctx,
				"UPDATE ojs_workflows SET state = 'failed', completed = $1, failed = $2, completed_at = $3 WHERE id = $4",
				completed, failedCount, time.Now(), workflowID)
			return nil
		}

		if totalFinished >= total {
			_, _ = b.pool.Exec(ctx,
				"UPDATE ojs_workflows SET state = 'completed', completed = $1, failed = $2, completed_at = $3 WHERE id = $4",
				completed, failedCount, time.Now(), workflowID)
			return nil
		}

		// Update counters and enqueue next step
		_, _ = b.pool.Exec(ctx,
			"UPDATE ojs_workflows SET completed = $1, failed = $2 WHERE id = $3",
			completed, failedCount, workflowID)
		return b.enqueueChainStep(ctx, workflowID, jobDefsJSON, stepIdx+1)
	}

	// Group/Batch
	_, _ = b.pool.Exec(ctx,
		"UPDATE ojs_workflows SET completed = $1, failed = $2 WHERE id = $3",
		completed, failedCount, workflowID)

	if totalFinished >= total {
		finalState := "completed"
		if failedCount > 0 {
			finalState = "failed"
		}
		_, _ = b.pool.Exec(ctx,
			"UPDATE ojs_workflows SET state = $1, completed_at = $2 WHERE id = $3",
			finalState, time.Now(), workflowID)

		if wfType == "batch" && len(callbacksJSON) > 0 {
			b.fireBatchCallbacks(ctx, callbacksJSON, failedCount > 0)
		}
	}

	return nil
}

func (b *PostgresBackend) enqueueChainStep(ctx context.Context, workflowID string, jobDefsJSON []byte, stepIdx int) error {
	var jobDefs []core.WorkflowJobRequest
	if err := json.Unmarshal(jobDefsJSON, &jobDefs); err != nil {
		return err
	}

	if stepIdx >= len(jobDefs) {
		return nil
	}

	step := jobDefs[stepIdx]
	queue := "default"
	if step.Options != nil && step.Options.Queue != "" {
		queue = step.Options.Queue
	}

	// Collect parent results
	var resultsJSON []byte
	_ = b.pool.QueryRow(ctx, "SELECT results FROM ojs_workflows WHERE id = $1", workflowID).Scan(&resultsJSON)

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

	job := &core.Job{
		Type:          step.Type,
		Args:          step.Args,
		Queue:         queue,
		WorkflowID:    workflowID,
		WorkflowStep:  stepIdx,
		ParentResults: parentResults,
	}
	if step.Options != nil && step.Options.RetryPolicy != nil {
		job.Retry = step.Options.RetryPolicy
		job.MaxAttempts = &step.Options.RetryPolicy.MaxAttempts
	} else if step.Options != nil && step.Options.Retry != nil {
		job.Retry = step.Options.Retry
		job.MaxAttempts = &step.Options.Retry.MaxAttempts
	}

	created, err := b.Push(ctx, job)
	if err != nil {
		return err
	}

	_, _ = b.pool.Exec(ctx, "UPDATE ojs_workflows SET job_ids = array_append(job_ids, $1) WHERE id = $2",
		created.ID, workflowID)
	return nil
}

func (b *PostgresBackend) fireBatchCallbacks(ctx context.Context, callbacksJSON []byte, hasFailure bool) {
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

func (b *PostgresBackend) fireCallback(ctx context.Context, cb *core.WorkflowCallback) {
	queue := "default"
	if cb.Options != nil && cb.Options.Queue != "" {
		queue = cb.Options.Queue
	}
	b.Push(ctx, &core.Job{
		Type:  cb.Type,
		Args:  cb.Args,
		Queue: queue,
	})
}

// PushBatch atomically enqueues multiple jobs.
func (b *PostgresBackend) PushBatch(ctx context.Context, jobs []*core.Job) ([]*core.Job, error) {
	now := time.Now()
	var created []*core.Job

	tx, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin batch tx: %w", err)
	}
	defer tx.Rollback(ctx)

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
		if job.Meta != nil && len(job.Meta) > 0 {
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

	// Notify for each unique queue
	queues := make(map[string]bool)
	for _, job := range created {
		if !queues[job.Queue] {
			queues[job.Queue] = true
			notifyJobAvailable(ctx, b.pool, job.Queue)
		}
	}

	return created, nil
}

// QueueStats returns statistics for a queue.
func (b *PostgresBackend) QueueStats(ctx context.Context, name string) (*core.QueueStats, error) {
	status := "active"
	var queueStatus *string
	var completedCount int64
	err := b.pool.QueryRow(ctx, "SELECT status, completed_count FROM ojs_queues WHERE name = $1", name).Scan(&queueStatus, &completedCount)
	if err == nil && queueStatus != nil {
		status = *queueStatus
	}

	var available, active, scheduled, retryable, dead int
	b.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM ojs_jobs WHERE queue = $1 AND state = 'available'", name).Scan(&available)
	b.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM ojs_jobs WHERE queue = $1 AND state = 'active'", name).Scan(&active)
	b.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM ojs_jobs WHERE queue = $1 AND state = 'scheduled'", name).Scan(&scheduled)
	b.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM ojs_jobs WHERE queue = $1 AND state = 'retryable'", name).Scan(&retryable)
	b.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM ojs_jobs WHERE queue = $1 AND state = 'discarded'", name).Scan(&dead)

	return &core.QueueStats{
		Queue:  name,
		Status: status,
		Stats: core.Stats{
			Available: available,
			Active:    active,
			Completed: int(completedCount),
			Scheduled: scheduled,
			Retryable: retryable,
			Dead:      dead,
		},
	}, nil
}

// PauseQueue pauses a queue.
func (b *PostgresBackend) PauseQueue(ctx context.Context, name string) error {
	_, err := b.pool.Exec(ctx, `
		INSERT INTO ojs_queues (name, status) VALUES ($1, 'paused')
		ON CONFLICT (name) DO UPDATE SET status = 'paused'`, name)
	return err
}

// ResumeQueue resumes a queue.
func (b *PostgresBackend) ResumeQueue(ctx context.Context, name string) error {
	_, err := b.pool.Exec(ctx, `
		INSERT INTO ojs_queues (name, status) VALUES ($1, 'active')
		ON CONFLICT (name) DO UPDATE SET status = 'active'`, name)
	return err
}

// SetWorkerState sets a directive for a worker.
func (b *PostgresBackend) SetWorkerState(ctx context.Context, workerID string, state string) error {
	_, err := b.pool.Exec(ctx, `
		INSERT INTO ojs_workers (worker_id, directive) VALUES ($1, $2)
		ON CONFLICT (worker_id) DO UPDATE SET directive = $2`,
		workerID, state)
	return err
}

// advanceWorkflowForJob checks if a job belongs to a workflow and advances it.
func (b *PostgresBackend) advanceWorkflowForJob(ctx context.Context, jobID, state string, result []byte) {
	var workflowID *string
	err := b.pool.QueryRow(ctx, "SELECT workflow_id FROM ojs_jobs WHERE id = $1", jobID).Scan(&workflowID)
	if err != nil || workflowID == nil {
		return
	}

	failed := state == core.StateDiscarded || state == core.StateCancelled
	b.AdvanceWorkflow(ctx, *workflowID, jobID, json.RawMessage(result), failed)
}

// PromoteScheduled moves due scheduled jobs to available.
func (b *PostgresBackend) PromoteScheduled(ctx context.Context) error {
	now := time.Now()
	tag, err := b.pool.Exec(ctx, `
		UPDATE ojs_jobs SET state = 'available', enqueued_at = $1
		WHERE state = 'scheduled' AND scheduled_at <= $1`, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		log.Printf("[scheduler] promoted %d scheduled jobs", tag.RowsAffected())
	}
	return nil
}

// PromoteRetries moves due retry jobs to available.
func (b *PostgresBackend) PromoteRetries(ctx context.Context) error {
	now := time.Now()
	tag, err := b.pool.Exec(ctx, `
		UPDATE ojs_jobs SET state = 'available', enqueued_at = $1
		WHERE state = 'retryable' AND scheduled_at <= $1`, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		log.Printf("[scheduler] promoted %d retry jobs", tag.RowsAffected())
	}
	return nil
}

// RequeueStalled requeues jobs past their visibility timeout.
func (b *PostgresBackend) RequeueStalled(ctx context.Context) error {
	now := time.Now()
	tag, err := b.pool.Exec(ctx, `
		UPDATE ojs_jobs SET
			state = 'available',
			started_at = NULL,
			worker_id = NULL,
			visibility_timeout = NULL,
			enqueued_at = $1
		WHERE state = 'active' AND visibility_timeout IS NOT NULL AND visibility_timeout < $1`, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		log.Printf("[scheduler] requeued %d stalled jobs", tag.RowsAffected())
	}
	return nil
}

// FireCronJobs checks cron schedules and fires due jobs.
func (b *PostgresBackend) FireCronJobs(ctx context.Context) error {
	// Try to acquire advisory lock for leader election
	var acquired bool
	err := b.pool.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext('ojs_cron_scheduler'))").Scan(&acquired)
	if err != nil || !acquired {
		return nil
	}
	defer b.pool.Exec(ctx, "SELECT pg_advisory_unlock(hashtext('ojs_cron_scheduler'))")

	now := time.Now()
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	rows, err := b.pool.Query(ctx, `
		SELECT name, expression, timezone, overlap_policy, job_template, queue, next_run_at, instance_job_id
		FROM ojs_cron_jobs
		WHERE enabled = true AND next_run_at <= $1`, now)
	if err != nil {
		return err
	}
	defer rows.Close()

	type cronEntry struct {
		name, expression, overlapPolicy, queue string
		timezone                               *string
		templateJSON                           []byte
		nextRunAt                              time.Time
		instanceJobID                          *string
	}

	var entries []cronEntry
	for rows.Next() {
		var e cronEntry
		if err := rows.Scan(&e.name, &e.expression, &e.timezone, &e.overlapPolicy,
			&e.templateJSON, &e.queue, &e.nextRunAt, &e.instanceJobID); err != nil {
			continue
		}
		entries = append(entries, e)
	}
	rows.Close()

	for _, e := range entries {
		// Check overlap policy
		if e.overlapPolicy == "skip" && e.instanceJobID != nil {
			var jobState string
			err := b.pool.QueryRow(ctx, "SELECT state FROM ojs_jobs WHERE id = $1", *e.instanceJobID).Scan(&jobState)
			if err == nil && !core.IsTerminalState(jobState) {
				// Update next_run_at even though we skip
				expr := e.expression
				schedule, err := parser.Parse(expr)
				if err == nil {
					_, _ = b.pool.Exec(ctx,
						"UPDATE ojs_cron_jobs SET last_run_at = $1, next_run_at = $2 WHERE name = $3",
						now, schedule.Next(now), e.name)
				}
				continue
			}
		}

		// Parse job template
		var tmpl core.CronJobTemplate
		if err := json.Unmarshal(e.templateJSON, &tmpl); err != nil {
			continue
		}

		queue := e.queue
		if queue == "" {
			queue = "default"
		}

		cronVisTimeout := 600000 // 10 minutes
		job := &core.Job{
			Type:                tmpl.Type,
			Args:                tmpl.Args,
			Queue:               queue,
			VisibilityTimeoutMs: &cronVisTimeout,
		}

		created, pushErr := b.Push(ctx, job)
		if pushErr != nil {
			continue
		}

		// Track instance for overlap
		instanceUpdate := ""
		if e.overlapPolicy == "skip" {
			instanceUpdate = ", instance_job_id = '" + created.ID + "'"
		}

		// Update next run time
		schedule, err := parser.Parse(e.expression)
		if err == nil {
			_, _ = b.pool.Exec(ctx,
				fmt.Sprintf("UPDATE ojs_cron_jobs SET last_run_at = $1, next_run_at = $2%s WHERE name = $3", instanceUpdate),
				now, schedule.Next(now), e.name)
		}
	}

	return nil
}

// PruneOldJobs removes old completed and discarded jobs.
func (b *PostgresBackend) PruneOldJobs(ctx context.Context) error {
	completedRetention := 7 * 24 * time.Hour  // 7 days
	discardedRetention := 30 * 24 * time.Hour // 30 days
	now := time.Now()

	_, _ = b.pool.Exec(ctx,
		"DELETE FROM ojs_jobs WHERE state = 'completed' AND completed_at < $1",
		now.Add(-completedRetention))

	_, _ = b.pool.Exec(ctx,
		"DELETE FROM ojs_jobs WHERE state = 'discarded' AND completed_at < $1",
		now.Add(-discardedRetention))

	return nil
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

func matchesPattern(s, pattern string) bool {
	re, err := regexp.Compile("^" + pattern + "$")
	if err == nil {
		return re.MatchString(s)
	}
	return s == pattern
}
