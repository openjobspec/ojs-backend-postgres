package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
	"github.com/openjobspec/ojs-backend-postgres/internal/metrics"
)

// Fetch claims jobs from the specified queues.
func (b *Backend) Fetch(ctx context.Context, queues []string, count int, workerID string, visibilityTimeoutMs int) ([]*core.Job, error) {
	fetchStart := time.Now()
	defer func() {
		metrics.FetchDuration.Observe(time.Since(fetchStart).Seconds())
	}()

	now := fetchStart
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
			effectiveVisTimeout = b.config.DefaultVisibilityTimeoutMs
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
				b.logExec(ctx, "visibility-update", "UPDATE ojs_jobs SET visibility_timeout = $1 WHERE id = $2", jobVisDeadline, job.ID)
			}

			allJobs = append(allJobs, job)
		}
		rows.Close()

		// Discard expired jobs that were skipped
		b.logExec(ctx, "discard-expired", `
			UPDATE ojs_jobs SET state = 'discarded'
			WHERE queue = $1 AND state = 'available' AND expires_at IS NOT NULL AND expires_at <= $2`,
			queue, now)
	}

	for _, job := range allJobs {
		metrics.JobsFetched.WithLabelValues(job.Queue).Inc()
		metrics.ActiveJobs.WithLabelValues(job.Queue).Inc()
	}

	return allJobs, nil
}
