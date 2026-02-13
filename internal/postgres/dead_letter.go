package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// ListDeadLetter returns dead letter (discarded) jobs.
func (b *Backend) ListDeadLetter(ctx context.Context, limit, offset int) ([]*core.Job, int, error) {
	var total int
	err := b.pool.QueryRow(ctx, "SELECT COUNT(*) FROM ojs_jobs WHERE state = 'discarded'").Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("count dead letter jobs: %w", err)
	}

	query := fmt.Sprintf(`SELECT %s FROM ojs_jobs WHERE state = 'discarded'
		ORDER BY completed_at DESC LIMIT $1 OFFSET $2`, allJobColumns)
	rows, err := b.pool.Query(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("list dead letter jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*core.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan dead letter job: %w", err)
		}
		jobs = append(jobs, job)
	}

	return jobs, total, nil
}

// RetryDeadLetter retries a dead letter job.
func (b *Backend) RetryDeadLetter(ctx context.Context, jobID string) (*core.Job, error) {
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
func (b *Backend) DeleteDeadLetter(ctx context.Context, jobID string) error {
	tag, err := b.pool.Exec(ctx, "DELETE FROM ojs_jobs WHERE id = $1 AND state = 'discarded'", jobID)
	if err != nil {
		return fmt.Errorf("delete dead letter job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return core.NewNotFoundError("Dead letter job", jobID)
	}
	return nil
}
