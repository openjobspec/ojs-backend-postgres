package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// PromoteScheduled moves due scheduled jobs to available.
func (b *Backend) PromoteScheduled(ctx context.Context) error {
	now := time.Now()
	tag, err := b.pool.Exec(ctx, `
		UPDATE ojs_jobs SET state = 'available', enqueued_at = $1
		WHERE state = 'scheduled' AND scheduled_at <= $1`, now)
	if err != nil {
		return fmt.Errorf("promote scheduled: %w", err)
	}
	if tag.RowsAffected() > 0 {
		slog.Info("promoted scheduled jobs", "count", tag.RowsAffected())
	}
	return nil
}

// PromoteRetries moves due retry jobs to available.
func (b *Backend) PromoteRetries(ctx context.Context) error {
	now := time.Now()
	tag, err := b.pool.Exec(ctx, `
		UPDATE ojs_jobs SET state = 'available', enqueued_at = $1
		WHERE state = 'retryable' AND scheduled_at <= $1`, now)
	if err != nil {
		return fmt.Errorf("promote retries: %w", err)
	}
	if tag.RowsAffected() > 0 {
		slog.Info("promoted retry jobs", "count", tag.RowsAffected())
	}
	return nil
}

// RequeueStalled requeues jobs past their visibility timeout.
func (b *Backend) RequeueStalled(ctx context.Context) error {
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
		return fmt.Errorf("requeue stalled: %w", err)
	}
	if tag.RowsAffected() > 0 {
		slog.Info("requeued stalled jobs", "count", tag.RowsAffected())
	}
	return nil
}

// PruneOldJobs removes old completed and discarded jobs.
func (b *Backend) PruneOldJobs(ctx context.Context) error {
	completedRetention := time.Duration(b.config.RetentionCompletedDays) * 24 * time.Hour
	discardedRetention := time.Duration(b.config.RetentionDiscardedDays) * 24 * time.Hour
	now := time.Now()

	b.logExec(ctx, "pruner",
		"DELETE FROM ojs_jobs WHERE state = 'completed' AND completed_at < $1",
		now.Add(-completedRetention))

	b.logExec(ctx, "pruner",
		"DELETE FROM ojs_jobs WHERE state = 'discarded' AND completed_at < $1",
		now.Add(-discardedRetention))

	return nil
}
