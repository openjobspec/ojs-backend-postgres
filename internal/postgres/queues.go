package postgres

import (
	"context"
	"fmt"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// ListQueues returns all known queues.
func (b *Backend) ListQueues(ctx context.Context) ([]core.QueueInfo, error) {
	rows, err := b.pool.Query(ctx, "SELECT name, status FROM ojs_queues ORDER BY name")
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	defer rows.Close()

	var queues []core.QueueInfo
	for rows.Next() {
		var name, status string
		if err := rows.Scan(&name, &status); err != nil {
			return nil, fmt.Errorf("scan queue row: %w", err)
		}
		queues = append(queues, core.QueueInfo{
			Name:   name,
			Status: status,
		})
	}
	return queues, nil
}

// QueueStats returns statistics for a queue.
func (b *Backend) QueueStats(ctx context.Context, name string) (*core.QueueStats, error) {
	status := "active"
	var queueStatus *string
	var completedCount int64
	err := b.pool.QueryRow(ctx, "SELECT status, completed_count FROM ojs_queues WHERE name = $1", name).Scan(&queueStatus, &completedCount)
	if err == nil && queueStatus != nil {
		status = *queueStatus
	}

	var available, active, scheduled, retryable, dead int
	err = b.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE state = 'available'),
			COUNT(*) FILTER (WHERE state = 'active'),
			COUNT(*) FILTER (WHERE state = 'scheduled'),
			COUNT(*) FILTER (WHERE state = 'retryable'),
			COUNT(*) FILTER (WHERE state = 'discarded')
		FROM ojs_jobs WHERE queue = $1`, name).Scan(&available, &active, &scheduled, &retryable, &dead)
	if err != nil {
		return nil, fmt.Errorf("queue stats: %w", err)
	}

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
func (b *Backend) PauseQueue(ctx context.Context, name string) error {
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO ojs_queues (name, status) VALUES ($1, 'paused')
		ON CONFLICT (name) DO UPDATE SET status = 'paused'`, name); err != nil {
		return fmt.Errorf("pause queue: %w", err)
	}
	return nil
}

// ResumeQueue resumes a queue.
func (b *Backend) ResumeQueue(ctx context.Context, name string) error {
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO ojs_queues (name, status) VALUES ($1, 'active')
		ON CONFLICT (name) DO UPDATE SET status = 'active'`, name); err != nil {
		return fmt.Errorf("resume queue: %w", err)
	}
	return nil
}
