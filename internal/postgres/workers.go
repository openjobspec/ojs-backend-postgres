package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// Heartbeat extends visibility timeout and reports worker state.
func (b *Backend) Heartbeat(ctx context.Context, workerID string, activeJobs []string, visibilityTimeoutMs int) (*core.HeartbeatResponse, error) {
	now := time.Now()

	// Upsert worker
	b.logExec(ctx, "heartbeat", `
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

// SetWorkerState sets a directive for a worker.
func (b *Backend) SetWorkerState(ctx context.Context, workerID string, state string) error {
	if _, err := b.pool.Exec(ctx, `
		INSERT INTO ojs_workers (worker_id, directive) VALUES ($1, $2)
		ON CONFLICT (worker_id) DO UPDATE SET directive = $2`,
		workerID, state); err != nil {
		return fmt.Errorf("set worker state: %w", err)
	}
	return nil
}
