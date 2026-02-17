package postgres

import (
	"context"
	"fmt"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// ListJobs returns a paginated, filtered list of jobs.
func (b *Backend) ListJobs(ctx context.Context, filters core.JobListFilters, limit, offset int) ([]*core.Job, int, error) {
	query := fmt.Sprintf("SELECT %s FROM ojs_jobs WHERE 1=1", allJobColumns)
	countQuery := "SELECT COUNT(*) FROM ojs_jobs WHERE 1=1"
	var args []any
	argIdx := 1

	if filters.State != "" {
		query += fmt.Sprintf(" AND state = $%d", argIdx)
		countQuery += fmt.Sprintf(" AND state = $%d", argIdx)
		args = append(args, filters.State)
		argIdx++
	}
	if filters.Queue != "" {
		query += fmt.Sprintf(" AND queue = $%d", argIdx)
		countQuery += fmt.Sprintf(" AND queue = $%d", argIdx)
		args = append(args, filters.Queue)
		argIdx++
	}
	if filters.Type != "" {
		query += fmt.Sprintf(" AND type = $%d", argIdx)
		countQuery += fmt.Sprintf(" AND type = $%d", argIdx)
		args = append(args, filters.Type)
		argIdx++
	}

	var total int
	if err := b.pool.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count jobs: %w", err)
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, limit, offset)

	rows, err := b.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*core.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, job)
	}

	return jobs, total, nil
}

// ListWorkers returns a paginated list of known workers and summary counts.
func (b *Backend) ListWorkers(ctx context.Context, limit, offset int) ([]*core.WorkerInfo, core.WorkerSummary, error) {
	rows, err := b.pool.Query(ctx,
		"SELECT worker_id, directive, active_jobs_count, last_heartbeat FROM ojs_workers ORDER BY last_heartbeat DESC LIMIT $1 OFFSET $2",
		limit, offset,
	)
	if err != nil {
		return nil, core.WorkerSummary{}, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()

	var workers []*core.WorkerInfo
	for rows.Next() {
		w := &core.WorkerInfo{}
		var lastHeartbeat *string
		if err := rows.Scan(&w.ID, &w.Directive, &w.ActiveJobs, &lastHeartbeat); err != nil {
			return nil, core.WorkerSummary{}, fmt.Errorf("scan worker: %w", err)
		}
		if lastHeartbeat != nil {
			w.LastHeartbeat = *lastHeartbeat
		}
		// Derive state from directive
		switch w.Directive {
		case "quiet":
			w.State = "quiet"
		default:
			w.State = "running"
		}
		workers = append(workers, w)
	}

	var summary core.WorkerSummary
	// Get total count (not just the page)
	if err := b.pool.QueryRow(ctx, "SELECT COUNT(*) FROM ojs_workers").Scan(&summary.Total); err != nil {
		summary.Total = len(workers)
	}
	for _, w := range workers {
		switch w.State {
		case "running":
			summary.Running++
		case "quiet":
			summary.Quiet++
		default:
			summary.Stale++
		}
	}

	return workers, summary, nil
}
