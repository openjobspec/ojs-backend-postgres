package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// RecordEvent persists a history event for a job.
func (b *Backend) RecordEvent(ctx context.Context, event *core.HistoryEvent) error {
	var actorType, actorID string
	if event.Actor != nil {
		actorType = event.Actor.Type
		actorID = event.Actor.ID
	}

	dataJSON, _ := json.Marshal(event.Data)
	metaJSON, _ := json.Marshal(event.Metadata)

	_, err := b.pool.Exec(ctx, `
		INSERT INTO ojs_job_history (id, job_id, event_type, timestamp, actor_type, actor_id, data, metadata)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT DO NOTHING`,
		event.ID, event.JobID, event.EventType, event.Timestamp,
		actorType, actorID, dataJSON, metaJSON,
	)
	if err != nil {
		return fmt.Errorf("insert history event: %w", err)
	}
	return nil
}

// GetJobHistory returns the execution history for a specific job.
func (b *Backend) GetJobHistory(ctx context.Context, jobID string, limit int, cursor string) (*core.HistoryPage, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}

	var rows interface{ Next() bool }
	var err error

	query := `
		SELECT id, job_id, event_type, timestamp, actor_type, actor_id, data, metadata
		FROM ojs_job_history
		WHERE job_id = $1`

	args := []any{jobID}
	argIdx := 2

	if cursor != "" {
		query += fmt.Sprintf(` AND id > $%d`, argIdx)
		args = append(args, cursor)
		argIdx++
	}

	query += ` ORDER BY timestamp ASC, id ASC`
	query += fmt.Sprintf(` LIMIT $%d`, argIdx)
	args = append(args, limit+1)

	pgRows, err := b.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query history: %w", err)
	}
	defer pgRows.Close()
	rows = pgRows

	events := make([]*core.HistoryEvent, 0, limit)
	var nextCursor string
	count := 0

	for pgRows.Next() {
		if count >= limit {
			var id string
			var dummy any
			pgRows.Scan(&id, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy, &dummy)
			nextCursor = id
			break
		}

		var (
			id, jobID2, eventType, timestamp string
			actorType, actorID               *string
			dataJSON, metaJSON               []byte
		)
		if err := pgRows.Scan(&id, &jobID2, &eventType, &timestamp, &actorType, &actorID, &dataJSON, &metaJSON); err != nil {
			continue
		}

		event := &core.HistoryEvent{
			ID:        id,
			JobID:     jobID2,
			EventType: eventType,
			Timestamp: timestamp,
		}
		if actorType != nil && *actorType != "" {
			aid := ""
			if actorID != nil {
				aid = *actorID
			}
			event.Actor = &core.HistoryActor{Type: *actorType, ID: aid}
		}
		if len(dataJSON) > 0 {
			json.Unmarshal(dataJSON, &event.Data)
		}
		if len(metaJSON) > 0 {
			json.Unmarshal(metaJSON, &event.Metadata)
		}
		events = append(events, event)
		count++
	}

	_ = rows

	// Get total count
	var total int
	b.pool.QueryRow(ctx, `SELECT COUNT(*) FROM ojs_job_history WHERE job_id = $1`, jobID).Scan(&total)

	return &core.HistoryPage{
		Events:     events,
		NextCursor: nextCursor,
		Total:      total,
	}, nil
}

// GetJobLineage returns the parent-child relationship tree for a job.
func (b *Backend) GetJobLineage(ctx context.Context, jobID string) (*core.JobLineage, error) {
	job, err := b.Info(ctx, jobID)
	if err != nil {
		return &core.JobLineage{JobID: jobID}, nil
	}

	lineage := &core.JobLineage{
		JobID:      job.ID,
		ParentID:   job.ParentID,
		WorkflowID: job.WorkflowID,
		RootID:     job.RootID,
	}

	// Find children using parent_id
	rows, err := b.pool.Query(ctx,
		`SELECT id FROM ojs_jobs WHERE parent_id = $1 LIMIT 100`, jobID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var childID string
			if rows.Scan(&childID) == nil {
				lineage.Children = append(lineage.Children, core.JobLineage{JobID: childID})
			}
		}
	}

	return lineage, nil
}

// PurgeJobHistory deletes all history events for a specific job.
func (b *Backend) PurgeJobHistory(ctx context.Context, jobID string) error {
	_, err := b.pool.Exec(ctx, `DELETE FROM ojs_job_history WHERE job_id = $1`, jobID)
	return err
}

// PurgeHistory deletes history events older than the given duration.
func (b *Backend) PurgeHistory(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := time.Now().Add(-olderThan)
	tag, err := b.pool.Exec(ctx, `DELETE FROM ojs_job_history WHERE timestamp < $1`, cutoff.UTC().Format(time.RFC3339))
	if err != nil {
		return 0, fmt.Errorf("purge history: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
