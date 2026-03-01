package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// SaveCheckpoint persists a checkpoint for a running job.
func (b *Backend) SaveCheckpoint(ctx context.Context, jobID string, state json.RawMessage) error {
	_, err := b.pool.Exec(ctx, `
		INSERT INTO ojs_checkpoints (job_id, state, sequence, created_at, updated_at)
		VALUES ($1, $2, 1, NOW(), NOW())
		ON CONFLICT (job_id) DO UPDATE
		SET state = $2, sequence = ojs_checkpoints.sequence + 1, updated_at = NOW()`,
		jobID, state,
	)
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// GetCheckpoint retrieves the last checkpoint for a job.
func (b *Backend) GetCheckpoint(ctx context.Context, jobID string) (*core.Checkpoint, error) {
	var cp core.Checkpoint
	var stateBytes []byte
	err := b.pool.QueryRow(ctx, `
		SELECT job_id, state, sequence, created_at
		FROM ojs_checkpoints WHERE job_id = $1`, jobID,
	).Scan(&cp.JobID, &stateBytes, &cp.Sequence, &cp.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("get checkpoint: %w", err)
	}
	cp.State = stateBytes
	return &cp, nil
}

// DeleteCheckpoint removes a checkpoint for a job (called on ACK).
func (b *Backend) DeleteCheckpoint(ctx context.Context, jobID string) error {
	_, err := b.pool.Exec(ctx, `DELETE FROM ojs_checkpoints WHERE job_id = $1`, jobID)
	return err
}
