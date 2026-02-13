package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// RegisterCron registers a cron job.
func (b *Backend) RegisterCron(ctx context.Context, cronJob *core.CronJob) (*core.CronJob, error) {
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
func (b *Backend) ListCron(ctx context.Context) ([]*core.CronJob, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT name, expression, timezone, overlap_policy, enabled, job_template,
			queue, created_at, next_run_at, last_run_at
		FROM ojs_cron_jobs ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list cron jobs: %w", err)
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
			return nil, fmt.Errorf("scan cron job: %w", err)
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
func (b *Backend) DeleteCron(ctx context.Context, name string) (*core.CronJob, error) {
	var (
		expression, overlapPolicy, queue string
		timezone                         *string
		enabled                          bool
		templateJSON                     []byte
		createdAt                        time.Time
		nextRunAt, lastRunAt             *time.Time
	)

	err := b.pool.QueryRow(ctx, `
		DELETE FROM ojs_cron_jobs WHERE name = $1
		RETURNING expression, timezone, overlap_policy, enabled, job_template, queue, created_at, next_run_at, last_run_at`,
		name).Scan(&expression, &timezone, &overlapPolicy, &enabled, &templateJSON, &queue, &createdAt, &nextRunAt, &lastRunAt)
	if err != nil {
		return nil, core.NewNotFoundError("Cron job", name)
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

	return cj, nil
}

// FireCronJobs checks cron schedules and fires due jobs.
func (b *Backend) FireCronJobs(ctx context.Context) error {
	// Try to acquire advisory lock for leader election
	var acquired bool
	err := b.pool.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext('ojs_cron_scheduler'))").Scan(&acquired)
	if err != nil || !acquired {
		return nil
	}
	defer func() { _, _ = b.pool.Exec(ctx, "SELECT pg_advisory_unlock(hashtext('ojs_cron_scheduler'))") }()

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
					b.logExec(ctx, "cron-schedule",
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

		// Update next run time and optionally track instance for overlap
		schedule, err := parser.Parse(e.expression)
		if err == nil {
			if e.overlapPolicy == "skip" {
				b.logExec(ctx, "cron-schedule",
					"UPDATE ojs_cron_jobs SET last_run_at = $1, next_run_at = $2, instance_job_id = $3 WHERE name = $4",
					now, schedule.Next(now), created.ID, e.name)
			} else {
				b.logExec(ctx, "cron-schedule",
					"UPDATE ojs_cron_jobs SET last_run_at = $1, next_run_at = $2 WHERE name = $3",
					now, schedule.Next(now), e.name)
			}
		}
	}

	return nil
}
