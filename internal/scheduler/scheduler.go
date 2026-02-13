package scheduler

import (
	"context"
	"log/slog"
	"time"
)

// Backend defines the operations the scheduler needs from the backend.
type Backend interface {
	PromoteScheduled(ctx context.Context) error
	PromoteRetries(ctx context.Context) error
	RequeueStalled(ctx context.Context) error
	FireCronJobs(ctx context.Context) error
	PruneOldJobs(ctx context.Context) error
}

// Config holds configurable scheduler intervals.
type Config struct {
	PromoteInterval time.Duration
	RetryInterval   time.Duration
	ReaperInterval  time.Duration
	CronInterval    time.Duration
	PrunerInterval  time.Duration
}

// DefaultConfig returns scheduler config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		PromoteInterval: 1 * time.Second,
		RetryInterval:   200 * time.Millisecond,
		ReaperInterval:  500 * time.Millisecond,
		CronInterval:    10 * time.Second,
		PrunerInterval:  60 * time.Second,
	}
}

// Scheduler runs background tasks for the OJS server.
type Scheduler struct {
	backend Backend
	config  Config
	stop    chan struct{}
}

// New creates a new Scheduler with the given config.
func New(backend Backend, cfg Config) *Scheduler {
	return &Scheduler{
		backend: backend,
		config:  cfg,
		stop:    make(chan struct{}),
	}
}

// Start begins all background scheduling goroutines.
func (s *Scheduler) Start() {
	go s.runLoop("scheduled-promoter", s.config.PromoteInterval, s.backend.PromoteScheduled)
	go s.runLoop("retry-promoter", s.config.RetryInterval, s.backend.PromoteRetries)
	go s.runLoop("stalled-reaper", s.config.ReaperInterval, s.backend.RequeueStalled)
	go s.runLoop("cron-scheduler", s.config.CronInterval, s.backend.FireCronJobs)
	go s.runLoop("job-pruner", s.config.PrunerInterval, s.backend.PruneOldJobs)
}

// Stop signals all background goroutines to stop.
func (s *Scheduler) Stop() {
	close(s.stop)
}

func (s *Scheduler) runLoop(name string, interval time.Duration, fn func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := fn(ctx); err != nil {
				slog.Error("scheduler loop error", "loop", name, "error", err)
			}
			cancel()
		}
	}
}
