package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type countingBackend struct {
	promoteScheduled atomic.Int32
	promoteRetries   atomic.Int32
	requeueStalled   atomic.Int32
	fireCronJobs     atomic.Int32
	pruneOldJobs     atomic.Int32
}

func (b *countingBackend) PromoteScheduled(context.Context) error {
	b.promoteScheduled.Add(1)
	return nil
}

func (b *countingBackend) PromoteRetries(context.Context) error {
	b.promoteRetries.Add(1)
	return nil
}

func (b *countingBackend) RequeueStalled(context.Context) error {
	b.requeueStalled.Add(1)
	return nil
}

func (b *countingBackend) FireCronJobs(context.Context) error {
	b.fireCronJobs.Add(1)
	return nil
}

func (b *countingBackend) PruneOldJobs(context.Context) error {
	b.pruneOldJobs.Add(1)
	return nil
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.PromoteInterval != 1*time.Second {
		t.Fatalf("PromoteInterval = %v, want 1s", cfg.PromoteInterval)
	}
	if cfg.RetryInterval != 200*time.Millisecond {
		t.Fatalf("RetryInterval = %v, want 200ms", cfg.RetryInterval)
	}
	if cfg.ReaperInterval != 500*time.Millisecond {
		t.Fatalf("ReaperInterval = %v, want 500ms", cfg.ReaperInterval)
	}
	if cfg.CronInterval != 10*time.Second {
		t.Fatalf("CronInterval = %v, want 10s", cfg.CronInterval)
	}
	if cfg.PrunerInterval != 60*time.Second {
		t.Fatalf("PrunerInterval = %v, want 60s", cfg.PrunerInterval)
	}
}

func TestStartRunsAllLoops(t *testing.T) {
	backend := &countingBackend{}
	s := New(backend, Config{
		PromoteInterval: 5 * time.Millisecond,
		RetryInterval:   5 * time.Millisecond,
		ReaperInterval:  5 * time.Millisecond,
		CronInterval:    5 * time.Millisecond,
		PrunerInterval:  5 * time.Millisecond,
	})

	s.Start()
	defer s.Stop()

	waitForCondition(t, 500*time.Millisecond, func() bool {
		return backend.promoteScheduled.Load() > 0 &&
			backend.promoteRetries.Load() > 0 &&
			backend.requeueStalled.Load() > 0 &&
			backend.fireCronJobs.Load() > 0 &&
			backend.pruneOldJobs.Load() > 0
	})
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatal("condition not met before timeout")
}
