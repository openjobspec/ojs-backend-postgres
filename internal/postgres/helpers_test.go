package postgres

import (
	"encoding/json"
	"testing"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

func TestComputeFingerprint_DefaultKeysIgnoreQueue(t *testing.T) {
	jobA := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`["user@example.com"]`),
		Queue:  "priority",
		Unique: &core.UniquePolicy{},
	}
	jobB := &core.Job{
		Type:   "email.send",
		Args:   json.RawMessage(`["user@example.com"]`),
		Queue:  "default",
		Unique: &core.UniquePolicy{},
	}

	fpA := computeFingerprint(jobA)
	fpB := computeFingerprint(jobB)
	if fpA != fpB {
		t.Fatalf("fingerprints differ for default keys: %q != %q", fpA, fpB)
	}
}

func TestComputeFingerprint_CustomKeysIncludeQueueAndSortKeys(t *testing.T) {
	jobA := &core.Job{
		Type:  "email.send",
		Args:  json.RawMessage(`["user@example.com"]`),
		Queue: "priority",
		Unique: &core.UniquePolicy{
			Keys: []string{"queue", "type"},
		},
	}
	jobB := &core.Job{
		Type:  "email.send",
		Args:  json.RawMessage(`["user@example.com"]`),
		Queue: "default",
		Unique: &core.UniquePolicy{
			Keys: []string{"type", "queue"},
		},
	}

	fpA := computeFingerprint(jobA)
	fpB := computeFingerprint(jobB)
	if fpA == fpB {
		t.Fatal("fingerprints should differ when queue is part of unique keys")
	}
}

func TestMatchesPattern_RegexAndInvalidFallback(t *testing.T) {
	if !matchesPattern("network.timeout", "network\\..*") {
		t.Fatal("expected regex pattern to match")
	}

	if matchesPattern("abc", "[") {
		t.Fatal("invalid regex should fall back to exact match and return false")
	}
	if !matchesPattern("[", "[") {
		t.Fatal("invalid regex should fall back to exact match and return true")
	}
}

func TestWorkflowStepToJob_MapsWorkflowFieldsAndRetry(t *testing.T) {
	retry := &core.RetryPolicy{MaxAttempts: 4}
	step := core.WorkflowJobRequest{
		Type: "report.generate",
		Args: json.RawMessage(`[42]`),
		Options: &core.EnqueueOptions{
			Queue:       "reports",
			RetryPolicy: retry,
		},
	}

	b := &Backend{}
	job := b.workflowStepToJob(step, "wf-123", 2)

	if job.Queue != "reports" {
		t.Fatalf("Queue = %q, want %q", job.Queue, "reports")
	}
	if job.WorkflowID != "wf-123" {
		t.Fatalf("WorkflowID = %q, want %q", job.WorkflowID, "wf-123")
	}
	if job.WorkflowStep != 2 {
		t.Fatalf("WorkflowStep = %d, want 2", job.WorkflowStep)
	}
	if job.Retry == nil || job.Retry.MaxAttempts != 4 {
		t.Fatal("RetryPolicy was not mapped correctly")
	}
	if job.MaxAttempts == nil || *job.MaxAttempts != 4 {
		t.Fatal("MaxAttempts was not mapped from retry policy")
	}
}
