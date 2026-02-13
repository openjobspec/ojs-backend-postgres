package core

import (
	"testing"
	"time"
)

func TestCalculateBackoff_Exponential(t *testing.T) {
	policy := &RetryPolicy{
		MaxAttempts:        5,
		InitialInterval:    "PT1S",
		BackoffCoefficient: 2.0,
		BackoffType:        "exponential",
		Jitter:             false,
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},  // 1s * 2^0
		{2, 2 * time.Second},  // 1s * 2^1
		{3, 4 * time.Second},  // 1s * 2^2
		{4, 8 * time.Second},  // 1s * 2^3
		{5, 16 * time.Second}, // 1s * 2^4
	}

	for _, tt := range tests {
		got := CalculateBackoff(policy, tt.attempt)
		if got != tt.want {
			t.Errorf("CalculateBackoff(exponential, attempt=%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestCalculateBackoff_Linear(t *testing.T) {
	policy := &RetryPolicy{
		MaxAttempts:     5,
		InitialInterval: "PT1S",
		BackoffType:     "linear",
		Jitter:          false,
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 3 * time.Second},
		{4, 4 * time.Second},
	}

	for _, tt := range tests {
		got := CalculateBackoff(policy, tt.attempt)
		if got != tt.want {
			t.Errorf("CalculateBackoff(linear, attempt=%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestCalculateBackoff_Constant(t *testing.T) {
	policy := &RetryPolicy{
		MaxAttempts:     5,
		InitialInterval: "PT5S",
		BackoffType:     "constant",
		Jitter:          false,
	}

	for attempt := 1; attempt <= 5; attempt++ {
		got := CalculateBackoff(policy, attempt)
		if got != 5*time.Second {
			t.Errorf("CalculateBackoff(constant, attempt=%d) = %v, want %v", attempt, got, 5*time.Second)
		}
	}
}

func TestCalculateBackoff_MaxInterval(t *testing.T) {
	policy := &RetryPolicy{
		MaxAttempts:        10,
		InitialInterval:    "PT1S",
		BackoffCoefficient: 2.0,
		MaxInterval:        "PT10S",
		Jitter:             false,
	}

	got := CalculateBackoff(policy, 5) // 1s * 2^4 = 16s, capped to 10s
	if got != 10*time.Second {
		t.Errorf("CalculateBackoff with max_interval, attempt=5: got %v, want %v", got, 10*time.Second)
	}
}

func TestCalculateBackoff_Jitter(t *testing.T) {
	policy := &RetryPolicy{
		MaxAttempts:        5,
		InitialInterval:    "PT1S",
		BackoffCoefficient: 2.0,
		Jitter:             true,
	}

	// With jitter enabled, result should be between 0.5x and 1.5x the base delay
	for i := 0; i < 50; i++ {
		got := CalculateBackoff(policy, 1) // base = 1s
		if got < 500*time.Millisecond || got > 1500*time.Millisecond {
			t.Errorf("CalculateBackoff with jitter, attempt=1: got %v, expected between 500ms and 1500ms", got)
		}
	}
}

func TestCalculateBackoff_NilPolicy(t *testing.T) {
	// Should use default policy without panicking
	got := CalculateBackoff(nil, 1)
	if got <= 0 {
		t.Errorf("CalculateBackoff(nil, 1) = %v, expected positive duration", got)
	}
}

func TestCalculateBackoff_BackoffStrategy(t *testing.T) {
	// Test BackoffStrategy alias
	policy := &RetryPolicy{
		MaxAttempts:     5,
		InitialInterval: "PT2S",
		BackoffStrategy: "constant",
		Jitter:          false,
	}

	got := CalculateBackoff(policy, 3)
	if got != 2*time.Second {
		t.Errorf("CalculateBackoff with backoff_strategy=constant: got %v, want 2s", got)
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	if p.MaxAttempts != 3 {
		t.Errorf("DefaultRetryPolicy().MaxAttempts = %d, want 3", p.MaxAttempts)
	}
	if p.InitialInterval != "PT1S" {
		t.Errorf("DefaultRetryPolicy().InitialInterval = %q, want PT1S", p.InitialInterval)
	}
	if p.BackoffCoefficient != 2.0 {
		t.Errorf("DefaultRetryPolicy().BackoffCoefficient = %f, want 2.0", p.BackoffCoefficient)
	}
	if p.MaxInterval != "PT5M" {
		t.Errorf("DefaultRetryPolicy().MaxInterval = %q, want PT5M", p.MaxInterval)
	}
	if !p.Jitter {
		t.Error("DefaultRetryPolicy().Jitter = false, want true")
	}
}
