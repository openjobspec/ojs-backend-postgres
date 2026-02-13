package core

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFormatTime(t *testing.T) {
	// Fixed time
	ts := time.Date(2025, 6, 15, 10, 30, 45, 123000000, time.UTC)
	got := FormatTime(ts)
	want := "2025-06-15T10:30:45.123Z"
	if got != want {
		t.Errorf("FormatTime() = %q, want %q", got, want)
	}
}

func TestFormatTime_NonUTC(t *testing.T) {
	loc := time.FixedZone("EST", -5*60*60)
	ts := time.Date(2025, 6, 15, 10, 30, 45, 0, loc)
	got := FormatTime(ts)
	// Should be converted to UTC
	want := "2025-06-15T15:30:45.000Z"
	if got != want {
		t.Errorf("FormatTime(non-UTC) = %q, want %q", got, want)
	}
}

func TestNowFormatted(t *testing.T) {
	result := NowFormatted()
	if result == "" {
		t.Fatal("NowFormatted() returned empty string")
	}
	// Should end with Z (UTC)
	if result[len(result)-1] != 'Z' {
		t.Errorf("NowFormatted() = %q, expected to end with Z", result)
	}
}

func TestJob_MarshalJSON(t *testing.T) {
	priority := 10
	job := Job{
		ID:       "test-id",
		Type:     "email.send",
		State:    StateAvailable,
		Queue:    "default",
		Args:     json.RawMessage(`["hello"]`),
		Priority: &priority,
		Attempt:  0,
	}

	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if m["id"] != "test-id" {
		t.Errorf("id = %v", m["id"])
	}
	if m["type"] != "email.send" {
		t.Errorf("type = %v", m["type"])
	}
	if m["state"] != "available" {
		t.Errorf("state = %v", m["state"])
	}
	p, ok := m["priority"].(float64)
	if !ok || p != 10 {
		t.Errorf("priority = %v", m["priority"])
	}
}

func TestJob_MarshalJSON_UnknownFields(t *testing.T) {
	job := Job{
		ID:    "test-id",
		Type:  "test",
		State: "available",
		Queue: "default",
		Args:  json.RawMessage(`[1]`),
		UnknownFields: map[string]json.RawMessage{
			"custom": json.RawMessage(`"preserved"`),
		},
	}

	data, err := json.Marshal(job)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if m["custom"] != "preserved" {
		t.Errorf("custom field not preserved, got %v", m["custom"])
	}
}

func TestJob_MarshalJSON_OmitsEmpty(t *testing.T) {
	job := Job{
		ID:    "test-id",
		Type:  "test",
		State: "available",
		Queue: "default",
		Args:  json.RawMessage(`[1]`),
	}

	data, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}

	// These should not be in output when empty
	for _, key := range []string{"meta", "priority", "result", "error", "tags", "errors"} {
		if _, ok := m[key]; ok {
			t.Errorf("expected %q to be omitted when empty", key)
		}
	}
}
