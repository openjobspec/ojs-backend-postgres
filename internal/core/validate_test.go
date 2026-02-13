package core

import (
	"encoding/json"
	"testing"
)

func TestValidateEnqueueRequest_Valid(t *testing.T) {
	req := &EnqueueRequest{
		Type: "email.send",
		Args: json.RawMessage(`["hello@example.com", "subject"]`),
	}

	if err := ValidateEnqueueRequest(req); err != nil {
		t.Fatalf("ValidateEnqueueRequest() unexpected error: %v", err)
	}
}

func TestValidateEnqueueRequest_TypeRequired(t *testing.T) {
	req := &EnqueueRequest{
		Type: "",
		Args: json.RawMessage(`["hello"]`),
	}

	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("ValidateEnqueueRequest() expected error for missing type")
	}
	if err.Code != ErrCodeInvalidRequest {
		t.Errorf("error code = %q, want %q", err.Code, ErrCodeInvalidRequest)
	}
}

func TestValidateEnqueueRequest_TypeFormat(t *testing.T) {
	tests := []struct {
		typ     string
		wantErr bool
	}{
		{"email.send", false},
		{"process-data", false},
		{"simple", false},
		{"multi.dotted.name", false},
		{"with-dashes", false},
		{"with_underscores", false},
		{"a1b2c3", false},

		// Invalid formats
		{"UPPERCASE", true},
		{"Invalid", true},
		{"123start", true},
		{"-start-dash", true},
		{"has space", true},
		{"special@char", true},
		{".starts-with-dot", true},
	}

	for _, tt := range tests {
		req := &EnqueueRequest{
			Type: tt.typ,
			Args: json.RawMessage(`[1]`),
		}
		err := ValidateEnqueueRequest(req)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateEnqueueRequest(type=%q) error = %v, wantErr = %v", tt.typ, err, tt.wantErr)
		}
	}
}

func TestValidateEnqueueRequest_ArgsRequired(t *testing.T) {
	req := &EnqueueRequest{
		Type: "test",
		Args: nil,
	}

	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for nil args")
	}
}

func TestValidateEnqueueRequest_ArgsNull(t *testing.T) {
	req := &EnqueueRequest{
		Type: "test",
		Args: json.RawMessage(`null`),
	}

	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for null args")
	}
}

func TestValidateEnqueueRequest_ArgsMustBeArray(t *testing.T) {
	req := &EnqueueRequest{
		Type: "test",
		Args: json.RawMessage(`{"key": "value"}`),
	}

	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for non-array args")
	}
}

func TestValidateEnqueueRequest_ArgsValidArray(t *testing.T) {
	tests := []struct {
		name string
		args string
	}{
		{"empty array", `[]`},
		{"string elements", `["a", "b"]`},
		{"number elements", `[1, 2, 3]`},
		{"mixed types", `["hello", 42, true, null, {"key": "val"}, [1,2]]`},
	}

	for _, tt := range tests {
		req := &EnqueueRequest{
			Type: "test",
			Args: json.RawMessage(tt.args),
		}
		if err := ValidateEnqueueRequest(req); err != nil {
			t.Errorf("%s: unexpected error: %v", tt.name, err)
		}
	}
}

func TestValidateEnqueueRequest_InvalidID(t *testing.T) {
	req := &EnqueueRequest{
		Type:  "test",
		Args:  json.RawMessage(`[1]`),
		ID:    "not-a-uuid",
		HasID: true,
	}

	err := ValidateEnqueueRequest(req)
	if err == nil {
		t.Fatal("expected error for invalid ID")
	}
}

func TestValidateEnqueueRequest_ValidID(t *testing.T) {
	req := &EnqueueRequest{
		Type:  "test",
		Args:  json.RawMessage(`[1]`),
		ID:    NewUUIDv7(),
		HasID: true,
	}

	if err := ValidateEnqueueRequest(req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateOptions_QueueName(t *testing.T) {
	tests := []struct {
		queue   string
		wantErr bool
	}{
		{"default", false},
		{"my-queue", false},
		{"queue.name", false},
		{"q1", false},
		{"0-queue", false},

		// Invalid
		{"UPPER", true},
		{"-start", true},
		{".start", true},
		{"has space", true},
		{"special@", true},
	}

	for _, tt := range tests {
		req := &EnqueueRequest{
			Type:    "test",
			Args:    json.RawMessage(`[1]`),
			Options: &EnqueueOptions{Queue: tt.queue},
		}
		err := ValidateEnqueueRequest(req)
		if (err != nil) != tt.wantErr {
			t.Errorf("queue=%q: error = %v, wantErr = %v", tt.queue, err, tt.wantErr)
		}
	}
}

func TestValidateOptions_Priority(t *testing.T) {
	tests := []struct {
		priority int
		wantErr  bool
	}{
		{0, false},
		{100, false},
		{-100, false},
		{50, false},
		{-50, false},
		{101, true},
		{-101, true},
		{1000, true},
	}

	for _, tt := range tests {
		p := tt.priority
		req := &EnqueueRequest{
			Type:    "test",
			Args:    json.RawMessage(`[1]`),
			Options: &EnqueueOptions{Priority: &p},
		}
		err := ValidateEnqueueRequest(req)
		if (err != nil) != tt.wantErr {
			t.Errorf("priority=%d: error = %v, wantErr = %v", tt.priority, err, tt.wantErr)
		}
	}
}

func TestValidateRetryPolicy(t *testing.T) {
	tests := []struct {
		name    string
		policy  RetryPolicy
		wantErr bool
	}{
		{
			name:    "valid policy",
			policy:  RetryPolicy{MaxAttempts: 3, InitialInterval: "PT1S", BackoffCoefficient: 2.0},
			wantErr: false,
		},
		{
			name:    "negative max_attempts",
			policy:  RetryPolicy{MaxAttempts: -1},
			wantErr: true,
		},
		{
			name:    "backoff_coefficient below 1",
			policy:  RetryPolicy{MaxAttempts: 3, BackoffCoefficient: 0.5},
			wantErr: true,
		},
		{
			name:    "invalid initial_interval",
			policy:  RetryPolicy{MaxAttempts: 3, InitialInterval: "invalid"},
			wantErr: true,
		},
		{
			name:    "invalid max_interval",
			policy:  RetryPolicy{MaxAttempts: 3, MaxInterval: "not-duration"},
			wantErr: true,
		},
		{
			name:    "zero max_attempts is valid",
			policy:  RetryPolicy{MaxAttempts: 0},
			wantErr: false,
		},
		{
			name:    "zero coefficient is valid (uses default)",
			policy:  RetryPolicy{MaxAttempts: 3, BackoffCoefficient: 0},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		rp := tt.policy
		req := &EnqueueRequest{
			Type:    "test",
			Args:    json.RawMessage(`[1]`),
			Options: &EnqueueOptions{Retry: &rp},
		}
		err := ValidateEnqueueRequest(req)
		if (err != nil) != tt.wantErr {
			t.Errorf("%s: error = %v, wantErr = %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestDetectJSONType(t *testing.T) {
	tests := []struct {
		input json.RawMessage
		want  string
	}{
		{json.RawMessage(`"hello"`), "string"},
		{json.RawMessage(`42`), "number"},
		{json.RawMessage(`true`), "boolean"},
		{json.RawMessage(`false`), "boolean"},
		{json.RawMessage(`null`), "null"},
		{json.RawMessage(`{"a":1}`), "object"},
		{json.RawMessage(`[1,2]`), "array"},
		{json.RawMessage(``), "empty"},
	}

	for _, tt := range tests {
		got := detectJSONType(tt.input)
		if got != tt.want {
			t.Errorf("detectJSONType(%s) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseEnqueueRequest(t *testing.T) {
	data := []byte(`{
		"type": "email.send",
		"args": ["hello@example.com"],
		"options": {"queue": "emails"},
		"custom_field": "preserved"
	}`)

	req, err := ParseEnqueueRequest(data)
	if err != nil {
		t.Fatalf("ParseEnqueueRequest() unexpected error: %v", err)
	}

	if req.Type != "email.send" {
		t.Errorf("Type = %q, want %q", req.Type, "email.send")
	}

	if req.Options == nil || req.Options.Queue != "emails" {
		t.Error("Options.Queue not parsed correctly")
	}

	if _, ok := req.UnknownFields["custom_field"]; !ok {
		t.Error("custom_field not preserved in UnknownFields")
	}
}

func TestParseEnqueueRequest_HasID(t *testing.T) {
	withID := []byte(`{"type": "test", "args": [1], "id": "01912345-6789-7abc-8def-0123456789ab"}`)
	req, err := ParseEnqueueRequest(withID)
	if err != nil {
		t.Fatal(err)
	}
	if !req.HasID {
		t.Error("HasID should be true when id is present")
	}

	withoutID := []byte(`{"type": "test", "args": [1]}`)
	req, err = ParseEnqueueRequest(withoutID)
	if err != nil {
		t.Fatal(err)
	}
	if req.HasID {
		t.Error("HasID should be false when id is absent")
	}
}
