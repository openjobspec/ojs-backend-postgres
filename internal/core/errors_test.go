package core

import "testing"

func TestOJSError_Error(t *testing.T) {
	err := &OJSError{Code: "not_found", Message: "Job 'abc' not found."}
	got := err.Error()
	want := "[not_found] Job 'abc' not found."
	if got != want {
		t.Errorf("OJSError.Error() = %q, want %q", got, want)
	}
}

func TestNewInvalidRequestError(t *testing.T) {
	err := NewInvalidRequestError("bad field", map[string]any{"field": "type"})
	if err.Code != ErrCodeInvalidRequest {
		t.Errorf("Code = %q, want %q", err.Code, ErrCodeInvalidRequest)
	}
	if err.Retryable {
		t.Error("InvalidRequestError should not be retryable")
	}
	if err.Details["field"] != "type" {
		t.Error("Details not set correctly")
	}
}

func TestNewNotFoundError(t *testing.T) {
	err := NewNotFoundError("Job", "abc-123")
	if err.Code != ErrCodeNotFound {
		t.Errorf("Code = %q, want %q", err.Code, ErrCodeNotFound)
	}
	if err.Message != "Job 'abc-123' not found." {
		t.Errorf("Message = %q", err.Message)
	}
	if err.Details["resource_type"] != "Job" {
		t.Error("resource_type not set")
	}
}

func TestNewConflictError(t *testing.T) {
	err := NewConflictError("state conflict", nil)
	if err.Code != ErrCodeConflict {
		t.Errorf("Code = %q, want %q", err.Code, ErrCodeConflict)
	}
	if err.Retryable {
		t.Error("ConflictError should not be retryable")
	}
}

func TestNewValidationError(t *testing.T) {
	err := NewValidationError("bad value", map[string]any{"field": "priority"})
	if err.Code != ErrCodeValidationError {
		t.Errorf("Code = %q, want %q", err.Code, ErrCodeValidationError)
	}
	if err.Type != ErrCodeValidationError {
		t.Errorf("Type = %q, want %q", err.Type, ErrCodeValidationError)
	}
}

func TestNewInternalError(t *testing.T) {
	err := NewInternalError("something broke")
	if err.Code != ErrCodeInternalError {
		t.Errorf("Code = %q, want %q", err.Code, ErrCodeInternalError)
	}
	if !err.Retryable {
		t.Error("InternalError should be retryable")
	}
}
