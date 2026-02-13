package core

import "fmt"

// Standard error codes used in OJS error responses.
const (
	ErrCodeInvalidRequest  = "invalid_request"
	ErrCodeValidationError = "validation_error"
	ErrCodeNotFound        = "not_found"
	ErrCodeConflict        = "conflict"
	ErrCodeDuplicate       = "duplicate"
	ErrCodeInternalError   = "internal_error"
	ErrCodeUnsupported     = "unsupported"
	ErrCodeQueuePaused     = "queue_paused"
)

// OJSError represents a structured error conforming to the OJS error format.
type OJSError struct {
	Code      string         `json:"code,omitempty"`
	Type      string         `json:"type,omitempty"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
}

func (e *OJSError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// NewInvalidRequestError creates an error for malformed or invalid requests.
func NewInvalidRequestError(message string, details map[string]any) *OJSError {
	return &OJSError{
		Code:      ErrCodeInvalidRequest,
		Message:   message,
		Retryable: false,
		Details:   details,
	}
}

// NewNotFoundError creates an error for missing resources.
func NewNotFoundError(resourceType, resourceID string) *OJSError {
	return &OJSError{
		Code:      ErrCodeNotFound,
		Message:   fmt.Sprintf("%s '%s' not found.", resourceType, resourceID),
		Retryable: false,
		Details: map[string]any{
			"resource_type": resourceType,
			"resource_id":   resourceID,
		},
	}
}

// NewConflictError creates an error for state conflicts.
func NewConflictError(message string, details map[string]any) *OJSError {
	return &OJSError{
		Code:      ErrCodeConflict,
		Message:   message,
		Retryable: false,
		Details:   details,
	}
}

// NewValidationError creates an error for field validation failures.
func NewValidationError(message string, details map[string]any) *OJSError {
	return &OJSError{
		Code:      ErrCodeValidationError,
		Type:      ErrCodeValidationError,
		Message:   message,
		Retryable: false,
		Details:   details,
	}
}

// NewInternalError creates an error for internal server failures.
func NewInternalError(message string) *OJSError {
	return &OJSError{
		Code:      ErrCodeInternalError,
		Message:   message,
		Retryable: true,
	}
}
