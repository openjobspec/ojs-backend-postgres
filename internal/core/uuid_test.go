package core

import (
	"regexp"
	"testing"
)

func TestNewUUIDv7(t *testing.T) {
	id := NewUUIDv7()
	if id == "" {
		t.Fatal("NewUUIDv7() returned empty string")
	}

	// Should match UUID format
	uuidPattern := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	if !uuidPattern.MatchString(id) {
		t.Errorf("NewUUIDv7() = %q, does not match UUID format", id)
	}

	// Should be v7 (4th section starts with 7)
	if !IsValidUUIDv7(id) {
		t.Errorf("NewUUIDv7() = %q, not a valid UUIDv7", id)
	}
}

func TestNewUUIDv7_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := NewUUIDv7()
		if seen[id] {
			t.Fatalf("NewUUIDv7() produced duplicate ID: %s", id)
		}
		seen[id] = true
	}
}

func TestIsValidUUIDv7(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{NewUUIDv7(), true},
		{"01912345-6789-7abc-8def-0123456789ab", true},
		// v4 UUID (4th section starts with 4)
		{"550e8400-e29b-41d4-a716-446655440000", false},
		// Invalid format
		{"not-a-uuid", false},
		{"", false},
		// Wrong variant (4th section must start with 8, 9, a, or b)
		{"01912345-6789-7abc-0def-0123456789ab", false},
	}

	for _, tt := range tests {
		got := IsValidUUIDv7(tt.input)
		if got != tt.want {
			t.Errorf("IsValidUUIDv7(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsValidUUID(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{NewUUIDv7(), true},
		{"550e8400-e29b-41d4-a716-446655440000", true},
		{"not-a-uuid", false},
		{"", false},
	}

	for _, tt := range tests {
		got := IsValidUUID(tt.input)
		if got != tt.want {
			t.Errorf("IsValidUUID(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
