package core

import (
	"testing"
	"time"
)

func TestParseISO8601Duration(t *testing.T) {
	tests := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"PT1S", 1 * time.Second, false},
		{"PT5S", 5 * time.Second, false},
		{"PT30S", 30 * time.Second, false},
		{"PT1M", 1 * time.Minute, false},
		{"PT5M", 5 * time.Minute, false},
		{"PT1H", 1 * time.Hour, false},
		{"PT1H30M", 1*time.Hour + 30*time.Minute, false},
		{"PT2H15M10S", 2*time.Hour + 15*time.Minute + 10*time.Second, false},
		{"PT0.5S", 500 * time.Millisecond, false},

		// Invalid inputs
		{"", 0, true},
		{"P1D", 0, true},
		{"1S", 0, true},
		{"PT", 0, true},
		{"hello", 0, true},
		{"PT0S", 0, true}, // zero duration
	}

	for _, tt := range tests {
		got, err := ParseISO8601Duration(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseISO8601Duration(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if !tt.wantErr && got != tt.want {
			t.Errorf("ParseISO8601Duration(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestFormatISO8601Duration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{0, "PT0S"},
		{1 * time.Second, "PT1S"},
		{30 * time.Second, "PT30S"},
		{1 * time.Minute, "PT1M"},
		{5 * time.Minute, "PT5M"},
		{1 * time.Hour, "PT1H"},
		{1*time.Hour + 30*time.Minute, "PT1H30M"},
		{2*time.Hour + 15*time.Minute + 10*time.Second, "PT2H15M10S"},
	}

	for _, tt := range tests {
		got := FormatISO8601Duration(tt.input)
		if got != tt.want {
			t.Errorf("FormatISO8601Duration(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseFormatRoundTrip(t *testing.T) {
	durations := []string{"PT1S", "PT5M", "PT1H", "PT1H30M", "PT2H15M10S"}
	for _, s := range durations {
		d, err := ParseISO8601Duration(s)
		if err != nil {
			t.Fatalf("ParseISO8601Duration(%q) unexpected error: %v", s, err)
		}
		got := FormatISO8601Duration(d)
		if got != s {
			t.Errorf("round-trip: ParseISO8601Duration(%q) -> FormatISO8601Duration = %q", s, got)
		}
	}
}
