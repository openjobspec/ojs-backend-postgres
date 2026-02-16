package server

import (
	"testing"
	"time"
)

func TestLoadConfig_Defaults(t *testing.T) {
	cfg := LoadConfig()

	if cfg.Port != "8080" {
		t.Errorf("Port = %q, want %q", cfg.Port, "8080")
	}
	if cfg.PoolMinConns != 2 {
		t.Errorf("PoolMinConns = %d, want 2", cfg.PoolMinConns)
	}
	if cfg.PoolMaxConns != 20 {
		t.Errorf("PoolMaxConns = %d, want 20", cfg.PoolMaxConns)
	}
	if cfg.ReadTimeout != 30*time.Second {
		t.Errorf("ReadTimeout = %v, want 30s", cfg.ReadTimeout)
	}
	if cfg.RetentionCompletedDays != 7 {
		t.Errorf("RetentionCompletedDays = %d, want 7", cfg.RetentionCompletedDays)
	}
	if cfg.RetentionDiscardedDays != 30 {
		t.Errorf("RetentionDiscardedDays = %d, want 30", cfg.RetentionDiscardedDays)
	}
	if cfg.DefaultVisibilityTimeoutMs != 30000 {
		t.Errorf("DefaultVisibilityTimeoutMs = %d, want 30000", cfg.DefaultVisibilityTimeoutMs)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	t.Setenv("OJS_PORT", "9090")
	t.Setenv("OJS_POOL_MAX_CONNS", "50")
	t.Setenv("OJS_READ_TIMEOUT", "10s")
	t.Setenv("OJS_RETENTION_COMPLETED_DAYS", "14")

	cfg := LoadConfig()

	if cfg.Port != "9090" {
		t.Errorf("Port = %q, want %q", cfg.Port, "9090")
	}
	if cfg.PoolMaxConns != 50 {
		t.Errorf("PoolMaxConns = %d, want 50", cfg.PoolMaxConns)
	}
	if cfg.ReadTimeout != 10*time.Second {
		t.Errorf("ReadTimeout = %v, want 10s", cfg.ReadTimeout)
	}
	if cfg.RetentionCompletedDays != 14 {
		t.Errorf("RetentionCompletedDays = %d, want 14", cfg.RetentionCompletedDays)
	}
}

func TestLoadConfig_InvalidEnvFallsBackToDefault(t *testing.T) {
	t.Setenv("OJS_POOL_MAX_CONNS", "not-a-number")
	t.Setenv("OJS_READ_TIMEOUT", "not-a-duration")

	cfg := LoadConfig()

	if cfg.PoolMaxConns != 20 {
		t.Errorf("PoolMaxConns = %d, want 20 (default on invalid)", cfg.PoolMaxConns)
	}
	if cfg.ReadTimeout != 30*time.Second {
		t.Errorf("ReadTimeout = %v, want 30s (default on invalid)", cfg.ReadTimeout)
	}
}
