package server

import (
	"math"
	"os"
	"strconv"
	"time"
)

// Config holds server configuration from environment variables.
type Config struct {
	Port        string
	DatabaseURL string

	// HTTP server timeouts
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration

	// Connection pool
	PoolMinConns int32
	PoolMaxConns int32

	// Scheduler intervals
	SchedulerPromoteInterval time.Duration
	SchedulerRetryInterval   time.Duration
	SchedulerReaperInterval  time.Duration
	SchedulerCronInterval    time.Duration
	SchedulerPrunerInterval  time.Duration

	// Retention periods
	RetentionCompletedDays int
	RetentionDiscardedDays int

	// Default visibility timeout (ms)
	DefaultVisibilityTimeoutMs int

	// Shutdown drain timeout
	ShutdownTimeout time.Duration

	// Authentication (optional)
	APIKey string
}

// LoadConfig reads configuration from environment variables with defaults.
func LoadConfig() Config {
	return Config{
		Port:        getEnv("OJS_PORT", "8080"),
		DatabaseURL: getEnv("DATABASE_URL", "postgres://localhost:5432/ojs?sslmode=disable"),

		ReadTimeout:  getDurationEnv("OJS_READ_TIMEOUT", 30*time.Second),
		WriteTimeout: getDurationEnv("OJS_WRITE_TIMEOUT", 30*time.Second),
		IdleTimeout:  getDurationEnv("OJS_IDLE_TIMEOUT", 120*time.Second),

		PoolMinConns: getInt32Env("OJS_POOL_MIN_CONNS", 2),
		PoolMaxConns: getInt32Env("OJS_POOL_MAX_CONNS", 20),

		SchedulerPromoteInterval: getDurationEnv("OJS_SCHEDULER_PROMOTE_INTERVAL", 1*time.Second),
		SchedulerRetryInterval:   getDurationEnv("OJS_SCHEDULER_RETRY_INTERVAL", 200*time.Millisecond),
		SchedulerReaperInterval:  getDurationEnv("OJS_SCHEDULER_REAPER_INTERVAL", 500*time.Millisecond),
		SchedulerCronInterval:    getDurationEnv("OJS_SCHEDULER_CRON_INTERVAL", 10*time.Second),
		SchedulerPrunerInterval:  getDurationEnv("OJS_SCHEDULER_PRUNER_INTERVAL", 60*time.Second),

		RetentionCompletedDays: getIntEnv("OJS_RETENTION_COMPLETED_DAYS", 7),
		RetentionDiscardedDays: getIntEnv("OJS_RETENTION_DISCARDED_DAYS", 30),

		DefaultVisibilityTimeoutMs: getIntEnv("OJS_DEFAULT_VISIBILITY_TIMEOUT_MS", 30000),

		ShutdownTimeout: getDurationEnv("OJS_SHUTDOWN_TIMEOUT", 30*time.Second),

		APIKey: getEnv("OJS_API_KEY", ""),
	}
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func getIntEnv(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(val); err == nil {
			return n
		}
	}
	return defaultVal
}

func getInt32Env(key string, defaultVal int32) int32 {
	if val, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(val); err == nil && n >= 0 && n <= math.MaxInt32 {
			return int32(n) //nolint:gosec // bounds checked above
		}
	}
	return defaultVal
}

func getDurationEnv(key string, defaultVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
