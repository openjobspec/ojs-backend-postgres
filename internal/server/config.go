package server

import (
	"time"

	commonconfig "github.com/openjobspec/ojs-go-backend-common/config"
)

// Config holds server configuration from environment variables.
type Config struct {
	commonconfig.BaseConfig
	DatabaseURL string

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
}

// LoadConfig reads configuration from environment variables with defaults.
func LoadConfig() Config {
	return Config{
		BaseConfig:  commonconfig.LoadBaseConfig(),
		DatabaseURL: commonconfig.GetEnv("DATABASE_URL", ""),

		PoolMinConns: commonconfig.GetEnvInt32("OJS_POOL_MIN_CONNS", 2),
		PoolMaxConns: commonconfig.GetEnvInt32("OJS_POOL_MAX_CONNS", 20),

		SchedulerPromoteInterval: commonconfig.GetDurationEnv("OJS_SCHEDULER_PROMOTE_INTERVAL", 1*time.Second),
		SchedulerRetryInterval:   commonconfig.GetDurationEnv("OJS_SCHEDULER_RETRY_INTERVAL", 200*time.Millisecond),
		SchedulerReaperInterval:  commonconfig.GetDurationEnv("OJS_SCHEDULER_REAPER_INTERVAL", 500*time.Millisecond),
		SchedulerCronInterval:    commonconfig.GetDurationEnv("OJS_SCHEDULER_CRON_INTERVAL", 10*time.Second),
		SchedulerPrunerInterval:  commonconfig.GetDurationEnv("OJS_SCHEDULER_PRUNER_INTERVAL", 60*time.Second),

		RetentionCompletedDays: commonconfig.GetEnvInt("OJS_RETENTION_COMPLETED_DAYS", 7),
		RetentionDiscardedDays: commonconfig.GetEnvInt("OJS_RETENTION_DISCARDED_DAYS", 30),

		DefaultVisibilityTimeoutMs: commonconfig.GetEnvInt("OJS_DEFAULT_VISIBILITY_TIMEOUT_MS", 30000),
	}
}
