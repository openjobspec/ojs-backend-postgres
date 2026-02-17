package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
)

// Backend implements core.Backend using PostgreSQL.
type Backend struct {
	pool      *pgxpool.Pool
	startTime time.Time
	cancel    context.CancelFunc
	config    BackendConfig
	pubsub    *pubSub
}

// logExec executes a query and logs any error without failing the caller.
// Use this for non-critical side-effect queries where the primary operation has already succeeded.
// Returns the error so callers can optionally handle it.
func (b *Backend) logExec(ctx context.Context, label, sql string, args ...any) error {
	if _, err := b.pool.Exec(ctx, sql, args...); err != nil {
		slog.Error("side-effect query failed", "operation", label, "error", err)
		return err
	}
	return nil
}

// BackendConfig holds configurable backend parameters.
type BackendConfig struct {
	PoolMinConns               int32
	PoolMaxConns               int32
	DefaultVisibilityTimeoutMs int
	RetentionCompletedDays     int
	RetentionDiscardedDays     int
}

// DefaultBackendConfig returns configuration with sensible defaults.
func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		PoolMinConns:               2,
		PoolMaxConns:               20,
		DefaultVisibilityTimeoutMs: 30000,
		RetentionCompletedDays:     7,
		RetentionDiscardedDays:     30,
	}
}

// New creates a new Backend, runs migrations, and starts LISTEN/NOTIFY.
func New(databaseURL string, opts ...func(*BackendConfig)) (*Backend, error) {
	cfg := DefaultBackendConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing database URL: %w", err)
	}

	config.MinConns = cfg.PoolMinConns
	config.MaxConns = cfg.PoolMaxConns

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}

	// Run migrations
	if err := RunMigrations(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("running migrations: %w", err)
	}

	listenCtx, cancel := context.WithCancel(ctx)

	b := &Backend{
		pool:      pool,
		startTime: time.Now(),
		cancel:    cancel,
		config:    cfg,
		pubsub:    newPubSub(),
	}

	// Start LISTEN/NOTIFY listener, broadcasting to SSE subscribers
	listenForNotifications(listenCtx, pool, func(queue string) {
		b.pubsub.publish(queue)
	})

	return b, nil
}

// Close stops the LISTEN/NOTIFY goroutine and closes the connection pool.
func (b *Backend) Close() error {
	b.cancel()
	b.pool.Close()
	return nil
}

// Health returns the health status.
func (b *Backend) Health(ctx context.Context) (*core.HealthResponse, error) {
	start := time.Now()
	err := b.pool.Ping(ctx)
	latency := time.Since(start).Milliseconds()

	resp := &core.HealthResponse{
		Version:       core.OJSVersion,
		UptimeSeconds: int64(time.Since(b.startTime).Seconds()),
	}

	if err != nil {
		resp.Status = "degraded"
		resp.Backend = core.BackendHealth{
			Type:   "postgres",
			Status: "disconnected",
			Error:  err.Error(),
		}
		return resp, err
	}

	resp.Status = "ok"
	resp.Backend = core.BackendHealth{
		Type:      "postgres",
		Status:    "connected",
		LatencyMs: latency,
	}
	return resp, nil
}
