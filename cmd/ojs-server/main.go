package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	ojsotel "github.com/openjobspec/ojs-go-backend-common/otel"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
	pgbackend "github.com/openjobspec/ojs-backend-postgres/internal/postgres"
	"github.com/openjobspec/ojs-backend-postgres/internal/scheduler"
	"github.com/openjobspec/ojs-backend-postgres/internal/server"
)

func main() {
	cfg := server.LoadConfig()
	if cfg.DatabaseURL == "" {
		slog.Error("missing required database configuration", "env", "DATABASE_URL")
		os.Exit(1)
	}
	if cfg.APIKey == "" && !cfg.AllowInsecureNoAuth {
		slog.Error("refusing to start without API authentication", "hint", "set OJS_API_KEY or OJS_ALLOW_INSECURE_NO_AUTH=true for local development")
		os.Exit(1)
	}
	if cfg.AllowInsecureNoAuth {
		slog.Warn("⚠️  RUNNING WITHOUT AUTHENTICATION — this is intended for local development only. Set OJS_API_KEY for any shared or production environment.")
	}

	// Initialize OpenTelemetry (opt-in via OJS_OTEL_ENABLED or OTEL_EXPORTER_OTLP_ENDPOINT)
	otelEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if ep := os.Getenv("OJS_OTEL_ENDPOINT"); ep != "" {
		otelEndpoint = ep
	}
	otelShutdown, err := ojsotel.Init(context.Background(), ojsotel.Config{
		ServiceName:    "ojs-backend-postgres",
		ServiceVersion: core.OJSVersion,
		Enabled:        os.Getenv("OJS_OTEL_ENABLED") == "true" || otelEndpoint != "",
		Endpoint:       otelEndpoint,
	})
	if err != nil {
		slog.Error("failed to initialize OpenTelemetry", "error", err)
		os.Exit(1)
	}
	defer func() { _ = otelShutdown(context.Background()) }()

	// Connect to Postgres
	backend, err := pgbackend.New(cfg.DatabaseURL, func(bc *pgbackend.BackendConfig) {
		bc.PoolMinConns = cfg.PoolMinConns
		bc.PoolMaxConns = cfg.PoolMaxConns
		bc.DefaultVisibilityTimeoutMs = cfg.DefaultVisibilityTimeoutMs
		bc.RetentionCompletedDays = cfg.RetentionCompletedDays
		bc.RetentionDiscardedDays = cfg.RetentionDiscardedDays
	})
	if err != nil {
		slog.Error("failed to connect to Postgres", "error", err)
		os.Exit(1)
	}
	defer func() { _ = backend.Close() }()

	slog.Info("connected to Postgres", "url", redactDatabaseURL(cfg.DatabaseURL))

	// Start background scheduler
	schedCfg := scheduler.Config{
		PromoteInterval: cfg.SchedulerPromoteInterval,
		RetryInterval:   cfg.SchedulerRetryInterval,
		ReaperInterval:  cfg.SchedulerReaperInterval,
		CronInterval:    cfg.SchedulerCronInterval,
		PrunerInterval:  cfg.SchedulerPrunerInterval,
	}
	sched := scheduler.New(backend, schedCfg)
	sched.Start()
	defer sched.Stop()

	// Create HTTP server
	router := server.NewRouter(backend, cfg)
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	// Start server
	go func() {
		slog.Info("OJS server listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}

	slog.Info("server stopped")
}

// redactDatabaseURL masks the password in a PostgreSQL connection URL.
func redactDatabaseURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "***"
	}
	if u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(u.User.Username(), "***")
		}
	}
	return u.String()
}

