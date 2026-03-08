package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"

	ojsotel "github.com/openjobspec/ojs-go-backend-common/otel"

	"github.com/openjobspec/ojs-backend-postgres/internal/core"
	ojsgrpc "github.com/openjobspec/ojs-backend-postgres/internal/grpc"
	pgbackend "github.com/openjobspec/ojs-backend-postgres/internal/postgres"
	"github.com/openjobspec/ojs-backend-postgres/internal/scheduler"
	"github.com/openjobspec/ojs-backend-postgres/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
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

	// Initialize event broker for real-time SSE/WebSocket/gRPC streaming
	broker := pgbackend.NewEventPubSubBroker()
	defer broker.Close()

	// Create HTTP server with real-time events
	router := server.NewRouterWithRealtime(backend, cfg, broker, broker)
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	// Start HTTP server
	go func() {
		slog.Info("OJS HTTP server listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// Start gRPC server
	grpcServer := grpc.NewServer()
	ojsgrpc.Register(grpcServer, backend, ojsgrpc.WithEventSubscriber(broker))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("ojs.v1.OJSService", healthpb.HealthCheckResponse_SERVING)
	reflection.Register(grpcServer)

	go func() {
		lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
		if err != nil {
			slog.Error("failed to listen for gRPC", "port", cfg.GRPCPort, "error", err)
			os.Exit(1)
		}
		slog.Info("OJS gRPC server listening", "port", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server error", "error", err)
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

	grpcServer.GracefulStop()
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

