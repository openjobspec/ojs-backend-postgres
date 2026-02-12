# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- MIT License
- Configurable operational parameters via environment variables (pool sizes, retention periods, scheduler intervals, timeouts)
- Structured logging with `log/slog` across all packages
- Unit tests for core package (state, validation, backoff, duration, UUID, errors)
- Configuration tests for environment variable parsing
- `.golangci.yml` linter configuration
- `.env.example` documenting all supported environment variables
- `CONTRIBUTING.md` with development setup guide
- GitHub Actions CI pipeline (lint, test, build)

### Changed
- Replaced `log.Printf` with `log/slog` structured logging throughout
- Scheduler now accepts configurable intervals instead of hardcoded values
- Backend constructor now accepts option functions for pool and retention config
- Dead letter listing now caps limit at 1000

### Fixed
- Consistent error wrapping across all postgres package operations
- Missing `defer rows.Close()` in workflow cancellation
- Error wrapping in queue, dead letter, cron, worker, and scheduler operations

## [0.1.0] - 2025-01-01

### Added
- Initial implementation of OJS PostgreSQL backend
- Full OJS 1.0.0-rc.1 compliance (levels 0-4)
- Job lifecycle: enqueue, fetch (SKIP LOCKED), ack, nack, cancel
- Batch enqueue support
- Retry policies with exponential, linear, and constant backoff
- Cron job scheduling with timezone support
- Workflow orchestration (chain, group, batch)
- Dead letter queue management
- Queue pause/resume
- Worker heartbeat and visibility timeout
- LISTEN/NOTIFY for real-time job availability signaling
- Embedded SQL migrations with idempotent tracking
- Docker and Docker Compose support
- HTTP API with chi router
