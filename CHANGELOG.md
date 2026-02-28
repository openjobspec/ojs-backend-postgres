# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0](https://github.com/openjobspec/ojs-backend-postgres/compare/v0.1.0...v0.2.0) (2026-02-28)


### Features

* add connection pool metrics ([7bee342](https://github.com/openjobspec/ojs-backend-postgres/commit/7bee342f952d37452aa019d9c52f4901c34a54b4))
* add health check endpoint ([927ce44](https://github.com/openjobspec/ojs-backend-postgres/commit/927ce44ce4fb36dbf59ad3a86cd3dc8fe0f1ab5c))
* add metrics endpoint for monitoring ([ae012cf](https://github.com/openjobspec/ojs-backend-postgres/commit/ae012cf60518bf613a16980e782b5b9bca0f1a6f))
* implement graceful shutdown with drain timeout ([945d33d](https://github.com/openjobspec/ojs-backend-postgres/commit/945d33d56aae04b14108c21e7b954100ed5398f8))


### Bug Fixes

* correct retry backoff calculation for edge cases ([bd0ea5b](https://github.com/openjobspec/ojs-backend-postgres/commit/bd0ea5b35c5e6c7ab802bc56fa4bc5696ac97ceb))
* correct SKIP LOCKED query for high concurrency ([6e7daac](https://github.com/openjobspec/ojs-backend-postgres/commit/6e7daac291943449672973113cb4ab6ffbf7d907))
* handle connection timeout gracefully ([c1160fa](https://github.com/openjobspec/ojs-backend-postgres/commit/c1160fab5f858f008e1e310ac35626053e7b630b))
* resolve race condition in concurrent job completion ([f64cda4](https://github.com/openjobspec/ojs-backend-postgres/commit/f64cda42f7800b12ba887278d5cf9212e9a3b26c))


### Performance Improvements

* optimize batch dequeue query performance ([0700493](https://github.com/openjobspec/ojs-backend-postgres/commit/070049361a64500745e614d8b937b7bbe3da9774))

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
