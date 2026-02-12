# ojs-backend-postgres

A production-ready PostgreSQL backend for the [OpenJobSpec](https://github.com/openjobspec) (OJS) job queue specification. It provides a full-featured HTTP API for distributed job queue management -- including priority queues, scheduled jobs, retries with backoff, cron scheduling, workflows, dead letter queues, and more -- all backed by PostgreSQL's `SELECT FOR UPDATE SKIP LOCKED` for safe, non-blocking concurrent job processing.

## Features

- **Priority Queues** -- Integer-priority ordering with concurrent-safe fetching
- **Scheduled Jobs** -- Enqueue jobs for future execution with absolute or relative timestamps
- **Retries with Backoff** -- Exponential, linear, or constant backoff strategies with jitter
- **Dead Letter Queue** -- Inspect and retry permanently failed jobs
- **Cron Jobs** -- Recurring jobs with cron expressions, timezone support, and overlap policies
- **Workflows** -- Chain (sequential), Group (parallel), and Batch (parallel with callbacks) orchestration
- **Job Deduplication** -- Unique keys with configurable conflict resolution (reject, ignore, replace)
- **Worker Heartbeats** -- Visibility timeout extension and automatic stalled-job reaping
- **Queue Management** -- Pause/resume queues, real-time statistics
- **Batch Enqueue** -- Atomically enqueue multiple jobs in a single request
- **LISTEN/NOTIFY** -- Real-time PostgreSQL notifications for immediate job pickup
- **Graceful Shutdown** -- SIGINT/SIGTERM handling with a 30-second drain timeout

Implements OJS spec version `1.0.0-rc.1` at compliance levels 0 through 4.

## Prerequisites

- **Go** 1.22+
- **PostgreSQL** 12+ (16 recommended)
- **Docker** and **Docker Compose** (optional, for containerized deployment)

## Quick Start

### Option 1: Docker Compose (recommended)

The fastest way to get a running instance with PostgreSQL included:

```bash
make docker-up
```

The server will be available at `http://localhost:8080`. To stop:

```bash
make docker-down
```

### Option 2: Build from source

```bash
# Build the binary
make build

# Start the server (requires a running PostgreSQL instance)
DATABASE_URL="postgres://user:pass@localhost:5432/ojs?sslmode=disable" make run
```

Database migrations run automatically on startup.

## Usage

All endpoints accept and return `application/json` (or `application/openjobspec+json`).

### Enqueue a job

```bash
curl -X POST http://localhost:8080/ojs/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email.send",
    "args": {"to": "user@example.com", "subject": "Hello"},
    "options": {
      "queue": "emails",
      "priority": 10
    }
  }'
```

### Fetch jobs as a worker

```bash
curl -X POST http://localhost:8080/ojs/v1/workers/fetch \
  -H "Content-Type: application/json" \
  -d '{
    "queues": ["emails"],
    "worker_id": "worker-1",
    "max_jobs": 5
  }'
```

### Acknowledge a completed job

```bash
curl -X POST http://localhost:8080/ojs/v1/workers/ack \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "<job-id>",
    "worker_id": "worker-1"
  }'
```

### Schedule a job for later

```bash
curl -X POST http://localhost:8080/ojs/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "report.generate",
    "args": {"report_id": 42},
    "options": {
      "queue": "reports",
      "scheduled_at": "2025-12-01T09:00:00Z"
    }
  }'
```

### Register a cron job

```bash
curl -X POST http://localhost:8080/ojs/v1/cron \
  -H "Content-Type: application/json" \
  -d '{
    "name": "nightly-cleanup",
    "expression": "0 3 * * *",
    "timezone": "America/New_York",
    "queue": "maintenance",
    "job_template": {
      "type": "db.cleanup",
      "args": {"older_than_days": 90}
    }
  }'
```

### Create a workflow

```bash
curl -X POST http://localhost:8080/ojs/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "etl-pipeline",
    "type": "chain",
    "jobs": [
      {"type": "extract", "args": {"source": "s3://bucket/data"}},
      {"type": "transform", "args": {"format": "parquet"}},
      {"type": "load", "args": {"destination": "warehouse"}}
    ]
  }'
```

## API Reference

### System

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/ojs/manifest` | OJS implementation manifest |
| `GET` | `/ojs/v1/health` | Health check |

### Jobs

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/ojs/v1/jobs` | Enqueue a job |
| `GET` | `/ojs/v1/jobs/{id}` | Get job details |
| `DELETE` | `/ojs/v1/jobs/{id}` | Cancel a job |
| `POST` | `/ojs/v1/jobs/batch` | Enqueue multiple jobs atomically |

### Workers

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/ojs/v1/workers/fetch` | Fetch available jobs from queues |
| `POST` | `/ojs/v1/workers/ack` | Acknowledge successful completion |
| `POST` | `/ojs/v1/workers/nack` | Report job failure |
| `POST` | `/ojs/v1/workers/heartbeat` | Extend visibility timeout |

### Queues

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/ojs/v1/queues` | List all queues |
| `GET` | `/ojs/v1/queues/{name}/stats` | Get queue statistics |
| `POST` | `/ojs/v1/queues/{name}/pause` | Pause a queue |
| `POST` | `/ojs/v1/queues/{name}/resume` | Resume a queue |

### Dead Letter

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/ojs/v1/dead-letter` | List dead letter jobs |
| `POST` | `/ojs/v1/dead-letter/{id}/retry` | Retry a dead letter job |
| `DELETE` | `/ojs/v1/dead-letter/{id}` | Delete a dead letter job |

### Cron

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/ojs/v1/cron` | List cron jobs |
| `POST` | `/ojs/v1/cron` | Register a cron job |
| `DELETE` | `/ojs/v1/cron/{name}` | Delete a cron job |

### Workflows

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/ojs/v1/workflows` | Create a workflow (chain/group/batch) |
| `GET` | `/ojs/v1/workflows/{id}` | Get workflow status |
| `DELETE` | `/ojs/v1/workflows/{id}` | Cancel a workflow |

## Configuration

The server is configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `OJS_PORT` | `8080` | HTTP server listen port |
| `DATABASE_URL` | `postgres://localhost:5432/ojs?sslmode=disable` | PostgreSQL connection string |

## Project Structure

```
ojs-backend-postgres/
├── cmd/ojs-server/          # Application entry point
│   └── main.go
├── internal/
│   ├── api/                 # HTTP handlers and middleware
│   ├── core/                # Domain types, interfaces, and validation
│   ├── postgres/            # PostgreSQL backend implementation
│   │   └── migrations/      # SQL schema migrations (auto-applied)
│   ├── scheduler/           # Background task scheduler
│   └── server/              # HTTP router and configuration
├── docker/
│   ├── Dockerfile           # Multi-stage production build
│   └── docker-compose.yml   # Postgres + OJS server stack
├── Makefile                 # Build, test, and Docker targets
└── go.mod
```

**Key packages:**

- `core` -- Backend-agnostic domain types (`Job`, `Backend` interface, state machine, validation). This is the contract that any OJS backend must satisfy.
- `postgres` -- PostgreSQL implementation using `pgx/v5`, `SKIP LOCKED` dequeue, `LISTEN/NOTIFY`, and embedded SQL migrations.
- `api` -- HTTP handlers that translate requests into backend operations with proper error mapping.
- `scheduler` -- Background goroutines for scheduled-job promotion, retry promotion, stalled-job reaping, cron firing, and old-job pruning.

## Development

```bash
# Run tests with race detection and coverage
make test

# Run the Go vet linter
make lint

# Build the binary to bin/ojs-server
make build

# Clean build artifacts
make clean
```

## Contributing

Contributions are welcome. To get started:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-change`)
3. Make your changes and ensure `make test` and `make lint` pass
4. Commit with a clear message describing the change
5. Open a pull request against `main`

When submitting issues, please include steps to reproduce, expected behavior, and the PostgreSQL version you are using.

## License

This project does not currently include a license file. Contact the maintainers for usage terms.
