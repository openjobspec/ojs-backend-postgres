# Contributing to OJS Backend PostgreSQL

Thank you for your interest in contributing! This guide will help you get started.

## Prerequisites

- Go 1.22+
- PostgreSQL 15+ (or Docker)
- Make

## Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/openjobspec/ojs-backend-postgres.git
   cd ojs-backend-postgres
   ```

2. **Start PostgreSQL:**
   ```bash
   make docker-up
   ```

3. **Run the server locally:**
   ```bash
   make run
   ```

4. **Run tests:**
   ```bash
   make test
   ```

5. **Run linter:**
   ```bash
   make lint
   ```

## Making Changes

1. Create a branch from `main`
2. Make your changes
3. Ensure tests pass: `make test`
4. Ensure linting passes: `make lint`
5. Open a pull request

## Commit Convention

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): description

feat(api): add rate limiting to fetch endpoint
fix(postgres): handle null visibility_timeout in fetch
docs(readme): update configuration section
chore(deps): update pgx to v5.8.0
```

**Types:** `feat`, `fix`, `docs`, `chore`, `refactor`, `test`, `perf`

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- Use `log/slog` for structured logging
- Wrap errors with context: `fmt.Errorf("operation: %w", err)`
- Use parameterized SQL queries (never string interpolation)
- Add `defer rows.Close()` after every `Query()` call

## Architecture

The codebase follows a layered architecture:

- `internal/core/` - Backend-agnostic interfaces and types
- `internal/api/` - HTTP handlers (chi router)
- `internal/postgres/` - PostgreSQL implementation of core interfaces
- `internal/scheduler/` - Background task scheduler
- `internal/server/` - HTTP router and configuration

When adding a new feature, changes typically touch multiple layers.

## Running with Docker

```bash
make docker-up    # Start PostgreSQL + OJS server
make docker-down  # Stop everything
```

## Questions?

Open an issue if you have questions about contributing.
