.PHONY: build run test test-integration lint vet clean docker-up docker-down dev docker-dev \
	conformance conformance-level-0 conformance-level-1 conformance-level-2 conformance-level-3 conformance-level-4 conformance-docker

BINARY_NAME := ojs-server
BUILD_DIR := bin
CONFORMANCE_RUNNER := ../ojs-conformance/runner/http
CONFORMANCE_SUITES := ../../suites
DATABASE_URL ?= postgres://ojs:ojs@localhost:5432/ojs?sslmode=prefer
OJS_URL ?= http://localhost:8080

build: ## Build the server binary
	@mkdir -p $(BUILD_DIR)
	go build -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/ojs-server

run: build ## Build and run the server
	OJS_ALLOW_INSECURE_NO_AUTH=true DATABASE_URL=$(DATABASE_URL) ./$(BUILD_DIR)/$(BINARY_NAME)

test: ## Run all tests
	go test ./... -race -cover

test-integration: ## Run integration tests (requires running PostgreSQL)
	go test ./... -tags=integration -race -cover -timeout=180s

lint: ## Run linter
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8 run ./...; \
	fi

vet: ## Run go vet
	go vet ./...

clean: ## Remove build artifacts
	rm -rf $(BUILD_DIR)

# Docker
docker-up: ## Start server + PostgreSQL via Docker Compose
	docker compose -f docker/docker-compose.yml up --build -d

docker-down: ## Stop Docker Compose
	docker compose -f docker/docker-compose.yml down

# Development with hot reload
dev:
	air -c .air.toml

docker-dev:
	docker compose -f docker/docker-compose.yml --profile dev up --build

# Conformance testing
conformance: ## Run all conformance levels (requires running server at OJS_URL)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES)

conformance-level-0: ## Run conformance level 0 (Core)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 0

conformance-level-1: ## Run conformance level 1 (Reliable)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 1

conformance-level-2: ## Run conformance level 2 (Scheduled)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 2

conformance-level-3: ## Run conformance level 3 (Workflows)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 3

conformance-level-4: ## Run conformance level 4 (Advanced)
	cd $(CONFORMANCE_RUNNER) && go run . -url $(OJS_URL) -suites $(CONFORMANCE_SUITES) -level 4

conformance-docker: ## Start PostgreSQL + server via Docker, run conformance, then stop
	@echo "Starting PostgreSQL and OJS server..."
	docker compose -f docker/docker-compose.yml up -d --build --wait
	@echo "Running conformance tests..."
	cd $(CONFORMANCE_RUNNER) && go run . -url http://localhost:8080 -suites $(CONFORMANCE_SUITES) ; \
		EXIT_CODE=$$? ; \
		echo "Stopping containers..." ; \
		docker compose -f docker/docker-compose.yml down ; \
		exit $$EXIT_CODE

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'
