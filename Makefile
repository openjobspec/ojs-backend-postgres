.PHONY: build run test test-integration lint vet clean docker-up docker-down dev docker-dev

DATABASE_URL ?= postgres://ojs:ojs@localhost:5432/ojs?sslmode=prefer
OJS_URL ?= http://localhost:8080

build:
	go build -o bin/ojs-server ./cmd/ojs-server

run: build
	OJS_ALLOW_INSECURE_NO_AUTH=true DATABASE_URL=$(DATABASE_URL) ./bin/ojs-server

test:
	go test ./... -race -cover

test-integration:
	go test ./... -tags=integration -race -cover -timeout=180s

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8 run ./...; \
	fi

vet:
	go vet ./...

clean:
	rm -rf bin/

# Docker
docker-up:
	docker compose -f docker/docker-compose.yml up --build -d

docker-down:
	docker compose -f docker/docker-compose.yml down

# Development with hot reload
dev:
	air -c .air.toml

docker-dev:
	docker compose -f docker/docker-compose.yml --profile dev up --build
