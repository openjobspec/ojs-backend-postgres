.PHONY: build run test lint clean docker-up docker-down

DATABASE_URL ?= postgres://localhost:5432/ojs?sslmode=disable
OJS_URL ?= http://localhost:8080

build:
	go build -o bin/ojs-server ./cmd/ojs-server

run: build
	DATABASE_URL=$(DATABASE_URL) ./bin/ojs-server

test:
	go test ./... -race -cover

lint:
	go vet ./...

clean:
	rm -rf bin/

# Docker
docker-up:
	docker compose -f docker/docker-compose.yml up --build -d

docker-down:
	docker compose -f docker/docker-compose.yml down
