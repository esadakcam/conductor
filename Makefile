.PHONY: build run clean test test-coverage fmt fmt-check vet mod mod-download mod-verify docker-up docker-down docker-logs docker-ps docker-restart docker-stop help

# Binary name
BINARY_NAME=conductor
# Build directory
BUILD_DIR=bin
# Main package path
MAIN_PATH=./cmd

# Default target
.DEFAULT_GOAL := help

## build: Build the application
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) $(MAIN_PATH)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

## build-amd64: Build the application for Linux amd64
build-amd64:
	@echo "Building $(BINARY_NAME) for Linux amd64..."
	@mkdir -p $(BUILD_DIR)
	@GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 $(MAIN_PATH)
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64"

## run: Run the application
run:
	@go run $(MAIN_PATH)

## clean: Remove build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html
	@go clean
	@echo "Clean complete"

## test: Run tests
test:
	@echo "Running tests..."
	@go test -v -race ./...

## test-cover: Run tests with coverage
test-cover:
	@echo "Running tests with coverage..."
	@go test -v -race -coverprofile=coverage.out -coverpkg=./... ./internal/k8s/... ./internal/server/... ./internal/utils/... 2>/dev/null || true
	@go tool cover -func=coverage.out 2>/dev/null || echo "Coverage report not available"

## test-coverage: Run tests with HTML coverage report
test-coverage: test-cover
	@echo "Generating coverage report..."
	@go tool cover -html=coverage.out -o coverage.html 2>/dev/null || echo "Could not generate HTML coverage"
	@echo "Coverage report generated: coverage.html"

## fmt: Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...

## fmt-check: Check if code is formatted (fails if not)
fmt-check:
	@echo "Checking code formatting..."
	@if [ "$$(gofmt -s -l . | wc -l)" -gt 0 ]; then \
		echo "Error: Code is not formatted. Run 'make fmt' to fix."; \
		gofmt -s -d .; \
		exit 1; \
	fi
	@echo "Code is properly formatted."

## vet: Run go vet
vet:
	@echo "Running go vet..."
	@go vet ./...

## mod-download: Download dependencies
mod-download:
	@echo "Downloading dependencies..."
	@go mod download

## mod-verify: Verify dependencies (CI-safe, does not modify files)
mod-verify:
	@echo "Verifying dependencies..."
	@go mod verify

## mod: Tidy and verify dependencies
mod:
	@echo "Tidying dependencies..."
	@go mod tidy
	@go mod verify

## docker-up: Start the etcd cluster
docker-up:
	@echo "Starting etcd cluster..."
	@cd deployment && docker-compose up -d
	@echo "Etcd cluster started"

## docker-down: Stop and remove the etcd cluster
docker-down:
	@echo "Stopping etcd cluster..."
	@cd deployment && docker-compose down
	@echo "Etcd cluster stopped"

## docker-stop: Stop the etcd cluster (without removing containers)
docker-stop:
	@echo "Stopping etcd cluster..."
	@cd deployment && docker-compose stop
	@echo "Etcd cluster stopped"

## docker-restart: Restart the etcd cluster
docker-restart:
	@echo "Restarting etcd cluster..."
	@cd deployment && docker-compose restart
	@echo "Etcd cluster restarted"

## docker-logs: Show etcd cluster logs
docker-logs:
	@cd deployment && docker-compose logs -f

## docker-ps: Show running etcd containers
docker-ps:
	@cd deployment && docker-compose ps

## help: Show this help message
help:
	@echo "Available targets:"
	@grep -E '^##' $(MAKEFILE_LIST) | sed -e 's/## //' | column -t -s ':'

