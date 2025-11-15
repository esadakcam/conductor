.PHONY: build run clean test test-coverage fmt fmt-check vet mod mod-download mod-verify help

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
	@go test -v -race -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out

## test-coverage: Run tests with HTML coverage report
test-coverage: test
	@echo "Generating coverage report..."
	@go tool cover -html=coverage.out -o coverage.html
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

## help: Show this help message
help:
	@echo "Available targets:"
	@grep -E '^##' $(MAKEFILE_LIST) | sed -e 's/## //' | column -t -s ':'

