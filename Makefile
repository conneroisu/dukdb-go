.PHONY: all test bench coverage lint clean run-basic run-adv check-duckdb

# Default target
all: test

# Check if DuckDB library is available
check-duckdb:
	@echo "Checking DuckDB library availability..."
	@if [ -z "$$DUCKDB_LIB_DIR" ] && ! ldconfig -p | grep -q libduckdb; then \
		echo "❌ DuckDB library not found!"; \
		echo "Please run 'nix develop' or install DuckDB"; \
		exit 1; \
	else \
		echo "✅ DuckDB library found"; \
	fi

# Run all tests
test: check-duckdb
	@echo "Running tests..."
	CGO_ENABLED=0 go test -v ./...

# Run tests with coverage
coverage: check-duckdb
	@echo "Running tests with coverage..."
	CGO_ENABLED=0 go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run benchmarks
bench: check-duckdb
	@echo "Running benchmarks..."
	CGO_ENABLED=0 go test -bench=. -benchmem ./test/...

# Run linters
lint:
	@echo "Running linters..."
	golangci-lint run ./...

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Run basic example
run-basic: check-duckdb
	@echo "Running basic example..."
	CGO_ENABLED=0 go run examples/basic/main.go

# Run advanced example
run-adv: check-duckdb
	@echo "Running advanced example..."
	CGO_ENABLED=0 go run examples/advanced/main.go

# Run complex types example
run-complex: check-duckdb
	@echo "Running complex types example..."
	CGO_ENABLED=0 go run examples/complex-types/main.go

# Run performance example
run-perf: check-duckdb
	@echo "Running performance example..."
	CGO_ENABLED=0 go run examples/performance/main.go

# Test all examples
test-examples: check-duckdb
	@echo "Testing all examples..."
	CGO_ENABLED=0 go test -v ./examples/basic/...
	CGO_ENABLED=0 go test -v ./examples/advanced/...
	CGO_ENABLED=0 go test -v ./examples/complex-types/...
	CGO_ENABLED=0 go test -v ./examples/performance/...

# Benchmark all examples
bench-examples: check-duckdb
	@echo "Benchmarking all examples..."
	CGO_ENABLED=0 go test -bench=. -benchmem ./examples/basic/...
	CGO_ENABLED=0 go test -bench=. -benchmem ./examples/advanced/...
	CGO_ENABLED=0 go test -bench=. -benchmem ./examples/complex-types/...
	CGO_ENABLED=0 go test -bench=. -benchmem ./examples/performance/...

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

# Build the module
build:
	@echo "Building..."
	CGO_ENABLED=0 go build ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f coverage.out coverage.html
	go clean -cache -testcache

# Run integration tests against real DuckDB
integration-test: check-duckdb
	@echo "Running integration tests..."
	CGO_ENABLED=0 go test -v -tags=integration ./test/integration/...

# Quick test for CI
ci-test: lint test

# Install git hooks
install-hooks:
	@echo "Installing git hooks..."
	@mkdir -p .git/hooks
	@echo '#!/bin/sh\nmake lint' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Git hooks installed"

# Help
help:
	@echo "Available targets:"
	@echo "  make test          - Run all tests"
	@echo "  make bench         - Run benchmarks"
	@echo "  make coverage      - Generate coverage report"
	@echo "  make lint          - Run linters"
	@echo "  make fmt           - Format code"
	@echo "  make run-basic     - Run basic example"
	@echo "  make run-adv       - Run advanced example"
	@echo "  make run-complex   - Run complex types example"
	@echo "  make run-perf      - Run performance example"
	@echo "  make test-examples - Test all examples"
	@echo "  make bench-examples- Benchmark all examples"
	@echo "  make deps          - Download dependencies"
	@echo "  make build         - Build the module"
	@echo "  make clean         - Clean build artifacts"
	@echo "  make ci-test       - Run CI tests (lint + test)"
	@echo "  make help          - Show this help"