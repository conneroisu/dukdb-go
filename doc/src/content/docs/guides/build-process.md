# Build Process

This guide covers building the **dukdb-go** driver from source, including development setup, dependencies, and build configurations.

## Development Environment Setup

### Prerequisites

- **Go 1.21+** - Required for purego FFI support
- **Git** - For source code management
- **DuckDB shared library** - C library that the driver interfaces with
- **Nix (recommended)** - For reproducible builds and dependency management

### Using Nix (Recommended)

The project includes a comprehensive Nix flake that provides all build dependencies:

```bash
# Clone the repository
git clone https://github.com/connerohnesorge/dukdb-go.git
cd dukdb-go

# Enter the development shell
nix develop

# All dependencies are now available
go version
duckdb --version
```

The Nix environment includes:

- Go 1.21+ with development tools
- DuckDB shared library
- golangci-lint for code quality
- gopls language server
- Additional development utilities

### Manual Setup

If not using Nix, install dependencies manually:

#### Go Installation

**macOS (Homebrew):**

```bash
brew install go
```

**Ubuntu/Debian:**

```bash
sudo apt update
sudo apt install golang-go
```

**From Source:**

```bash
curl -L https://go.dev/dl/go1.21.6.linux-amd64.tar.gz -o go1.21.6.tar.gz
sudo tar -C /usr/local -xzf go1.21.6.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

#### DuckDB Library Installation

**macOS (Homebrew):**

```bash
brew install duckdb
```

**Ubuntu/Debian:**

```bash
# Method 1: Using the DuckDB repository
curl -L https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-linux-amd64.zip -o libduckdb.zip
unzip libduckdb.zip
sudo cp libduckdb.so /usr/local/lib/
sudo ldconfig

# Method 2: From APT (if available)
sudo apt install libduckdb-dev
```

**Building from Source:**

```bash
git clone https://github.com/duckdb/duckdb.git
cd duckdb
make release
# Library location: build/release/src/libduckdb.so
```

## Building the Driver

### Standard Build

```bash
# Clone the repository
git clone https://github.com/connerohnesorge/dukdb-go.git
cd dukdb-go

# Download dependencies
go mod download

# Build the driver
go build ./...

# Run tests to verify build
go test ./...
```

### Build with Custom DuckDB Location

If DuckDB is installed in a non-standard location:

```bash
export DUCKDB_LIB_DIR="/path/to/duckdb/lib"
go build ./...
```

### Cross-Compilation

One of the key advantages of the pure-Go approach is easy cross-compilation:

```bash
# Build for Windows
GOOS=windows GOARCH=amd64 go build ./...

# Build for macOS
GOOS=darwin GOARCH=amd64 go build ./...

# Build for Linux ARM64
GOOS=linux GOARCH=arm64 go build ./...

# Build for multiple platforms
for GOOS in darwin linux windows; do
    for GOARCH in amd64 arm64; do
        echo "Building for $GOOS/$GOARCH"
        GOOS=$GOOS GOARCH=$GOARCH go build -o bin/example-${GOOS}-${GOARCH} ./examples/basic.go
    done
done
```

### Static Binary Build

Build completely static binaries:

```bash
# Disable CGO and build static binary
CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' ./...

# For Alpine Linux compatibility
CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' ./...
```

## Build Configurations

### Development Build

For development with debugging symbols and race detection:

```bash
# Build with race detector
go build -race ./...

# Run tests with race detection
go test -race ./...

# Build with debug symbols
go build -gcflags="all=-N -l" ./...
```

### Production Build

Optimized build for production deployment:

```bash
# Optimized build with stripped symbols
go build -ldflags="-s -w" ./...

# With version information
VERSION=$(git describe --tags --always)
go build -ldflags="-s -w -X main.version=${VERSION}" ./...
```

## Testing

### Running Tests

```bash
# Run all tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run specific package tests
go test ./driver/...
go test ./internal/purego/...

# Run tests with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

### Integration Tests

Integration tests require a DuckDB library:

```bash
# Ensure DuckDB is available
export DUCKDB_LIB_DIR="/usr/local/lib"

# Run integration tests
go test -tags=integration ./test/integration/...

# Run compatibility tests (compares with CGO driver)
go test ./test/compatibility/...
```

### Benchmark Tests

```bash
# Run benchmark tests
go test -bench=. ./test/benchmark/...

# Run benchmarks with memory profiling
go test -bench=. -benchmem ./test/benchmark/...

# Compare benchmarks
go test -bench=. ./test/benchmark/... > old.txt
# Make changes...
go test -bench=. ./test/benchmark/... > new.txt
benchcmp old.txt new.txt
```

## Code Quality

### Linting

```bash
# Install golangci-lint (if not using Nix)
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Run linter
golangci-lint run

# Run with all linters
golangci-lint run --enable-all

# Auto-fix issues where possible
golangci-lint run --fix
```

### Code Formatting

```bash
# Format code
go fmt ./...

# Use goimports for import management
go install golang.org/x/tools/cmd/goimports@latest
goimports -w .

# Use gofumpt for stricter formatting
go install mvdan.cc/gofumpt@latest
gofumpt -w .
```

### Static Analysis

```bash
# Run go vet
go vet ./...

# Run staticcheck
go install honnef.co/go/tools/cmd/staticcheck@latest
staticcheck ./...

# Run gosec for security issues
go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest
gosec ./...
```

## Building Examples

The repository includes several example programs:

```bash
# Build basic example
go build -o bin/basic ./examples/basic.go

# Build advanced example with complex types
go build -o bin/advanced ./examples/advanced.go

# Build performance example
go build -o bin/performance ./examples/performance.go

# Run examples
./bin/basic
./bin/advanced
./bin/performance
```

## Docker Build

Create containerized builds:

```dockerfile
# Dockerfile
FROM golang:1.21-alpine AS builder

# Install DuckDB
RUN apk add --no-cache curl unzip
RUN curl -L https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-linux-amd64.zip -o libduckdb.zip && \
    unzip libduckdb.zip && \
    mv libduckdb.so /usr/local/lib/ && \
    rm libduckdb.zip

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o main ./examples/basic.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/main .
CMD ["./main"]
```

```bash
# Build Docker image
docker build -t dukdb-go .

# Run in container
docker run dukdb-go
```

## Makefile

The project includes a Makefile for common tasks:

```makefile
.PHONY: build test lint clean

# Default target
all: build test

# Build the project
build:
	go build ./...

# Run tests
test:
	go test ./...

# Run tests with coverage
test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run linter
lint:
	golangci-lint run

# Clean build artifacts
clean:
	go clean ./...
	rm -f coverage.out coverage.html

# Build examples
examples:
	mkdir -p bin
	go build -o bin/basic ./examples/basic.go
	go build -o bin/advanced ./examples/advanced.go
	go build -o bin/performance ./examples/performance.go

# Run benchmarks
bench:
	go test -bench=. -benchmem ./test/benchmark/...

# Install dependencies
deps:
	go mod download
	go mod tidy
```

Usage:

```bash
make build
make test
make lint
make examples
make bench
```

## CI/CD Pipeline

The project uses GitHub Actions for continuous integration:

### Workflow Configuration

```yaml
# .github/workflows/ci.yml
name: CI

on:
  push:
    branches: [ main, dev ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        go-version: [1.21, 1.22]
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Go
      uses: actions/setup-go@v4
      with:
        go-version: ${{ matrix.go-version }}
    
    - name: Install DuckDB
      run: |
        curl -L https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-linux-amd64.zip -o libduckdb.zip
        unzip libduckdb.zip
        sudo mv libduckdb.so /usr/local/lib/
        sudo ldconfig
    
    - name: Download dependencies
      run: go mod download
    
    - name: Run tests
      run: go test -v ./...
    
    - name: Run linter
      uses: golangci/golangci-lint-action@v3
      with:
        version: latest
    
    - name: Build examples
      run: |
        go build ./examples/basic.go
        go build ./examples/advanced.go
```

## Troubleshooting Build Issues

### Common Problems

**1. Library Not Found**

```bash
# Error: libduckdb.so: cannot open shared object file
export LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
# Or set DUCKDB_LIB_DIR
export DUCKDB_LIB_DIR="/usr/local/lib"
```

**2. Go Version Too Old**

```bash
# Error: purego requires Go 1.21+
go version
# Upgrade to Go 1.21 or later
```

**3. Import Errors**

```bash
# Error: cannot find package
go mod tidy
go mod download
```

**4. CGO Disabled**

```bash
# Our driver should work with CGO_ENABLED=0
export CGO_ENABLED=0
go build ./...
```

### Debug Build Issues

```bash
# Enable verbose build output
go build -v ./...

# Check module dependencies
go mod graph

# Verify library loading
go run ./scripts/test-duckdb-availability.sh
```

## Performance Considerations

### Build Optimizations

```bash
# Use build cache
export GOCACHE=/tmp/go-build-cache
go build ./...

# Parallel compilation
export GOMAXPROCS=8
go build ./...

# Link-time optimization
go build -ldflags="-s -w" ./...
```

### Runtime Library Loading

The driver uses several strategies to find the DuckDB library:

1. `DUCKDB_LIB_DIR` environment variable
1. Standard system library paths (`/usr/local/lib`, `/usr/lib`)
1. Nix store paths (when using Nix)
1. macOS Homebrew paths (`/opt/homebrew/lib`)

Configure the optimal path for your environment to improve startup time.

## Next Steps

- [Complex Types Guide](/guides/complex-types/) - Working with advanced DuckDB types
- [Performance Optimization](/guides/performance/) - Tuning for optimal performance
- [API Reference](/reference/api/) - Complete API documentation
