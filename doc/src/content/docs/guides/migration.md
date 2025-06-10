# Migration Guide

This guide helps you migrate from the CGO-based DuckDB driver (`github.com/marcboeker/go-duckdb`) to the pure-Go **dukdb-go** driver.

## Overview

The **dukdb-go** driver provides a drop-in replacement for the CGO driver while offering additional benefits:

- **Zero CGO Dependencies** - Easier deployment and cross-compilation
- **Improved Performance** - Optimized connection pooling and statement caching
- **Better Debugging** - Pure Go stack traces
- **Static Binaries** - No runtime library dependencies

## Quick Migration

For most applications, migration requires only changing the import:

### Before (CGO Driver)

```go
import (
    "database/sql"
    _ "github.com/marcboeker/go-duckdb"
)

func main() {
    db, err := sql.Open("duckdb", ":memory:")
    // ... rest of code unchanged
}
```

### After (Pure-Go Driver)

```go
import (
    "database/sql"
    _ "github.com/connerohnesorge/dukdb-go/driver"
)

func main() {
    db, err := sql.Open("duckdb", ":memory:")
    // ... rest of code unchanged
}
```

## Compatibility Matrix

| Feature | CGO Driver | Pure-Go Driver | Notes |
|---------|------------|----------------|-------|
| database/sql interface | ✅ | ✅ | Full compatibility |
| Connection strings | ✅ | ✅ | Same format |
| Prepared statements | ✅ | ✅ | Enhanced caching |
| Transactions | ✅ | ✅ | Full support |
| Context support | ✅ | ✅ | Enhanced implementation |
| Basic data types | ✅ | ✅ | All types supported |
| Complex types | ✅ | ✅ | LIST, STRUCT, MAP, UUID |
| Connection pooling | ✅ | ✅ | Improved implementation |
| Cross-compilation | ❌ | ✅ | Major advantage |
| Static binaries | ❌ | ✅ | No C dependencies |
| Extension loading | ✅ | 🚧 | Planned feature |

## Step-by-Step Migration

### 1. Update Dependencies

```bash
# Remove old CGO driver
go mod edit -droprequire github.com/marcboeker/go-duckdb

# Add new pure-Go driver
go get github.com/connerohnesorge/dukdb-go
```

### 2. Update Imports

```bash
# Find and replace imports across your codebase
grep -r "github.com/marcboeker/go-duckdb" . --include="*.go"

# Replace with
# github.com/connerohnesorge/dukdb-go/driver
```

### 3. Update go.mod

```go
// go.mod
module your-application

go 1.21

require (
    // Remove: github.com/marcboeker/go-duckdb v1.6.4
    github.com/connerohnesorge/dukdb-go v1.0.0
)
```

### 4. Test Functionality

```bash
# Run your existing tests
go test ./...

# Check for any issues
go build ./...
```

## Breaking Changes

### Driver Registration Name

The driver name remains the same (`duckdb`), so no connection string changes are needed:

```go
// Both drivers use the same registration name
db, err := sql.Open("duckdb", ":memory:")
```

### Error Types

Error handling may need minor adjustments:

#### Before (CGO Driver)

```go
import "github.com/marcboeker/go-duckdb"

_, err := db.Exec("INVALID SQL")
if err != nil {
    if duckdbErr, ok := err.(*duckdb.Error); ok {
        fmt.Printf("Error: %s\n", duckdbErr.Error())
    }
}
```

#### After (Pure-Go Driver)

```go
import "github.com/connerohnesorge/dukdb-go/driver"

_, err := db.Exec("INVALID SQL")
if err != nil {
    if duckdbErr, ok := err.(*driver.Error); ok {
        fmt.Printf("Error Code: %d, Message: %s\n", 
            duckdbErr.Code, duckdbErr.Message)
    }
}
```

### Complex Type Handling

Complex types are now returned as JSON strings by default:

#### Before (CGO Driver)

```go
// Complex types might be returned as binary data or Go types
var listData []interface{}
err := rows.Scan(&listData)
```

#### After (Pure-Go Driver)

```go
// Complex types are JSON strings
var listJSON string
err := rows.Scan(&listJSON)

// Parse JSON if needed
var listData []interface{}
json.Unmarshal([]byte(listJSON), &listData)

// Or use helper functions
import "github.com/connerohnesorge/dukdb-go/internal/types"
list, err := types.ParseList(listJSON)
```

## Environment Setup

### DuckDB Library Installation

The pure-Go driver still requires the DuckDB shared library at runtime.

#### Using Nix (Recommended)

```bash
# Nix provides automatic library management
nix develop

# Library is automatically available
go run your-app.go
```

#### Manual Installation

```bash
# macOS
brew install duckdb

# Ubuntu/Debian
curl -L https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-linux-amd64.zip -o libduckdb.zip
unzip libduckdb.zip
sudo mv libduckdb.so /usr/local/lib/
sudo ldconfig

# Set library path if needed
export DUCKDB_LIB_DIR="/usr/local/lib"
```

## Performance Improvements

### Automatic Optimizations

The pure-Go driver includes several performance improvements:

```go
// No configuration needed - these are automatic:
// - Statement caching (100 statements by default)
// - Optimized connection pooling
// - Efficient type conversions
```

### Optional Optimizations

```go
import "github.com/connerohnesorge/dukdb-go/driver"

// Increase statement cache size for heavy workloads
driver.SetStatementCacheSize(500)

// Monitor cache performance
stats := driver.GetCacheStats()
fmt.Printf("Cache hit rate: %.2f%%\n", 
    float64(stats.Hits) / float64(stats.Total) * 100)
```

## Cross-Compilation Benefits

### Before (CGO Driver)

```bash
# Cross-compilation was complex and often impossible
GOOS=windows GOARCH=amd64 go build .
# Error: C compiler not available for target platform
```

### After (Pure-Go Driver)

```bash
# Cross-compilation just works
GOOS=windows GOARCH=amd64 go build .
GOOS=darwin GOARCH=arm64 go build .
GOOS=linux GOARCH=arm64 go build .

# Build for all platforms
for GOOS in darwin linux windows; do
    for GOARCH in amd64 arm64; do
        echo "Building for $GOOS/$GOARCH"
        GOOS=$GOOS GOARCH=$GOARCH go build -o bin/app-${GOOS}-${GOARCH} .
    done
done
```

## Deployment Improvements

### Container Deployment

#### Before (CGO Driver)

```dockerfile
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Install DuckDB development headers
RUN curl -L https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-src.zip -o libduckdb.zip && \
    unzip libduckdb.zip && \
    # Complex build process...

COPY . .
RUN CGO_ENABLED=1 go build -o app .

FROM alpine:latest
# Install runtime dependencies
RUN apk add --no-cache libstdc++
COPY --from=builder /app/app .
```

#### After (Pure-Go Driver)

```dockerfile
FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -o app .

FROM scratch
# No runtime dependencies needed for the Go binary
COPY --from=builder /app/app .
# Still need DuckDB library accessible at runtime
```

### Static Binary Deployment

```bash
# Build completely static binary
CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' -o myapp .

# Deploy anywhere - no dependencies except DuckDB library
scp myapp production-server:~/
```

## Testing Migration

### Compatibility Tests

Create tests to verify migration success:

```go
// migration_test.go
func TestMigrationCompatibility(t *testing.T) {
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        t.Fatal(err)
    }
    defer db.Close()
    
    // Test basic functionality
    _, err = db.Exec("CREATE TABLE test (id INTEGER, name VARCHAR)")
    assert.NoError(t, err)
    
    _, err = db.Exec("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
    assert.NoError(t, err)
    
    rows, err := db.Query("SELECT id, name FROM test ORDER BY id")
    assert.NoError(t, err)
    defer rows.Close()
    
    var results []struct {
        ID   int
        Name string
    }
    
    for rows.Next() {
        var r struct {
            ID   int
            Name string
        }
        err := rows.Scan(&r.ID, &r.Name)
        assert.NoError(t, err)
        results = append(results, r)
    }
    
    assert.Len(t, results, 2)
    assert.Equal(t, "Alice", results[0].Name)
    assert.Equal(t, "Bob", results[1].Name)
}
```

### Performance Regression Tests

```go
func BenchmarkMigrationPerformance(b *testing.B) {
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        b.Fatal(err)
    }
    defer db.Close()
    
    // Setup test data
    _, err = db.Exec("CREATE TABLE bench_test (id INTEGER, value DOUBLE)")
    if err != nil {
        b.Fatal(err)
    }
    
    b.ResetTimer()
    
    for i := 0; i < b.N; i++ {
        _, err := db.Exec("INSERT INTO bench_test VALUES (?, ?)", i, float64(i)*1.5)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

## Troubleshooting Migration

### Common Issues

#### 1. Library Not Found

```bash
# Error: libduckdb.so: cannot open shared object file
export LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
# Or
export DUCKDB_LIB_DIR="/usr/local/lib"
```

#### 2. Build Errors

```bash
# Ensure Go version is 1.21+
go version

# Clean module cache
go clean -modcache
go mod download
```

#### 3. Type Assertion Errors

```go
// Update error handling for new error types
_, err := db.Exec("INVALID SQL")
if err != nil {
    // Old way - may not work
    // if duckdbErr, ok := err.(*duckdb.Error); ok { ... }
    
    // New way
    if duckdbErr, ok := err.(*driver.Error); ok {
        fmt.Printf("Error: %s\n", duckdbErr.Message)
    }
}
```

### Validation Script

Create a script to validate the migration:

```bash
#!/bin/bash
# validate_migration.sh

echo "Validating dukdb-go migration..."

# Check Go version
if ! go version | grep -q "go1.2[1-9]"; then
    echo "Error: Go 1.21+ required"
    exit 1
fi

# Check if DuckDB library is available
if ! go run scripts/test-duckdb-availability.sh; then
    echo "Error: DuckDB library not found"
    echo "Set DUCKDB_LIB_DIR or install DuckDB"
    exit 1
fi

# Run tests
echo "Running tests..."
if ! go test ./...; then
    echo "Error: Tests failed"
    exit 1
fi

# Try to build
echo "Testing build..."
if ! go build ./...; then
    echo "Error: Build failed"
    exit 1
fi

echo "Migration validation successful!"
```

## Rollback Plan

If you need to rollback to the CGO driver:

```bash
# Remove pure-Go driver
go mod edit -droprequire github.com/connerohnesorge/dukdb-go

# Add back CGO driver
go get github.com/marcboeker/go-duckdb

# Revert import changes
find . -name "*.go" -exec sed -i 's|github.com/connerohnesorge/dukdb-go/driver|github.com/marcboeker/go-duckdb|g' {} \;

# Test
go test ./...
```

## Next Steps

After successful migration:

1. **Remove CGO Build Tags** - Clean up any `// +build cgo` constraints
1. **Update CI/CD** - Simplify build pipelines without CGO
1. **Leverage Cross-Compilation** - Build for multiple platforms easily
1. **Optimize Performance** - Use new performance features
1. **Monitor Metrics** - Track the improved performance

## Getting Help

If you encounter issues during migration:

1. Check the [Troubleshooting Guide](/guides/troubleshooting/)
1. Review the [Performance Guide](/guides/performance/) for optimization tips
1. Consult the [API Reference](/reference/api/) for detailed documentation
1. Report issues on the [GitHub repository](https://github.com/connerohnesorge/dukdb-go/issues)
