# DuckDB Pure-Go Driver

A pure-Go implementation of a DuckDB driver for `database/sql`. This driver uses `purego` for FFI instead of CGO, allowing for easier cross-compilation and deployment without C dependencies.

## Status

⚠️ **Early Development** - This driver is in early development and not yet ready for production use.

### Completed
- ✅ Basic project structure
- ✅ Purego wrapper for DuckDB C API
- ✅ database/sql driver interface implementation
- ✅ Connection management
- ✅ Query execution (direct and prepared statements)
- ✅ Transaction support (BEGIN/COMMIT/ROLLBACK)
- ✅ Basic type support (integers, floats, strings, booleans)
- ✅ Date/Time types (DATE, TIME, TIMESTAMP)
- ✅ BLOB type support
- ✅ Prepared statement parameter binding
- ✅ NULL value handling
- ✅ Basic decimal support
- ✅ UUID type support
- ✅ Complex types - basic support (LIST, STRUCT, MAP)
- ✅ HUGEINT (128-bit integer) type definitions
- ✅ Comprehensive test suite
- ✅ Nix development environment
- ✅ CI/CD with GitHub Actions

### In Progress
- 🚧 Full complex type integration (native LIST/STRUCT/MAP handling)
- 🚧 Full decimal type with precision/scale

### Recently Added
- ✅ Context support for cancellation
- ✅ Connection pooling implementation
- ✅ Prepared statement caching
- ✅ Comprehensive performance benchmarks
- ✅ Concurrent access optimizations

### TODO
- ❌ ENUM type
- ❌ DuckDB extensions support
- ❌ Native Go implementation (replace purego)
- ❌ SQL parser
- ❌ Query optimizer
- ❌ Storage engine

## Installation

```bash
go get github.com/connerohnesorge/dukdb-go
```

## Usage

```go
package main

import (
    "database/sql"
    "log"
    
    _ "github.com/connerohnesorge/dukdb-go"
)

func main() {
    // Open an in-memory database
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Create a table
    _, err = db.Exec(`CREATE TABLE users (id INTEGER, name VARCHAR)`)
    if err != nil {
        log.Fatal(err)
    }
    
    // Insert data
    _, err = db.Exec(`INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')`)
    if err != nil {
        log.Fatal(err)
    }
    
    // Query data
    rows, err := db.Query("SELECT id, name FROM users")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()
    
    for rows.Next() {
        var id int
        var name string
        err := rows.Scan(&id, &name)
        if err != nil {
            log.Fatal(err)
        }
        log.Printf("User: %d - %s", id, name)
    }
}
```

## Requirements

The driver requires the DuckDB shared library. You have several options:

### Option 1: Using Nix (Recommended)

The project includes a Nix flake that automatically provides DuckDB:

```bash
# Enter development shell with DuckDB
nix develop

# Or use direnv
direnv allow

# Verify DuckDB is available
./scripts/test-duckdb-availability.sh
```

### Option 2: System Installation

Install DuckDB on your system:
- **macOS**: `brew install duckdb`
- **Ubuntu/Debian**: Download from [DuckDB releases](https://github.com/duckdb/duckdb/releases)
- **Windows**: Download DLL from releases

The driver looks for:
- macOS: `libduckdb.dylib`
- Linux: `libduckdb.so`
- Windows: `duckdb.dll`

### Option 3: Custom Location

Set the `DUCKDB_LIB_DIR` environment variable:

```bash
export DUCKDB_LIB_DIR=/path/to/duckdb/lib
go run your-app.go
```

## Architecture

The driver is structured in layers:

1. **Purego Wrapper** (`internal/purego/`) - Low-level FFI bindings to DuckDB C API
2. **Driver Implementation** (`driver/`) - database/sql interface implementation
3. **Public API** (`duckdb.go`) - User-facing package

The long-term goal is to progressively replace the purego/C dependencies with native Go implementations.

## Development

### Quick Start with Nix

```bash
# Clone the repository
git clone https://github.com/connerohnesorge/dukdb-go
cd dukdb-go

# Enter development environment (installs DuckDB automatically)
nix develop

# Run tests
make test

# Run examples
make run-basic
make run-adv
```

### Development Commands

```bash
# Test commands
make test          # Run all tests
make coverage      # Generate coverage report
make bench         # Run benchmarks
make integration-test  # Run integration tests

# Development commands
make lint          # Run linters
make fmt           # Format code
make build         # Build the module

# Example commands
make run-basic     # Run basic example
make run-adv       # Run advanced example

# Utility commands
make check-duckdb  # Verify DuckDB is available
make help          # Show all commands
```

### Running Tests Manually

```bash
# All tests
go test ./...

# With coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Integration tests only
go test -v -tags=integration ./test/integration/...
```

### Running Examples Manually

```bash
# Basic usage example
go run examples/basic.go

# Advanced features (date/time, blobs, transactions, analytics)
go run examples/advanced.go

# Complex types (UUID, LIST, STRUCT, MAP)
go run examples/complex_types.go

# Performance testing and benchmarks
go run examples/performance.go
```

### Running Benchmarks

```bash
go test -bench=. -benchmem ./test/...
```

## License

GPL v3.0 - See LICENSE file for details.

## Contributing

This project is in early development. Contributions are welcome! Please see the architecture documents for implementation guidelines.
