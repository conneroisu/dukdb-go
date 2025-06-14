# DuckDB Pure-Go Driver

A pure-Go implementation of a DuckDB driver for `database/sql`. This project aims to provide a fully native Go implementation of DuckDB without CGO dependencies, enabling easy cross-compilation and deployment as static binaries.

## Project Status

🚀 **Active Development** - Core SQL functionality is working with 62.5% of analytical benchmarks passing.

### Recent Major Milestone
✅ **Analytical Query Support** - Successfully implemented core analytical SQL features including:
- Complex JOINs with aggregations
- GROUP BY with HAVING clauses
- Timestamp handling in aggregate functions
- Multi-table query support

See [Analytical Benchmark Status](benchmarks/ANALYTICAL_BENCHMARK_STATUS.md) for detailed results.

### Implementation Progress

#### ✅ Core Engine (Pure-Go)
- **SQL Parser** - Handles SELECT, INSERT, UPDATE, DELETE, CREATE TABLE
- **Query Planner** - Logical plan generation with type inference
- **Query Executor** - Vectorized execution engine
- **Storage Layer** - Columnar storage with DataChunks
- **Type System** - All major SQL types including timestamps
- **Aggregate Functions** - COUNT, SUM, AVG, MIN, MAX
- **JOIN Support** - Two-table and multi-table JOINs
- **GROUP BY/HAVING** - Full aggregation support
- **ORDER BY/LIMIT** - Sorting and result limiting

#### ✅ Driver Implementation
- **database/sql Interface** - Full compliance
- **Prepared Statements** - With parameter binding
- **Transaction Support** - BEGIN/COMMIT/ROLLBACK
- **Context Support** - Query cancellation
- **Type Conversions** - Automatic Go type mapping

#### 🚧 In Progress
- **Query Optimization** - Cost-based optimizer
- **Advanced SQL** - UNION, subqueries, CTEs
- **Performance** - Hash joins, parallel execution

#### ❌ Future Work
- **Indexes** - B-tree and adaptive indexes
- **Window Functions** - OVER clause support
- **Extensions** - Plugin system compatibility
- **Compression** - Native compression support

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

## Pure-Go Implementation

This is a **pure-Go implementation** that does not require any external DuckDB libraries or CGO. It includes:

- Native SQL parser and query engine
- Columnar storage with vectorized execution
- Full implementation of core SQL features
- No C dependencies - compiles to a single static binary

For development comparison with the C++ DuckDB implementation, see the benchmarks directory.

## Architecture

The pure-Go implementation is structured as:

```
driver/          - database/sql driver interface
internal/
  engine/        - SQL parser, planner, executor
  storage/       - Columnar storage and vectors
  types/         - Type system implementation
benchmarks/      - Performance and compatibility tests
```

See [Development Guide](DEVELOPMENT_GUIDE.md) for detailed architecture documentation.

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
go test ./...                    # Run all tests
go test -cover ./...            # With coverage
go test -race ./...             # Race detection

# Benchmark commands
cd benchmarks
go test -bench=. ./analytical/  # Run analytical benchmarks
./run_analytical_summary.sh     # Generate benchmark report

# Development
go fmt ./...                    # Format code
go vet ./...                    # Run static analysis
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
go run examples/basic/main.go

# Advanced features (date/time, blobs, transactions, analytics)
go run examples/advanced/main.go

# Complex types (UUID, LIST, STRUCT, MAP)
go run examples/complex-types/main.go

# Performance testing and benchmarks
go run examples/performance/main.go
```

### Testing Examples

Each example includes comprehensive tests:

```bash
# Test individual examples
go test -v ./examples/basic/...
go test -v ./examples/advanced/...
go test -v ./examples/complex-types/...
go test -v ./examples/performance/...

# Test all examples
make test-examples

# Benchmark examples
make bench-examples
```

### Running Benchmarks

```bash
go test -bench=. -benchmem ./test/...
```

## License

GPL v3.0 - See LICENSE file for details.

## Documentation

- [Development Guide](DEVELOPMENT_GUIDE.md) - Architecture and development workflows
- [Troubleshooting Guide](TROUBLESHOOTING.md) - Common issues and solutions
- [Analytical Benchmark Status](benchmarks/ANALYTICAL_BENCHMARK_STATUS.md) - Current test results
- [Architecture Designs](.) - Detailed design documents for each component

## Contributing

Contributions are welcome! Please:

1. Read the [Development Guide](DEVELOPMENT_GUIDE.md)
2. Check existing issues and PRs
3. Run tests and benchmarks before submitting
4. Follow Go code style conventions

Priority areas for contribution:
- Query optimization (hash joins, cost-based optimizer)
- SQL parser extensions (UNION, subqueries)
- Performance improvements
- Additional SQL functions
