# DuckDB Go Benchmarks

This directory contains comprehensive benchmarks comparing the pure-Go DuckDB implementation with the original CGO-based DuckDB driver.

## Overview

The benchmark suite tests various aspects of database performance:

- **Basic Queries**: SELECT, INSERT, UPDATE, DELETE operations
- **Analytical Queries**: Aggregations, JOINs, window functions, CTEs
- **Data Loading**: CSV, Parquet, and JSON import/export performance
- **TPC-H**: Industry-standard decision support benchmark (TODO)

## Running Benchmarks

### Quick Start

Run all benchmarks with default settings:

```bash
go run benchmarks/run_benchmarks.go
```

### Run Specific Benchmark Categories

```bash
# Basic queries only
go test -bench=. ./benchmarks/basic

# Analytical queries only
go test -bench=. ./benchmarks/analytical

# Data loading benchmarks
go test -bench=. ./benchmarks/dataload
```

### Run with Options

```bash
# Run with verbose output
go run benchmarks/run_benchmarks.go -v

# Filter benchmarks by pattern
go run benchmarks/run_benchmarks.go -filter="Insert"

# Skip CGO benchmarks (pure-Go only)
go run benchmarks/run_benchmarks.go -skip-cgo

# Compare with previous results
go run benchmarks/run_benchmarks.go -compare=results/benchmark_20240115_120000/report.json

# Custom output directory
go run benchmarks/run_benchmarks.go -output=my_results
```

## Building with CGO Support

To compare against the CGO DuckDB implementation, you need to install the original driver:

```bash
# Install CGO DuckDB driver
go get github.com/marcboeker/go-duckdb

# Run benchmarks with CGO enabled
CGO_ENABLED=1 go test -bench=. ./benchmarks/...
```

If CGO is not available or the CGO driver is not installed, those benchmarks will be skipped automatically.

## Benchmark Results

Results are saved in the `results/` directory (or custom directory specified with `-output`) with:

- `report.json` - Machine-readable benchmark results
- `report.txt` - Human-readable summary
- `report.csv` - CSV format for analysis in spreadsheet tools

## Understanding Results

### Performance Metrics

- **Duration**: Time taken to complete the operation
- **ns/op**: Nanoseconds per operation
- **B/op**: Bytes allocated per operation
- **allocs/op**: Number of allocations per operation

### Comparison Metrics

- **Speedup Factor**: Positive means pure-Go is faster, negative means CGO is faster
- **Memory Factor**: Positive means pure-Go uses less memory, negative means more

### Example Output

```
=== DuckDB Go Benchmark Report ===
Timestamp: 2024-01-15T12:00:00Z
System: linux/amd64, 8 CPUs, Go go1.21.5

=== Summary ===
Total Benchmarks: 42
Pure-Go Faster: 18 (42.9%)
CGO Faster: 20 (47.6%)
Similar Performance: 4 (9.5%)
Average Speedup: 0.95x slower

=== Detailed Results ===
Benchmark                    Pure-Go      CGO          Speedup      Memory
---------                    -------      ---          -------      ------
SimpleSelect                 1.234ms      1.456ms      1.18x faster 0.85x less
RangeSelect                  5.678ms      4.321ms      0.76x slower 1.12x more
...
```

## Adding New Benchmarks

To add new benchmarks:

1. Create a new test file in the appropriate directory
2. Follow the pattern of existing benchmarks
3. Use `runBothImplementations` helper for automatic comparison
4. Add the benchmark suite to `run_benchmarks.go` if needed

Example:

```go
func BenchmarkMyNewTest(b *testing.B) {
    runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
        // Setup
        setupTestData(b, db)
        
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
            // Your benchmark code here
        }
    })
}
```

## Troubleshooting

### CGO benchmarks not running

- Ensure CGO is enabled: `CGO_ENABLED=1`
- Install the CGO driver: `go get github.com/marcboeker/go-duckdb`
- Check for C compiler availability

### Out of memory errors

- Reduce test data sizes in the benchmark
- Increase available memory
- Run benchmarks individually instead of all at once

### Benchmarks timing out

- Increase timeout: `go run benchmarks/run_benchmarks.go -timeout=1h`
- Run specific benchmarks instead of the full suite
- Check for infinite loops or excessive data sizes

## Performance Tips

1. **Run on dedicated hardware** - Avoid running benchmarks on systems with other workloads
2. **Multiple runs** - Run benchmarks multiple times and average results
3. **System state** - Ensure consistent system state (CPU governor, etc.)
4. **Warm-up** - The benchmark framework includes warm-up iterations automatically

## Future Improvements

- [ ] TPC-H benchmark implementation
- [ ] TPC-DS benchmark implementation  
- [ ] Concurrent workload benchmarks
- [ ] Memory pressure benchmarks
- [ ] Large dataset benchmarks (>1GB)
- [ ] Benchmark result visualization
- [ ] Automated regression detection
- [ ] CI/CD integration for performance tracking