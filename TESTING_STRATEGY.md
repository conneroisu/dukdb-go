# DuckDB-Go Testing Strategy

This document outlines the comprehensive testing strategy for the pure-Go implementation of DuckDB, ensuring feature parity, performance, and reliability without CGO dependencies.

## Overview

The testing strategy is designed to:

1. Ensure compatibility with DuckDB's SQL dialect and behavior
1. Validate performance characteristics of the pure-Go implementation
1. Verify correctness across all supported data types and operations
1. Maintain compatibility with Go's database/sql interface
1. Provide confidence in deployment without CGO dependencies

## Testing Philosophy

### Core Principles

- **Compatibility First**: Ensure behavior matches DuckDB wherever possible
- **Progressive Coverage**: Start with core functionality, expand to edge cases
- **Performance Awareness**: Track performance characteristics from day one
- **Go-Native Design**: Leverage Go's testing strengths (table-driven, benchmarks, fuzzing)
- **Continuous Validation**: Tests run on every commit, catching regressions early

### Test Pyramid

```
        /\
       /  \  E2E Tests (10%)
      /    \ - Full application scenarios
     /      \ - Cross-feature integration
    /--------\
   /          \ Integration Tests (30%)
  /            \ - SQL Logic Tests
 /              \ - Multi-component tests
/----------------\
/                  \ Unit Tests (60%)
/                    \ - Component isolation
/                      \ - Fast, focused tests
```

## Test Categories

### 1. Unit Tests

#### Parser Tests (`parser/`)

```go
// parser_test.go
func TestParseSelectStatement(t *testing.T) {
    tests := []struct {
        name     string
        sql      string
        expected *ast.SelectStmt
        wantErr  bool
    }{
        {
            name: "simple select",
            sql:  "SELECT * FROM users",
            expected: &ast.SelectStmt{
                // Expected AST structure
            },
        },
        // More test cases...
    }
    // Table-driven test implementation
}
```

Coverage areas:

- Basic SQL statements (SELECT, INSERT, UPDATE, DELETE)
- DuckDB-specific syntax (LIST types, STRUCT access)
- Complex queries (CTEs, window functions)
- Error cases and malformed SQL

#### Type System Tests (`types/`)

```go
// types_test.go
func TestHugeIntOperations(t *testing.T) {
    // Test HUGEINT type operations
}

func TestListType(t *testing.T) {
    // Test LIST type operations
}

func TestStructType(t *testing.T) {
    // Test STRUCT type operations
}
```

Coverage areas:

- All DuckDB data types
- Type conversions and coercion
- NULL handling
- Type-specific operations

#### Execution Engine Tests (`execution/`)

```go
// operators_test.go
func TestScanOperator(t *testing.T) {
    // Test table scan functionality
}

func TestJoinOperator(t *testing.T) {
    // Test different join algorithms
}
```

Coverage areas:

- Individual operator correctness
- Memory management
- Parallel execution
- Error propagation

#### Storage Tests (`storage/`)

```go
// columnar_test.go
func TestColumnarStorage(t *testing.T) {
    // Test columnar data storage
}

func TestCompression(t *testing.T) {
    // Test compression algorithms
}
```

Coverage areas:

- Columnar storage operations
- Compression/decompression
- File format readers/writers
- Transaction management

### 2. Integration Tests

#### SQL Logic Tests (`test/sql/`)

Adopt DuckDB's SQL Logic Test format:

```sql
# test/sql/types/hugeint/test_hugeint.test
# description: Test HUGEINT type operations
# group: [types]

statement ok
CREATE TABLE hugeints (h HUGEINT)

statement ok
INSERT INTO hugeints VALUES (170141183460469231731687303715884105727)

query I
SELECT h + 1 FROM hugeints
----
170141183460469231731687303715884105728

query T
SELECT typeof(h) FROM hugeints
----
HUGEINT

statement error
SELECT h::INTEGER FROM hugeints
----
Conversion Error: HUGEINT out of range for INTEGER
```

Test runner implementation:

```go
// sqllogictest/runner.go
type TestRunner struct {
    db *sql.DB
}

func (r *TestRunner) RunTest(testFile string) error {
    // Parse .test file
    // Execute statements
    // Compare results
}
```

#### Compatibility Tests (`test/compatibility/`)

Compare behavior with CGO-based go-duckdb:

```go
// compatibility_test.go
func TestCompatibilityWithCGO(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping compatibility test in short mode")
    }
    
    // Run same query on both implementations
    cgoDB := openCGODuckDB(t)
    pureGoDB := openPureGoDuckDB(t)
    
    queries := []string{
        "SELECT 1 + 1",
        "SELECT LIST_VALUE(1, 2, 3)",
        // More queries...
    }
    
    for _, query := range queries {
        compareBehavior(t, cgoDB, pureGoDB, query)
    }
}
```

### 3. Benchmark Tests

#### Micro-benchmarks (`benchmark/micro/`)

```go
// scan_bench_test.go
func BenchmarkSequentialScan(b *testing.B) {
    // Benchmark table scanning
}

func BenchmarkIndexScan(b *testing.B) {
    // Benchmark index scanning
}

// join_bench_test.go
func BenchmarkHashJoin(b *testing.B) {
    // Benchmark hash join performance
}

func BenchmarkMergeJoin(b *testing.B) {
    // Benchmark merge join performance
}
```

#### Query Benchmarks (`benchmark/queries/`)

Standard benchmark queries:

- TPC-H (22 queries)
- TPC-DS (subset of queries)
- Real-world scenarios

```go
// tpch_bench_test.go
func BenchmarkTPCH_Q1(b *testing.B) {
    db := setupTPCHData(b)
    query := `
        SELECT l_returnflag, l_linestatus,
               SUM(l_quantity) as sum_qty,
               SUM(l_extendedprice) as sum_base_price
        FROM lineitem
        WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    `
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        rows, err := db.Query(query)
        // Process results
    }
}
```

#### Performance Comparison Framework

```go
// comparison/compare_test.go
type BenchmarkResult struct {
    Implementation string
    QueryName      string
    Duration       time.Duration
    MemoryUsed     int64
}

func CompareBenchmarks(t *testing.T) {
    results := runBenchmarkSuite()
    generateComparisonReport(results)
}
```

### 4. Fuzzing Tests

Leverage Go's native fuzzing support:

```go
// parser/fuzz_test.go
func FuzzParser(f *testing.F) {
    // Seed corpus from DuckDB fuzzer results
    f.Add("SELECT * FROM users")
    f.Add("SELECT 1 + 1")
    
    f.Fuzz(func(t *testing.T, sql string) {
        parser := NewParser()
        _, err := parser.Parse(sql)
        if err != nil {
            // Ensure no panics, only errors
            return
        }
    })
}

// types/fuzz_test.go
func FuzzTypeConversion(f *testing.F) {
    f.Fuzz(func(t *testing.T, data []byte) {
        // Test type conversion edge cases
    })
}
```

### 5. Property-Based Tests

Using gopter or similar libraries:

```go
// properties/query_properties_test.go
func TestQueryProperties(t *testing.T) {
    properties := gopter.NewProperties(nil)
    
    properties.Property("SELECT with WHERE is subset", prop.ForAll(
        func(table string, condition string) bool {
            // Verify that WHERE clause reduces result set
            return true
        },
        genTableName(),
        genCondition(),
    ))
}
```

## Test Data Management

### Test Data Sources

1. **DuckDB Test Data**: Reuse CSV, Parquet, and JSON files from DuckDB's test suite
1. **Generated Data**: Use data generators for large-scale testing
1. **Edge Cases**: Specific files for boundary conditions

### Test Database Setup

```go
// testutil/setup.go
type TestDB struct {
    *sql.DB
    tempDir string
}

func NewTestDB(t *testing.T) *TestDB {
    // Create temporary database
    // Load test schemas
    // Return configured DB
}

func (db *TestDB) LoadTPCH(scale float64) error {
    // Load TPC-H data at specified scale
}

func (db *TestDB) LoadTestData(dataSet string) error {
    // Load predefined test datasets
}
```

## Continuous Integration

### Test Stages

1. **Fast Tests** (< 1 minute)

   - Unit tests
   - Small integration tests
   - Basic SQL logic tests

1. **Standard Tests** (< 10 minutes)

   - All SQL logic tests
   - Integration tests
   - Short benchmarks

1. **Extended Tests** (< 1 hour)

   - Full benchmark suite
   - Compatibility tests
   - Large data tests

1. **Nightly Tests**

   - Fuzzing runs
   - Memory leak detection
   - Performance regression tests

### Test Matrix

| Platform | Go Version | Test Suite |
|----------|------------|------------|
| Linux | 1.21 | Full |
| Linux | 1.22 | Full |
| macOS | 1.22 | Full |
| Windows | 1.22 | Standard |

### Performance Tracking

```yaml
# .github/workflows/benchmark.yml
name: Performance Benchmarks
on:
  push:
    branches: [main]
  pull_request:

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
      - name: Run benchmarks
        run: go test -bench=. -benchmem ./... > benchmark.txt
      - name: Compare with baseline
        uses: benchmark-action/github-action-benchmark@v1
        with:
          tool: 'go'
          output-file-path: benchmark.txt
          alert-threshold: '150%'
```

## Test Coverage Requirements

### Minimum Coverage Targets

- Overall: 80%
- Core packages: 90%
  - `parser/`: 95%
  - `types/`: 90%
  - `execution/`: 85%
  - `storage/`: 85%

### Coverage Tracking

```bash
# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

# Check coverage meets requirements
go run scripts/check_coverage.go --min-coverage=80
```

## Testing Tools and Infrastructure

### Required Tools

1. **Go Testing**: Standard library + testify for assertions
1. **SQL Logic Test Runner**: Custom implementation
1. **Benchmark Framework**: Standard Go benchmarks + comparison tools
1. **Fuzzing**: Go 1.18+ native fuzzing
1. **Property Testing**: gopter or rapid
1. **Load Testing**: Custom harness for concurrent queries

### Test Helpers

```go
// testutil/assertions.go
func AssertQueryResults(t *testing.T, db *sql.DB, query string, expected [][]interface{}) {
    // Helper for query result validation
}

func AssertPanic(t *testing.T, fn func()) {
    // Helper for panic testing
}

// testutil/generators.go
func GenerateRandomTable(columns int, rows int) *Table {
    // Generate random test data
}

func GenerateQuery(complexity int) string {
    // Generate random valid SQL
}
```

## Implementation Timeline

### Phase 1: Foundation (Weeks 1-2)

- Set up test infrastructure
- Implement SQL Logic Test runner
- Create basic test suites for parser and types

### Phase 2: Core Tests (Weeks 3-6)

- Port essential DuckDB SQL logic tests
- Implement unit tests for all components
- Create benchmark framework

### Phase 3: Compatibility (Weeks 7-8)

- Set up CGO comparison tests
- Implement behavior validation
- Document any intentional differences

### Phase 4: Advanced Testing (Weeks 9-12)

- Add fuzzing tests
- Implement property-based tests
- Create performance regression suite

### Phase 5: Continuous Improvement

- Monitor test coverage
- Add tests for reported issues
- Expand benchmark suite
- Optimize based on profiling

## Success Criteria

The testing strategy will be considered successful when:

1. **Coverage**: Achieving 80%+ code coverage with critical paths at 90%+
1. **Compatibility**: Passing 95%+ of applicable DuckDB SQL logic tests
1. **Performance**: Benchmarks show acceptable performance (within 2x of CGO version for most queries)
1. **Reliability**: No crashes in 24-hour fuzzing runs
1. **Deployment**: Successful static binary deployment across platforms

## Maintenance and Evolution

### Test Maintenance

- Regular updates to match DuckDB's evolving test suite
- Continuous addition of regression tests
- Performance baseline updates
- Documentation of test patterns

### Test Review Process

1. All new features require corresponding tests
1. Bug fixes require regression tests
1. Performance changes require benchmark updates
1. API changes require compatibility test updates

This comprehensive testing strategy ensures that the pure-Go DuckDB implementation maintains high quality, compatibility, and performance standards throughout its development and evolution.
