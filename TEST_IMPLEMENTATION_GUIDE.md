# DuckDB-Go Test Implementation Guide

This guide provides detailed implementation patterns and examples for testing the pure-Go DuckDB implementation.

## Test Package Structure

```
dukdb-go/
├── internal/
│   ├── parser/
│   │   ├── parser_test.go
│   │   ├── parser_fuzz_test.go
│   │   └── testdata/
│   ├── planner/
│   │   └── planner_test.go
│   ├── execution/
│   │   ├── operators_test.go
│   │   └── vectorized_test.go
│   ├── storage/
│   │   ├── columnar_test.go
│   │   └── compression_test.go
│   └── types/
│       ├── types_test.go
│       └── conversion_test.go
├── test/
│   ├── sqllogictest/
│   │   ├── runner.go
│   │   ├── runner_test.go
│   │   └── testdata/
│   ├── compatibility/
│   │   ├── cgo_comparison_test.go
│   │   └── behavior_test.go
│   ├── integration/
│   │   ├── tpch_test.go
│   │   └── real_world_test.go
│   └── testutil/
│       ├── db.go
│       ├── assertions.go
│       └── generators.go
├── benchmark/
│   ├── micro/
│   │   ├── scan_bench_test.go
│   │   └── join_bench_test.go
│   ├── queries/
│   │   ├── tpch_bench_test.go
│   │   └── tpcds_bench_test.go
│   └── comparison/
│       └── cgo_comparison_bench_test.go
└── scripts/
    ├── test_runner.sh
    ├── coverage_check.go
    └── benchmark_compare.go
```

## Core Test Categories

### 1. Parser Tests

#### Basic Parsing Tests

```go
// internal/parser/parser_test.go
package parser

import (
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestParseSimpleQueries(t *testing.T) {
    tests := []struct {
        name    string
        sql     string
        wantErr bool
        validate func(t *testing.T, stmt Statement)
    }{
        {
            name: "simple SELECT",
            sql:  "SELECT 1",
            validate: func(t *testing.T, stmt Statement) {
                sel, ok := stmt.(*SelectStatement)
                require.True(t, ok)
                assert.Len(t, sel.Projections, 1)
            },
        },
        {
            name: "SELECT with FROM",
            sql:  "SELECT * FROM users WHERE id = 1",
            validate: func(t *testing.T, stmt Statement) {
                sel, ok := stmt.(*SelectStatement)
                require.True(t, ok)
                assert.Equal(t, "users", sel.From.TableName)
                assert.NotNil(t, sel.Where)
            },
        },
        {
            name: "complex JOIN",
            sql: `SELECT u.name, o.total 
                  FROM users u 
                  JOIN orders o ON u.id = o.user_id 
                  WHERE o.status = 'completed'`,
            validate: func(t *testing.T, stmt Statement) {
                sel, ok := stmt.(*SelectStatement)
                require.True(t, ok)
                assert.NotNil(t, sel.From.Join)
                assert.Equal(t, JoinTypeInner, sel.From.Join.Type)
            },
        },
    }

    parser := NewParser()
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            stmt, err := parser.Parse(tt.sql)
            if tt.wantErr {
                assert.Error(t, err)
                return
            }
            require.NoError(t, err)
            tt.validate(t, stmt)
        })
    }
}

func TestParseDuckDBSpecificSyntax(t *testing.T) {
    tests := []struct {
        name string
        sql  string
    }{
        {
            name: "LIST type",
            sql:  "SELECT [1, 2, 3] AS numbers",
        },
        {
            name: "STRUCT type",
            sql:  "SELECT {'name': 'John', 'age': 30} AS person",
        },
        {
            name: "LIST functions",
            sql:  "SELECT list_sum([1, 2, 3, 4, 5])",
        },
        {
            name: "EXCLUDE clause",
            sql:  "SELECT * EXCLUDE (password) FROM users",
        },
        {
            name: "COLUMNS expression",
            sql:  "SELECT COLUMNS('user_.*') FROM users",
        },
    }

    parser := NewParser()
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            _, err := parser.Parse(tt.sql)
            assert.NoError(t, err, "Failed to parse DuckDB-specific syntax")
        })
    }
}
```

#### Parser Error Tests

```go
func TestParserErrors(t *testing.T) {
    tests := []struct {
        name        string
        sql         string
        errContains string
    }{
        {
            name:        "incomplete SELECT",
            sql:         "SELECT FROM",
            errContains: "unexpected FROM",
        },
        {
            name:        "invalid syntax",
            sql:         "SELEKT * FROM users",
            errContains: "unexpected token SELEKT",
        },
        {
            name:        "unclosed string",
            sql:         "SELECT 'unclosed string FROM users",
            errContains: "unterminated string",
        },
    }

    parser := NewParser()
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            _, err := parser.Parse(tt.sql)
            require.Error(t, err)
            assert.Contains(t, err.Error(), tt.errContains)
        })
    }
}
```

### 2. Type System Tests

```go
// internal/types/types_test.go
package types

import (
    "testing"
    "math/big"
)

func TestHugeIntOperations(t *testing.T) {
    tests := []struct {
        name     string
        left     string
        right    string
        op       string
        expected string
    }{
        {
            name:     "addition",
            left:     "170141183460469231731687303715884105727",
            right:    "1",
            op:       "+",
            expected: "170141183460469231731687303715884105728",
        },
        {
            name:     "multiplication",
            left:     "123456789012345678901234567890",
            right:    "2",
            op:       "*",
            expected: "246913578024691357802469135780",
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            left, _ := new(big.Int).SetString(tt.left, 10)
            right, _ := new(big.Int).SetString(tt.right, 10)
            
            hugeLeft := NewHugeInt(left)
            hugeRight := NewHugeInt(right)
            
            var result Value
            switch tt.op {
            case "+":
                result = hugeLeft.Add(hugeRight)
            case "*":
                result = hugeLeft.Multiply(hugeRight)
            }
            
            assert.Equal(t, tt.expected, result.String())
        })
    }
}

func TestListTypeOperations(t *testing.T) {
    tests := []struct {
        name     string
        list     []Value
        op       string
        expected interface{}
    }{
        {
            name:     "list_sum integers",
            list:     []Value{NewInt64(1), NewInt64(2), NewInt64(3)},
            op:       "sum",
            expected: int64(6),
        },
        {
            name:     "list_contains",
            list:     []Value{NewString("a"), NewString("b"), NewString("c")},
            op:       "contains:b",
            expected: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            listVal := NewList(tt.list)
            
            switch {
            case tt.op == "sum":
                result := listVal.Sum()
                assert.Equal(t, tt.expected, result.GetInt64())
            case strings.HasPrefix(tt.op, "contains:"):
                search := tt.op[9:]
                result := listVal.Contains(NewString(search))
                assert.Equal(t, tt.expected, result)
            }
        })
    }
}
```

### 3. SQL Logic Tests

#### Test Runner Implementation

```go
// test/sqllogictest/runner.go
package sqllogictest

import (
    "bufio"
    "database/sql"
    "fmt"
    "strings"
)

type TestCase struct {
    Type     string   // "statement" or "query"
    SQL      string
    Expected string   // "ok", "error", or query results
    Columns  string   // Column types for query results
}

type Runner struct {
    db *sql.DB
}

func (r *Runner) RunFile(filename string) error {
    file, err := os.Open(filename)
    if err != nil {
        return err
    }
    defer file.Close()

    tests, err := r.parseFile(file)
    if err != nil {
        return err
    }

    for i, test := range tests {
        if err := r.runTest(test); err != nil {
            return fmt.Errorf("test %d failed: %w", i+1, err)
        }
    }
    return nil
}

func (r *Runner) runTest(test TestCase) error {
    switch test.Type {
    case "statement":
        return r.runStatement(test)
    case "query":
        return r.runQuery(test)
    default:
        return fmt.Errorf("unknown test type: %s", test.Type)
    }
}

func (r *Runner) runStatement(test TestCase) error {
    _, err := r.db.Exec(test.SQL)
    
    switch test.Expected {
    case "ok":
        if err != nil {
            return fmt.Errorf("expected success, got error: %w", err)
        }
    case "error":
        if err == nil {
            return fmt.Errorf("expected error, but statement succeeded")
        }
    default:
        if err != nil && !strings.Contains(err.Error(), test.Expected) {
            return fmt.Errorf("expected error containing '%s', got: %w", test.Expected, err)
        }
    }
    return nil
}

func (r *Runner) runQuery(test TestCase) error {
    rows, err := r.db.Query(test.SQL)
    if err != nil {
        return fmt.Errorf("query failed: %w", err)
    }
    defer rows.Close()

    results := r.fetchResults(rows)
    expected := strings.TrimSpace(test.Expected)
    actual := strings.Join(results, "\n")

    if actual != expected {
        return fmt.Errorf("result mismatch:\nExpected:\n%s\nActual:\n%s", expected, actual)
    }
    return nil
}
```

#### SQL Logic Test Example

```sql
# test/sqllogictest/testdata/types/hugeint.test
# Test HUGEINT type functionality

statement ok
CREATE TABLE huge_numbers (id INTEGER, value HUGEINT)

statement ok
INSERT INTO huge_numbers VALUES 
  (1, 170141183460469231731687303715884105727),
  (2, -170141183460469231731687303715884105728),
  (3, 0)

query II
SELECT id, value FROM huge_numbers ORDER BY id
----
1 170141183460469231731687303715884105727
2 -170141183460469231731687303715884105728
3 0

query I
SELECT value + 1 FROM huge_numbers WHERE id = 1
----
170141183460469231731687303715884105728

statement error
SELECT CAST(value AS INTEGER) FROM huge_numbers WHERE id = 1
----
Conversion Error: HUGEINT value out of range for INTEGER

query T
SELECT typeof(value) FROM huge_numbers LIMIT 1
----
HUGEINT
```

### 4. Compatibility Tests

```go
// test/compatibility/cgo_comparison_test.go
package compatibility

import (
    "database/sql"
    "testing"
    
    _ "github.com/marcboeker/go-duckdb"      // CGO version
    _ "github.com/yourusername/dukdb-go"     // Pure-Go version
)

type QueryTest struct {
    Name  string
    Setup []string
    Query string
    Args  []interface{}
}

func TestCompatibilityWithCGO(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping compatibility test in short mode")
    }

    tests := []QueryTest{
        {
            Name: "Basic SELECT",
            Query: "SELECT 1 + 1 AS result",
        },
        {
            Name: "Table operations",
            Setup: []string{
                "CREATE TABLE users (id INTEGER, name VARCHAR)",
                "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
            },
            Query: "SELECT * FROM users ORDER BY id",
        },
        {
            Name: "Complex types",
            Query: "SELECT [1, 2, 3] AS list, {'a': 1, 'b': 2} AS struct",
        },
        {
            Name: "Window functions",
            Setup: []string{
                "CREATE TABLE sales (date DATE, amount DECIMAL)",
                "INSERT INTO sales VALUES ('2024-01-01', 100), ('2024-01-02', 200)",
            },
            Query: "SELECT date, amount, SUM(amount) OVER (ORDER BY date) FROM sales",
        },
    }

    for _, test := range tests {
        t.Run(test.Name, func(t *testing.T) {
            compareBehavior(t, test)
        })
    }
}

func compareBehavior(t *testing.T, test QueryTest) {
    // Open both database connections
    cgoDB, err := sql.Open("duckdb", ":memory:")
    require.NoError(t, err)
    defer cgoDB.Close()

    pureGoDB, err := sql.Open("dukdb", ":memory:")
    require.NoError(t, err)
    defer pureGoDB.Close()

    // Run setup for both
    for _, setup := range test.Setup {
        _, err = cgoDB.Exec(setup)
        require.NoError(t, err, "CGO setup failed")
        
        _, err = pureGoDB.Exec(setup)
        require.NoError(t, err, "Pure-Go setup failed")
    }

    // Execute query on both
    cgoRows, err := cgoDB.Query(test.Query, test.Args...)
    require.NoError(t, err, "CGO query failed")
    defer cgoRows.Close()

    pureGoRows, err := pureGoDB.Query(test.Query, test.Args...)
    require.NoError(t, err, "Pure-Go query failed")
    defer pureGoRows.Close()

    // Compare results
    compareResults(t, cgoRows, pureGoRows)
}

func compareResults(t *testing.T, cgoRows, pureGoRows *sql.Rows) {
    // Get column information
    cgoCols, err := cgoRows.Columns()
    require.NoError(t, err)
    
    pureGoCols, err := pureGoRows.Columns()
    require.NoError(t, err)
    
    assert.Equal(t, cgoCols, pureGoCols, "Column names don't match")

    // Compare row by row
    rowNum := 0
    for cgoRows.Next() {
        require.True(t, pureGoRows.Next(), "Pure-Go has fewer rows")
        
        cgoVals := make([]interface{}, len(cgoCols))
        pureGoVals := make([]interface{}, len(pureGoCols))
        
        cgoPtrs := make([]interface{}, len(cgoCols))
        pureGoPtrs := make([]interface{}, len(pureGoCols))
        
        for i := range cgoCols {
            cgoPtrs[i] = &cgoVals[i]
            pureGoPtrs[i] = &pureGoVals[i]
        }
        
        err = cgoRows.Scan(cgoPtrs...)
        require.NoError(t, err)
        
        err = pureGoRows.Scan(pureGoPtrs...)
        require.NoError(t, err)
        
        for i := range cgoVals {
            assert.Equal(t, cgoVals[i], pureGoVals[i], 
                "Row %d, Column %d (%s) mismatch", rowNum, i, cgoCols[i])
        }
        
        rowNum++
    }
    
    assert.False(t, pureGoRows.Next(), "Pure-Go has more rows")
}
```

### 5. Benchmark Tests

#### Micro-benchmarks

```go
// benchmark/micro/scan_bench_test.go
package micro

import (
    "testing"
    "github.com/yourusername/dukdb-go/internal/storage"
)

func BenchmarkSequentialScan(b *testing.B) {
    sizes := []int{1000, 10000, 100000, 1000000}
    
    for _, size := range sizes {
        b.Run(fmt.Sprintf("rows_%d", size), func(b *testing.B) {
            table := generateTable(size, 5) // 5 columns
            
            b.ResetTimer()
            b.ReportAllocs()
            
            for i := 0; i < b.N; i++ {
                scanner := storage.NewScanner(table)
                count := 0
                for scanner.Next() {
                    _ = scanner.Row()
                    count++
                }
                if count != size {
                    b.Fatalf("Expected %d rows, got %d", size, count)
                }
            }
            
            b.SetBytes(int64(size * 5 * 8)) // Assuming 8 bytes per value
        })
    }
}

func BenchmarkFilteredScan(b *testing.B) {
    table := generateTable(100000, 5)
    predicate := func(row []interface{}) bool {
        return row[0].(int64) > 50000
    }
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        scanner := storage.NewFilteredScanner(table, predicate)
        count := 0
        for scanner.Next() {
            count++
        }
    }
}
```

#### Query Benchmarks

```go
// benchmark/queries/tpch_bench_test.go
package queries

import (
    "database/sql"
    "testing"
)

type BenchmarkSuite struct {
    db *sql.DB
    scale float64
}

func setupBenchmark(b *testing.B, scale float64) *BenchmarkSuite {
    db, err := sql.Open("dukdb", ":memory:")
    if err != nil {
        b.Fatal(err)
    }
    
    suite := &BenchmarkSuite{db: db, scale: scale}
    suite.loadTPCHData()
    
    return suite
}

func BenchmarkTPCH(b *testing.B) {
    scales := []float64{0.01, 0.1, 1.0}
    
    for _, scale := range scales {
        b.Run(fmt.Sprintf("SF%.2f", scale), func(b *testing.B) {
            suite := setupBenchmark(b, scale)
            defer suite.db.Close()
            
            b.Run("Q1_pricing_summary", func(b *testing.B) {
                benchmarkQuery(b, suite.db, tpchQueries["Q1"])
            })
            
            b.Run("Q6_forecast_revenue", func(b *testing.B) {
                benchmarkQuery(b, suite.db, tpchQueries["Q6"])
            })
            
            // Add more TPC-H queries...
        })
    }
}

func benchmarkQuery(b *testing.B, db *sql.DB, query string) {
    // Warm up
    rows, err := db.Query(query)
    if err != nil {
        b.Fatal(err)
    }
    consumeRows(rows)
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        rows, err := db.Query(query)
        if err != nil {
            b.Fatal(err)
        }
        consumeRows(rows)
    }
}

var tpchQueries = map[string]string{
    "Q1": `
        SELECT
            l_returnflag,
            l_linestatus,
            sum(l_quantity) as sum_qty,
            sum(l_extendedprice) as sum_base_price,
            sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
            sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
            avg(l_quantity) as avg_qty,
            avg(l_extendedprice) as avg_price,
            avg(l_discount) as avg_disc,
            count(*) as count_order
        FROM
            lineitem
        WHERE
            l_shipdate <= date '1998-12-01' - interval '90' day
        GROUP BY
            l_returnflag,
            l_linestatus
        ORDER BY
            l_returnflag,
            l_linestatus
    `,
    // More queries...
}
```

### 6. Fuzzing Tests

```go
// internal/parser/parser_fuzz_test.go
//go:build go1.18

package parser

import (
    "testing"
)

func FuzzParser(f *testing.F) {
    // Add seed corpus from known queries
    seedQueries := []string{
        "SELECT 1",
        "SELECT * FROM users",
        "INSERT INTO t VALUES (1, 'test')",
        "CREATE TABLE t (id INT, name TEXT)",
        "SELECT COUNT(*) FROM users GROUP BY status",
        // Add more complex queries...
    }
    
    for _, query := range seedQueries {
        f.Add(query)
    }
    
    // Also add some known problematic inputs
    f.Add("SELECT 'unterminated string")
    f.Add("SELECT * FROM")
    f.Add(";;;")
    
    f.Fuzz(func(t *testing.T, input string) {
        parser := NewParser()
        
        // Should not panic
        _, err := parser.Parse(input)
        
        // If parsing succeeds, try to validate the AST
        if err == nil {
            // Additional validation could go here
        }
    })
}

func FuzzTypeConversion(f *testing.F) {
    // Seed with various type conversion scenarios
    f.Add("123", "INTEGER")
    f.Add("123.45", "DOUBLE")
    f.Add("true", "BOOLEAN")
    f.Add("2024-01-01", "DATE")
    
    f.Fuzz(func(t *testing.T, value string, targetType string) {
        // Test type conversion without panics
        result, err := ConvertStringToType(value, targetType)
        
        if err == nil && result != nil {
            // Verify round-trip conversion
            str := result.String()
            _, err2 := ConvertStringToType(str, targetType)
            if err2 != nil {
                t.Errorf("Round-trip conversion failed: %v -> %v -> error", value, str)
            }
        }
    })
}
```

## Test Utilities

### Database Setup Helper

```go
// test/testutil/db.go
package testutil

import (
    "database/sql"
    "testing"
    "io/ioutil"
    "path/filepath"
)

type TestDB struct {
    *sql.DB
    t       *testing.T
    tempDir string
}

func NewTestDB(t *testing.T) *TestDB {
    t.Helper()
    
    tempDir, err := ioutil.TempDir("", "dukdb-test-*")
    require.NoError(t, err)
    
    db, err := sql.Open("dukdb", ":memory:")
    require.NoError(t, err)
    
    testDB := &TestDB{
        DB:      db,
        t:       t,
        tempDir: tempDir,
    }
    
    t.Cleanup(func() {
        db.Close()
        os.RemoveAll(tempDir)
    })
    
    return testDB
}

func (db *TestDB) LoadCSV(tableName, csvPath string) {
    db.t.Helper()
    
    query := fmt.Sprintf(`
        CREATE TABLE %s AS 
        SELECT * FROM read_csv_auto('%s')
    `, tableName, csvPath)
    
    _, err := db.Exec(query)
    require.NoError(db.t, err)
}

func (db *TestDB) MustExec(query string, args ...interface{}) {
    db.t.Helper()
    _, err := db.Exec(query, args...)
    require.NoError(db.t, err)
}

func (db *TestDB) QueryJSON(query string, args ...interface{}) []map[string]interface{} {
    db.t.Helper()
    
    rows, err := db.Query(query, args...)
    require.NoError(db.t, err)
    defer rows.Close()
    
    return RowsToJSON(db.t, rows)
}
```

### Assertion Helpers

```go
// test/testutil/assertions.go
package testutil

import (
    "database/sql"
    "testing"
    "reflect"
)

func AssertQuery(t *testing.T, db *sql.DB, query string, expected [][]interface{}) {
    t.Helper()
    
    rows, err := db.Query(query)
    require.NoError(t, err)
    defer rows.Close()
    
    actual := ScanAllRows(t, rows)
    assert.Equal(t, expected, actual, "Query results don't match")
}

func AssertQueryError(t *testing.T, db *sql.DB, query, errContains string) {
    t.Helper()
    
    _, err := db.Query(query)
    require.Error(t, err)
    assert.Contains(t, err.Error(), errContains)
}

func AssertEqualResults(t *testing.T, expected, actual *sql.Rows) {
    t.Helper()
    
    expectedCols, err := expected.Columns()
    require.NoError(t, err)
    
    actualCols, err := actual.Columns()
    require.NoError(t, err)
    
    assert.Equal(t, expectedCols, actualCols, "Column names don't match")
    
    expectedRows := ScanAllRows(t, expected)
    actualRows := ScanAllRows(t, actual)
    
    assert.Equal(t, expectedRows, actualRows, "Row data doesn't match")
}

func ScanAllRows(t *testing.T, rows *sql.Rows) [][]interface{} {
    t.Helper()
    
    cols, err := rows.Columns()
    require.NoError(t, err)
    
    var results [][]interface{}
    
    for rows.Next() {
        values := make([]interface{}, len(cols))
        valuePtrs := make([]interface{}, len(cols))
        
        for i := range values {
            valuePtrs[i] = &values[i]
        }
        
        err := rows.Scan(valuePtrs...)
        require.NoError(t, err)
        
        results = append(results, values)
    }
    
    require.NoError(t, rows.Err())
    return results
}
```

## Test Execution Scripts

### Main Test Runner

```bash
#!/bin/bash
# scripts/test_runner.sh

set -e

echo "Running DuckDB-Go Test Suite"
echo "==========================="

# Unit tests
echo "Running unit tests..."
go test -v -race ./internal/...

# Integration tests
echo "Running integration tests..."
go test -v ./test/integration/...

# SQL Logic tests
echo "Running SQL logic tests..."
go test -v ./test/sqllogictest/...

# Compatibility tests (if CGO version available)
if command -v duckdb &> /dev/null; then
    echo "Running compatibility tests..."
    go test -v ./test/compatibility/...
else
    echo "Skipping compatibility tests (CGO DuckDB not found)"
fi

# Benchmarks (quick mode)
echo "Running quick benchmarks..."
go test -bench=. -benchtime=10x ./benchmark/micro/...

# Coverage report
echo "Generating coverage report..."
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

echo "Test suite completed!"
```

### Coverage Check

```go
// scripts/coverage_check.go
package main

import (
    "flag"
    "fmt"
    "os"
    "os/exec"
    "regexp"
    "strconv"
)

func main() {
    minCoverage := flag.Float64("min-coverage", 80.0, "Minimum coverage percentage")
    flag.Parse()

    cmd := exec.Command("go", "test", "-cover", "./...")
    output, err := cmd.CombinedOutput()
    if err != nil {
        fmt.Printf("Error running tests: %v\n", err)
        os.Exit(1)
    }

    re := regexp.MustCompile(`coverage: (\d+\.\d+)% of statements`)
    matches := re.FindAllStringSubmatch(string(output), -1)

    if len(matches) == 0 {
        fmt.Println("Could not parse coverage output")
        os.Exit(1)
    }

    totalCoverage := 0.0
    for _, match := range matches {
        coverage, _ := strconv.ParseFloat(match[1], 64)
        totalCoverage += coverage
    }
    avgCoverage := totalCoverage / float64(len(matches))

    fmt.Printf("Average coverage: %.1f%%\n", avgCoverage)

    if avgCoverage < *minCoverage {
        fmt.Printf("Coverage %.1f%% is below minimum %.1f%%\n", avgCoverage, *minCoverage)
        os.Exit(1)
    }

    fmt.Println("Coverage check passed!")
}
```

## Continuous Integration Configuration

```yaml
# .github/workflows/test.yml
name: Tests

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        go: ['1.21', '1.22']
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Go
      uses: actions/setup-go@v4
      with:
        go-version: ${{ matrix.go }}
    
    - name: Install dependencies
      run: go mod download
    
    - name: Run tests
      run: go test -v -race -coverprofile=coverage.out ./...
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.out
    
    - name: Run benchmarks
      run: go test -bench=. -benchtime=10x ./benchmark/micro/...

  compatibility:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Install DuckDB CGO
      run: |
        # Install CGO DuckDB for compatibility testing
        go install github.com/marcboeker/go-duckdb@latest
    
    - name: Run compatibility tests
      run: go test -v ./test/compatibility/...

  sqllogic:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Download DuckDB test files
      run: |
        # Download SQL logic test files from DuckDB repo
        ./scripts/download_sqllogic_tests.sh
    
    - name: Run SQL logic tests
      run: go test -v -timeout=30m ./test/sqllogictest/...
```

This comprehensive testing implementation guide provides a solid foundation for ensuring the pure-Go DuckDB implementation is thoroughly tested and maintains compatibility with the original DuckDB.
