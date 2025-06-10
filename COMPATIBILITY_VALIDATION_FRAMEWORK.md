# DuckDB-Go Compatibility Validation Framework

This framework ensures the pure-Go DuckDB implementation maintains behavioral compatibility with the CGO-based DuckDB driver.

## Overview

The compatibility validation framework consists of:
1. **Behavioral Test Suite**: Comparing query results between implementations
2. **API Compatibility Layer**: Ensuring database/sql interface compliance
3. **Performance Comparison**: Tracking relative performance metrics
4. **Feature Parity Matrix**: Documenting supported vs unsupported features
5. **Migration Testing**: Validating data migration between implementations

## Behavioral Test Suite

### Core Compatibility Tests

```go
// test/compatibility/behavioral_test.go
package compatibility

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "reflect"
    "testing"
    
    _ "github.com/marcboeker/go-duckdb"  // CGO implementation
    _ "github.com/yourusername/dukdb-go" // Pure-Go implementation
)

type CompatibilityTest struct {
    Name        string
    Description string
    Category    string
    Setup       []string
    Query       string
    Params      []interface{}
    Cleanup     []string
    
    // Expected behavior
    ExpectError bool
    ErrorMatch  string
    
    // Feature flags
    RequiredFeatures []string
}

var compatibilityTests = []CompatibilityTest{
    // Basic Operations
    {
        Name:     "basic_arithmetic",
        Category: "core",
        Query:    "SELECT 1 + 1 AS result",
    },
    {
        Name:     "string_concatenation",
        Category: "core",
        Query:    "SELECT 'Hello' || ' ' || 'World' AS greeting",
    },
    {
        Name:     "null_handling",
        Category: "core",
        Query:    "SELECT NULL + 1, NULL || 'test', NULL IS NULL",
    },
    
    // Data Types
    {
        Name:     "integer_types",
        Category: "types",
        Setup: []string{
            "CREATE TABLE int_test (t TINYINT, s SMALLINT, i INTEGER, b BIGINT)",
            "INSERT INTO int_test VALUES (127, 32767, 2147483647, 9223372036854775807)",
        },
        Query: "SELECT * FROM int_test",
    },
    {
        Name:     "decimal_types",
        Category: "types",
        Query:    "SELECT 123.456::DECIMAL(10,3), 999.999::DECIMAL(6,3)",
    },
    {
        Name:     "hugeint_type",
        Category: "types",
        Query:    "SELECT 170141183460469231731687303715884105727::HUGEINT",
    },
    {
        Name:     "list_type",
        Category: "types",
        Query:    "SELECT [1, 2, 3], ['a', 'b', 'c'], [NULL, 1, NULL]",
    },
    {
        Name:     "struct_type",
        Category: "types",
        Query:    "SELECT {'name': 'John', 'age': 30, 'active': true}",
    },
    {
        Name:     "map_type",
        Category: "types",
        Query:    "SELECT MAP(['a', 'b', 'c'], [1, 2, 3])",
        RequiredFeatures: []string{"map_type"},
    },
    
    // Functions
    {
        Name:     "string_functions",
        Category: "functions",
        Query:    "SELECT LENGTH('test'), UPPER('hello'), SUBSTRING('world', 2, 3)",
    },
    {
        Name:     "date_functions",
        Category: "functions",
        Query:    "SELECT CURRENT_DATE, DATE '2024-01-01' + INTERVAL '1' MONTH",
    },
    {
        Name:     "list_functions",
        Category: "functions",
        Query:    "SELECT LIST_SUM([1, 2, 3, 4, 5]), LIST_CONTAINS([1, 2, 3], 2)",
    },
    {
        Name:     "window_functions",
        Category: "functions",
        Setup: []string{
            "CREATE TABLE sales (id INT, amount DECIMAL, date DATE)",
            "INSERT INTO sales VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-02'), (3, 150, '2024-01-03')",
        },
        Query: "SELECT id, amount, SUM(amount) OVER (ORDER BY date) AS running_total FROM sales",
    },
    
    // Joins
    {
        Name:     "inner_join",
        Category: "joins",
        Setup: []string{
            "CREATE TABLE users (id INT, name VARCHAR)",
            "CREATE TABLE orders (id INT, user_id INT, total DECIMAL)",
            "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
            "INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)",
        },
        Query: "SELECT u.name, SUM(o.total) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
    },
    {
        Name:     "left_join_with_nulls",
        Category: "joins",
        Setup: []string{
            "CREATE TABLE t1 (id INT, val VARCHAR)",
            "CREATE TABLE t2 (id INT, data VARCHAR)",
            "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')",
            "INSERT INTO t2 VALUES (1, 'x'), (3, 'z')",
        },
        Query: "SELECT t1.*, t2.data FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY t1.id",
    },
    
    // Aggregations
    {
        Name:     "basic_aggregations",
        Category: "aggregations",
        Setup: []string{
            "CREATE TABLE numbers (n INT)",
            "INSERT INTO numbers VALUES (1), (2), (3), (4), (5), (NULL)",
        },
        Query: "SELECT COUNT(*), COUNT(n), SUM(n), AVG(n), MIN(n), MAX(n) FROM numbers",
    },
    {
        Name:     "group_by_having",
        Category: "aggregations",
        Setup: []string{
            "CREATE TABLE sales (category VARCHAR, amount DECIMAL)",
            "INSERT INTO sales VALUES ('A', 100), ('A', 200), ('B', 150), ('B', 250), ('C', 50)",
        },
        Query: "SELECT category, SUM(amount) AS total FROM sales GROUP BY category HAVING SUM(amount) > 100",
    },
    
    // CTEs
    {
        Name:     "simple_cte",
        Category: "cte",
        Query: `WITH nums AS (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3)
                SELECT n * n FROM nums`,
    },
    {
        Name:     "recursive_cte",
        Category: "cte",
        Query: `WITH RECURSIVE seq AS (
                    SELECT 1 AS n
                    UNION ALL
                    SELECT n + 1 FROM seq WHERE n < 5
                )
                SELECT * FROM seq`,
        RequiredFeatures: []string{"recursive_cte"},
    },
    
    // Error Cases
    {
        Name:        "division_by_zero",
        Category:    "errors",
        Query:       "SELECT 1 / 0",
        ExpectError: true,
        ErrorMatch:  "division by zero",
    },
    {
        Name:        "type_mismatch",
        Category:    "errors",
        Query:       "SELECT 'abc' + 123",
        ExpectError: true,
        ErrorMatch:  "type mismatch",
    },
}

func TestBehavioralCompatibility(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping behavioral compatibility tests")
    }
    
    cgoConn, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        t.Skip("CGO DuckDB not available:", err)
    }
    defer cgoConn.Close()
    
    pureGoConn, err := sql.Open("dukdb", ":memory:")
    if err != nil {
        t.Fatal("Pure-Go DuckDB failed to open:", err)
    }
    defer pureGoConn.Close()
    
    results := runCompatibilityTests(t, cgoConn, pureGoConn)
    generateCompatibilityReport(t, results)
}

func runCompatibilityTests(t *testing.T, cgo, pureGo *sql.DB) []TestResult {
    var results []TestResult
    
    for _, test := range compatibilityTests {
        t.Run(test.Name, func(t *testing.T) {
            result := executeCompatibilityTest(t, test, cgo, pureGo)
            results = append(results, result)
            
            if !result.Compatible {
                t.Errorf("Compatibility mismatch in %s: %s", test.Name, result.Reason)
            }
        })
    }
    
    return results
}

type TestResult struct {
    Test       CompatibilityTest
    Compatible bool
    Reason     string
    CGOResult  QueryResult
    PureResult QueryResult
}

type QueryResult struct {
    Data      [][]interface{}
    Error     error
    Duration  time.Duration
    RowCount  int
    ColCount  int
    ColNames  []string
    ColTypes  []string
}

func executeCompatibilityTest(t *testing.T, test CompatibilityTest, cgo, pureGo *sql.DB) TestResult {
    // Skip if required features not supported
    if !checkRequiredFeatures(test.RequiredFeatures) {
        t.Skip("Required features not available:", test.RequiredFeatures)
    }
    
    // Setup
    for _, stmt := range test.Setup {
        if _, err := cgo.Exec(stmt); err != nil {
            t.Fatalf("CGO setup failed: %v", err)
        }
        if _, err := pureGo.Exec(stmt); err != nil {
            t.Fatalf("Pure-Go setup failed: %v", err)
        }
    }
    
    // Execute query
    cgoResult := executeQuery(cgo, test.Query, test.Params...)
    pureResult := executeQuery(pureGo, test.Query, test.Params...)
    
    // Cleanup
    for _, stmt := range test.Cleanup {
        cgo.Exec(stmt)
        pureGo.Exec(stmt)
    }
    
    // Compare results
    compatible, reason := compareResults(test, cgoResult, pureResult)
    
    return TestResult{
        Test:       test,
        Compatible: compatible,
        Reason:     reason,
        CGOResult:  cgoResult,
        PureResult: pureResult,
    }
}

func compareResults(test CompatibilityTest, cgo, pure QueryResult) (bool, string) {
    // Handle expected errors
    if test.ExpectError {
        if cgo.Error == nil || pure.Error == nil {
            return false, "Expected error but one implementation succeeded"
        }
        if test.ErrorMatch != "" {
            cgoMatch := strings.Contains(cgo.Error.Error(), test.ErrorMatch)
            pureMatch := strings.Contains(pure.Error.Error(), test.ErrorMatch)
            if cgoMatch != pureMatch {
                return false, fmt.Sprintf("Error message mismatch: CGO=%v, Pure=%v", cgo.Error, pure.Error)
            }
        }
        return true, ""
    }
    
    // Check for unexpected errors
    if cgo.Error != nil && pure.Error != nil {
        return false, fmt.Sprintf("Both implementations failed: CGO=%v, Pure=%v", cgo.Error, pure.Error)
    }
    if cgo.Error != nil {
        return false, fmt.Sprintf("CGO failed but Pure-Go succeeded: %v", cgo.Error)
    }
    if pure.Error != nil {
        return false, fmt.Sprintf("Pure-Go failed but CGO succeeded: %v", pure.Error)
    }
    
    // Compare column metadata
    if !reflect.DeepEqual(cgo.ColNames, pure.ColNames) {
        return false, fmt.Sprintf("Column names differ: CGO=%v, Pure=%v", cgo.ColNames, pure.ColNames)
    }
    
    // Compare row counts
    if cgo.RowCount != pure.RowCount {
        return false, fmt.Sprintf("Row count differs: CGO=%d, Pure=%d", cgo.RowCount, pure.RowCount)
    }
    
    // Compare data with type flexibility
    for i := range cgo.Data {
        for j := range cgo.Data[i] {
            if !valuesEqual(cgo.Data[i][j], pure.Data[i][j]) {
                return false, fmt.Sprintf("Data mismatch at [%d][%d]: CGO=%v (%T), Pure=%v (%T)",
                    i, j, cgo.Data[i][j], cgo.Data[i][j], pure.Data[i][j], pure.Data[i][j])
            }
        }
    }
    
    return true, ""
}

func valuesEqual(a, b interface{}) bool {
    // Handle nil
    if a == nil && b == nil {
        return true
    }
    if a == nil || b == nil {
        return false
    }
    
    // Handle numeric type flexibility (int64 vs int32, etc)
    if isNumeric(a) && isNumeric(b) {
        return numericEqual(a, b)
    }
    
    // Handle time types
    if isTime(a) && isTime(b) {
        return timeEqual(a, b)
    }
    
    // Handle byte arrays (for BLOB types)
    if isBytes(a) && isBytes(b) {
        return bytesEqual(a, b)
    }
    
    // Default equality
    return reflect.DeepEqual(a, b)
}
```

### API Compatibility Tests

```go
// test/compatibility/api_test.go
package compatibility

import (
    "context"
    "database/sql"
    "testing"
    "time"
)

func TestDatabaseSQLInterface(t *testing.T) {
    tests := []struct {
        name string
        test func(t *testing.T, db *sql.DB)
    }{
        {"OpenClose", testOpenClose},
        {"Ping", testPing},
        {"Exec", testExec},
        {"Query", testQuery},
        {"QueryRow", testQueryRow},
        {"Prepare", testPrepare},
        {"Transaction", testTransaction},
        {"Context", testContext},
        {"NullTypes", testNullTypes},
        {"NamedParameters", testNamedParameters},
    }
    
    for _, impl := range []string{"duckdb", "dukdb"} {
        t.Run(impl, func(t *testing.T) {
            db, err := sql.Open(impl, ":memory:")
            if err != nil {
                t.Skip("Implementation not available:", impl)
            }
            defer db.Close()
            
            for _, test := range tests {
                t.Run(test.name, func(t *testing.T) {
                    test.test(t, db)
                })
            }
        })
    }
}

func testPrepare(t *testing.T, db *sql.DB) {
    // Create test table
    _, err := db.Exec("CREATE TABLE test (id INTEGER, name VARCHAR)")
    require.NoError(t, err)
    
    // Test prepared insert
    stmt, err := db.Prepare("INSERT INTO test VALUES (?, ?)")
    require.NoError(t, err)
    defer stmt.Close()
    
    for i := 0; i < 5; i++ {
        _, err = stmt.Exec(i, fmt.Sprintf("name_%d", i))
        require.NoError(t, err)
    }
    
    // Test prepared select
    selectStmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
    require.NoError(t, err)
    defer selectStmt.Close()
    
    var id int
    var name string
    err = selectStmt.QueryRow(2).Scan(&id, &name)
    require.NoError(t, err)
    assert.Equal(t, 2, id)
    assert.Equal(t, "name_2", name)
}

func testTransaction(t *testing.T, db *sql.DB) {
    _, err := db.Exec("CREATE TABLE tx_test (id INTEGER)")
    require.NoError(t, err)
    
    // Test commit
    tx, err := db.Begin()
    require.NoError(t, err)
    
    _, err = tx.Exec("INSERT INTO tx_test VALUES (1)")
    require.NoError(t, err)
    
    err = tx.Commit()
    require.NoError(t, err)
    
    var count int
    err = db.QueryRow("SELECT COUNT(*) FROM tx_test").Scan(&count)
    require.NoError(t, err)
    assert.Equal(t, 1, count)
    
    // Test rollback
    tx, err = db.Begin()
    require.NoError(t, err)
    
    _, err = tx.Exec("INSERT INTO tx_test VALUES (2)")
    require.NoError(t, err)
    
    err = tx.Rollback()
    require.NoError(t, err)
    
    err = db.QueryRow("SELECT COUNT(*) FROM tx_test").Scan(&count)
    require.NoError(t, err)
    assert.Equal(t, 1, count) // Should still be 1
}

func testContext(t *testing.T, db *sql.DB) {
    ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
    defer cancel()
    
    // Test query with context
    _, err := db.QueryContext(ctx, "SELECT 1")
    require.NoError(t, err)
    
    // Test cancelled context
    cancel()
    _, err = db.QueryContext(ctx, "SELECT 1")
    assert.Error(t, err)
    assert.Contains(t, err.Error(), "context")
}
```

### Performance Comparison Framework

```go
// test/compatibility/performance_test.go
package compatibility

import (
    "database/sql"
    "fmt"
    "testing"
    "time"
)

type BenchmarkResult struct {
    Implementation string
    TestName       string
    Operations     int
    Duration       time.Duration
    OpsPerSecond   float64
    MemoryUsed     int64
    Allocations    int64
}

func BenchmarkCompatibility(b *testing.B) {
    benchmarks := []struct {
        name  string
        setup func(*sql.DB)
        bench func(*testing.B, *sql.DB)
    }{
        {
            name: "SimpleSelect",
            bench: func(b *testing.B, db *sql.DB) {
                b.ResetTimer()
                for i := 0; i < b.N; i++ {
                    var result int
                    db.QueryRow("SELECT 1 + 1").Scan(&result)
                }
            },
        },
        {
            name: "TableScan1000Rows",
            setup: func(db *sql.DB) {
                db.Exec("CREATE TABLE bench_scan (id INT, value DOUBLE)")
                for i := 0; i < 1000; i++ {
                    db.Exec("INSERT INTO bench_scan VALUES (?, ?)", i, float64(i)*1.1)
                }
            },
            bench: func(b *testing.B, db *sql.DB) {
                b.ResetTimer()
                for i := 0; i < b.N; i++ {
                    rows, _ := db.Query("SELECT * FROM bench_scan")
                    for rows.Next() {
                        var id int
                        var value float64
                        rows.Scan(&id, &value)
                    }
                    rows.Close()
                }
            },
        },
        {
            name: "Aggregation",
            setup: func(db *sql.DB) {
                db.Exec("CREATE TABLE bench_agg (category VARCHAR, amount DECIMAL)")
                for i := 0; i < 10000; i++ {
                    category := fmt.Sprintf("CAT_%d", i%10)
                    db.Exec("INSERT INTO bench_agg VALUES (?, ?)", category, float64(i))
                }
            },
            bench: func(b *testing.B, db *sql.DB) {
                b.ResetTimer()
                for i := 0; i < b.N; i++ {
                    rows, _ := db.Query(`
                        SELECT category, SUM(amount), AVG(amount), COUNT(*)
                        FROM bench_agg
                        GROUP BY category
                    `)
                    for rows.Next() {
                        var category string
                        var sum, avg float64
                        var count int
                        rows.Scan(&category, &sum, &avg, &count)
                    }
                    rows.Close()
                }
            },
        },
    }
    
    var results []BenchmarkResult
    
    for _, impl := range []string{"duckdb", "dukdb"} {
        b.Run(impl, func(b *testing.B) {
            db, err := sql.Open(impl, ":memory:")
            if err != nil {
                b.Skip("Implementation not available:", impl)
            }
            defer db.Close()
            
            for _, bench := range benchmarks {
                b.Run(bench.name, func(b *testing.B) {
                    if bench.setup != nil {
                        bench.setup(db)
                    }
                    
                    b.ReportAllocs()
                    start := time.Now()
                    bench.bench(b, db)
                    duration := time.Since(start)
                    
                    result := BenchmarkResult{
                        Implementation: impl,
                        TestName:       bench.name,
                        Operations:     b.N,
                        Duration:       duration,
                        OpsPerSecond:   float64(b.N) / duration.Seconds(),
                    }
                    results = append(results, result)
                })
            }
        })
    }
    
    generatePerformanceReport(b, results)
}

func generatePerformanceReport(b *testing.B, results []BenchmarkResult) {
    b.Log("\n=== Performance Comparison Report ===\n")
    
    // Group results by test name
    testResults := make(map[string][]BenchmarkResult)
    for _, r := range results {
        testResults[r.TestName] = append(testResults[r.TestName], r)
    }
    
    for testName, results := range testResults {
        b.Logf("\nTest: %s", testName)
        
        var cgoOps, pureOps float64
        for _, r := range results {
            b.Logf("  %s: %.2f ops/sec", r.Implementation, r.OpsPerSecond)
            if r.Implementation == "duckdb" {
                cgoOps = r.OpsPerSecond
            } else {
                pureOps = r.OpsPerSecond
            }
        }
        
        if cgoOps > 0 && pureOps > 0 {
            ratio := pureOps / cgoOps
            b.Logf("  Performance ratio (Pure/CGO): %.2fx", ratio)
            
            if ratio < 0.5 {
                b.Logf("  ⚠️  Pure-Go is significantly slower (>2x)")
            } else if ratio > 1.5 {
                b.Logf("  ✅ Pure-Go is faster!")
            } else {
                b.Logf("  ✓  Performance is comparable")
            }
        }
    }
}
```

## Feature Parity Matrix

```go
// test/compatibility/features.go
package compatibility

type Feature struct {
    Name        string
    Category    string
    CGOSupport  bool
    PureSupport bool
    Notes       string
}

var FeatureMatrix = []Feature{
    // Data Types
    {Name: "INTEGER", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "BIGINT", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "HUGEINT", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "DECIMAL", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "DOUBLE", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "VARCHAR", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "BLOB", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "DATE", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "TIME", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "TIMESTAMP", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "INTERVAL", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "LIST", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "STRUCT", Category: "types", CGOSupport: true, PureSupport: true},
    {Name: "MAP", Category: "types", CGOSupport: true, PureSupport: false, Notes: "Planned for v2"},
    {Name: "UNION", Category: "types", CGOSupport: true, PureSupport: false, Notes: "Complex implementation"},
    
    // SQL Features
    {Name: "CTEs", Category: "sql", CGOSupport: true, PureSupport: true},
    {Name: "Recursive CTEs", Category: "sql", CGOSupport: true, PureSupport: false, Notes: "Planned"},
    {Name: "Window Functions", Category: "sql", CGOSupport: true, PureSupport: true},
    {Name: "QUALIFY clause", Category: "sql", CGOSupport: true, PureSupport: false},
    {Name: "SAMPLE clause", Category: "sql", CGOSupport: true, PureSupport: false},
    {Name: "ASOF joins", Category: "sql", CGOSupport: true, PureSupport: false},
    
    // File Formats
    {Name: "CSV Reader", Category: "io", CGOSupport: true, PureSupport: true},
    {Name: "CSV Writer", Category: "io", CGOSupport: true, PureSupport: true},
    {Name: "Parquet Reader", Category: "io", CGOSupport: true, PureSupport: true},
    {Name: "Parquet Writer", Category: "io", CGOSupport: true, PureSupport: true},
    {Name: "JSON Reader", Category: "io", CGOSupport: true, PureSupport: true},
    {Name: "JSON Writer", Category: "io", CGOSupport: true, PureSupport: false},
    
    // Extensions
    {Name: "httpfs", Category: "extensions", CGOSupport: true, PureSupport: false},
    {Name: "postgres_scanner", Category: "extensions", CGOSupport: true, PureSupport: false},
    {Name: "sqlite_scanner", Category: "extensions", CGOSupport: true, PureSupport: false},
}

func GenerateFeatureReport() string {
    var report strings.Builder
    
    report.WriteString("# DuckDB Go Implementation Feature Parity Matrix\n\n")
    report.WriteString("| Category | Feature | CGO DuckDB | Pure-Go DuckDB | Notes |\n")
    report.WriteString("|----------|---------|------------|----------------|-------|\n")
    
    currentCategory := ""
    for _, f := range FeatureMatrix {
        if f.Category != currentCategory {
            currentCategory = f.Category
            report.WriteString(fmt.Sprintf("| **%s** | | | | |\n", strings.Title(f.Category)))
        }
        
        cgo := "❌"
        if f.CGOSupport {
            cgo = "✅"
        }
        
        pure := "❌"
        if f.PureSupport {
            pure = "✅"
        }
        
        report.WriteString(fmt.Sprintf("| | %s | %s | %s | %s |\n",
            f.Name, cgo, pure, f.Notes))
    }
    
    // Calculate statistics
    totalFeatures := len(FeatureMatrix)
    supportedFeatures := 0
    for _, f := range FeatureMatrix {
        if f.PureSupport {
            supportedFeatures++
        }
    }
    
    percentage := float64(supportedFeatures) / float64(totalFeatures) * 100
    report.WriteString(fmt.Sprintf("\n**Feature Coverage: %.1f%% (%d/%d features)**\n",
        percentage, supportedFeatures, totalFeatures))
    
    return report.String()
}
```

## Migration Testing

```go
// test/compatibility/migration_test.go
package compatibility

import (
    "database/sql"
    "io/ioutil"
    "os"
    "testing"
)

func TestDataMigration(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping migration tests")
    }
    
    tests := []struct {
        name string
        create string
        insert []string
        verify string
        expect int
    }{
        {
            name: "BasicTable",
            create: `CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(200),
                created_at TIMESTAMP
            )`,
            insert: []string{
                "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', '2024-01-01 10:00:00')",
                "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', '2024-01-02 11:00:00')",
            },
            verify: "SELECT COUNT(*) FROM users",
            expect: 2,
        },
        {
            name: "ComplexTypes",
            create: `CREATE TABLE complex_data (
                id INTEGER,
                numbers LIST<INTEGER>,
                metadata STRUCT(key VARCHAR, value VARCHAR),
                measurements DOUBLE[]
            )`,
            insert: []string{
                "INSERT INTO complex_data VALUES (1, [1,2,3], {'key': 'temp', 'value': '25C'}, [1.1, 2.2, 3.3])",
            },
            verify: "SELECT COUNT(*) FROM complex_data",
            expect: 1,
        },
    }
    
    for _, test := range tests {
        t.Run(test.name, func(t *testing.T) {
            // Create temporary directory
            tmpDir, err := ioutil.TempDir("", "duckdb-migration-*")
            require.NoError(t, err)
            defer os.RemoveAll(tmpDir)
            
            dbPath := filepath.Join(tmpDir, "test.db")
            
            // Create and populate with CGO DuckDB
            t.Log("Creating database with CGO DuckDB...")
            cgoDb, err := sql.Open("duckdb", dbPath)
            require.NoError(t, err)
            
            _, err = cgoDb.Exec(test.create)
            require.NoError(t, err)
            
            for _, insert := range test.insert {
                _, err = cgoDb.Exec(insert)
                require.NoError(t, err)
            }
            
            cgoDb.Close()
            
            // Open and verify with Pure-Go DuckDB
            t.Log("Opening database with Pure-Go DuckDB...")
            pureDb, err := sql.Open("dukdb", dbPath)
            require.NoError(t, err)
            defer pureDb.Close()
            
            var count int
            err = pureDb.QueryRow(test.verify).Scan(&count)
            require.NoError(t, err)
            assert.Equal(t, test.expect, count)
            
            // Try to read actual data
            rows, err := pureDb.Query("SELECT * FROM " + strings.Split(test.create, " ")[2])
            require.NoError(t, err)
            
            rowCount := 0
            for rows.Next() {
                rowCount++
                // Could add more detailed verification here
            }
            rows.Close()
            assert.Equal(t, test.expect, rowCount)
        })
    }
}

func TestExportImport(t *testing.T) {
    formats := []struct {
        name   string
        export string
        import string
    }{
        {
            name:   "CSV",
            export: "COPY users TO '%s' (FORMAT CSV, HEADER)",
            import: "CREATE TABLE users AS SELECT * FROM read_csv_auto('%s')",
        },
        {
            name:   "Parquet",
            export: "COPY users TO '%s' (FORMAT PARQUET)",
            import: "CREATE TABLE users AS SELECT * FROM parquet_scan('%s')",
        },
    }
    
    // Test data export from CGO and import to Pure-Go
    for _, format := range formats {
        t.Run(format.name, func(t *testing.T) {
            testExportImport(t, format.export, format.import)
        })
    }
}
```

## Continuous Validation

```yaml
# .github/workflows/compatibility.yml
name: Compatibility Validation

on:
  push:
    branches: [main]
  pull_request:
  schedule:
    - cron: '0 0 * * *'  # Daily validation

jobs:
  compatibility:
    runs-on: ubuntu-latest
    
    strategy:
      matrix:
        duckdb-version: ['0.9.2', '0.10.0', 'latest']
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup Go
      uses: actions/setup-go@v4
      with:
        go-version: '1.22'
    
    - name: Install DuckDB CGO
      run: |
        go install github.com/marcboeker/go-duckdb@latest
    
    - name: Run Compatibility Tests
      run: |
        go test -v ./test/compatibility/... -duckdb-version=${{ matrix.duckdb-version }}
    
    - name: Generate Reports
      run: |
        go run ./scripts/compatibility_report.go > compatibility_report.md
    
    - name: Upload Reports
      uses: actions/upload-artifact@v3
      with:
        name: compatibility-reports
        path: |
          compatibility_report.md
          feature_matrix.md
          performance_comparison.html
    
    - name: Comment on PR
      if: github.event_name == 'pull_request'
      uses: actions/github-script@v6
      with:
        script: |
          const fs = require('fs');
          const report = fs.readFileSync('compatibility_report.md', 'utf8');
          github.rest.issues.createComment({
            issue_number: context.issue.number,
            owner: context.repo.owner,
            repo: context.repo.repo,
            body: report
          });
```

## Compatibility Report Generator

```go
// scripts/compatibility_report.go
package main

import (
    "encoding/json"
    "fmt"
    "os"
    "text/template"
)

type CompatibilityReport struct {
    Timestamp        time.Time
    TotalTests       int
    PassedTests      int
    FailedTests      []FailedTest
    PerformanceRatio map[string]float64
    FeatureCoverage  float64
}

type FailedTest struct {
    Name     string
    Category string
    Reason   string
    CGOValue interface{}
    PureValue interface{}
}

const reportTemplate = `# DuckDB Pure-Go Implementation Compatibility Report

Generated: {{ .Timestamp.Format "2006-01-02 15:04:05" }}

## Summary

- Total Tests: {{ .TotalTests }}
- Passed: {{ .PassedTests }} ({{ printf "%.1f" .PassRate }}%)
- Failed: {{ len .FailedTests }}
- Feature Coverage: {{ printf "%.1f" .FeatureCoverage }}%

## Failed Tests

{{ range .FailedTests }}
### {{ .Name }} ({{ .Category }})
- **Reason**: {{ .Reason }}
- **CGO Result**: {{ .CGOValue }}
- **Pure-Go Result**: {{ .PureValue }}
{{ end }}

## Performance Comparison

| Benchmark | Performance Ratio |
|-----------|------------------|
{{ range $name, $ratio := .PerformanceRatio }}
| {{ $name }} | {{ printf "%.2fx" $ratio }} |
{{ end }}

## Recommendations

{{ if lt .PassRate 90.0 }}
⚠️ **Compatibility below 90%** - Review failed tests before production use.
{{ else }}
✅ **Good compatibility** - Implementation is ready for most use cases.
{{ end }}

{{ if .HasSlowBenchmarks }}
⚠️ **Performance concerns** - Some operations are significantly slower than CGO version.
{{ end }}
`

func generateReport(results TestResults) {
    report := CompatibilityReport{
        Timestamp:   time.Now(),
        TotalTests:  results.Total,
        PassedTests: results.Passed,
        FailedTests: results.Failed,
        // ... populate other fields
    }
    
    tmpl, err := template.New("report").Parse(reportTemplate)
    if err != nil {
        panic(err)
    }
    
    err = tmpl.Execute(os.Stdout, report)
    if err != nil {
        panic(err)
    }
}
```

This comprehensive compatibility validation framework ensures that the pure-Go DuckDB implementation maintains behavioral compatibility with the CGO version while providing clear visibility into any differences or limitations.