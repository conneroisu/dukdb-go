# DuckDB-Go Performance Benchmarking Framework

This framework provides comprehensive performance testing and analysis for the pure-Go DuckDB implementation, comparing it against the CGO version and tracking performance over time.

## Overview

The performance benchmarking framework consists of:

1. **Micro-benchmarks**: Testing individual components and operations
1. **Query Benchmarks**: Standard workloads (TPC-H, TPC-DS)
1. **Real-world Scenarios**: Common analytical patterns
1. **Memory Profiling**: Tracking allocations and GC pressure
1. **Regression Detection**: Automated performance regression alerts

## Benchmark Categories

### 1. Component Micro-benchmarks

#### Parser Performance

```go
// benchmark/micro/parser_bench_test.go
package micro

import (
    "testing"
    "github.com/yourusername/dukdb-go/internal/parser"
)

var parserQueries = []struct {
    name  string
    query string
}{
    {"simple_select", "SELECT 1"},
    {"table_select", "SELECT * FROM users WHERE id = 1"},
    {"complex_join", `
        SELECT u.name, COUNT(o.id), SUM(o.total)
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.created_at > '2024-01-01'
        GROUP BY u.id, u.name
        HAVING COUNT(o.id) > 5
        ORDER BY SUM(o.total) DESC
        LIMIT 10
    `},
    {"window_function", `
        SELECT 
            date,
            sales,
            SUM(sales) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as weekly_avg
        FROM daily_sales
    `},
    {"cte_recursive", `
        WITH RECURSIVE series(n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM series WHERE n < 1000
        )
        SELECT * FROM series
    `},
}

func BenchmarkParser(b *testing.B) {
    p := parser.New()
    
    for _, q := range parserQueries {
        b.Run(q.name, func(b *testing.B) {
            b.ReportAllocs()
            b.SetBytes(int64(len(q.query)))
            
            for i := 0; i < b.N; i++ {
                _, err := p.Parse(q.query)
                if err != nil {
                    b.Fatal(err)
                }
            }
        })
    }
}

// Benchmark parser with different query sizes
func BenchmarkParserScaling(b *testing.B) {
    sizes := []int{10, 50, 100, 500, 1000} // Number of columns/conditions
    
    for _, size := range sizes {
        b.Run(fmt.Sprintf("columns_%d", size), func(b *testing.B) {
            // Generate query with N columns
            query := generateSelectQuery(size)
            p := parser.New()
            
            b.ReportAllocs()
            b.SetBytes(int64(len(query)))
            
            for i := 0; i < b.N; i++ {
                _, err := p.Parse(query)
                if err != nil {
                    b.Fatal(err)
                }
            }
        })
    }
}
```

#### Type System Performance

```go
// benchmark/micro/types_bench_test.go
package micro

import (
    "math/big"
    "testing"
    "github.com/yourusername/dukdb-go/internal/types"
)

func BenchmarkHugeIntOperations(b *testing.B) {
    // Test data
    huge1 := types.NewHugeInt(new(big.Int).SetString("170141183460469231731687303715884105727", 10))
    huge2 := types.NewHugeInt(new(big.Int).SetString("123456789012345678901234567890", 10))
    
    b.Run("Addition", func(b *testing.B) {
        b.ReportAllocs()
        for i := 0; i < b.N; i++ {
            _ = huge1.Add(huge2)
        }
    })
    
    b.Run("Multiplication", func(b *testing.B) {
        b.ReportAllocs()
        for i := 0; i < b.N; i++ {
            _ = huge1.Multiply(huge2)
        }
    })
    
    b.Run("Comparison", func(b *testing.B) {
        b.ReportAllocs()
        for i := 0; i < b.N; i++ {
            _ = huge1.Compare(huge2)
        }
    })
}

func BenchmarkListOperations(b *testing.B) {
    // Create test lists of different sizes
    sizes := []int{10, 100, 1000, 10000}
    
    for _, size := range sizes {
        list := make([]types.Value, size)
        for i := range list {
            list[i] = types.NewInt64(int64(i))
        }
        listValue := types.NewList(list)
        
        b.Run(fmt.Sprintf("Sum_%d", size), func(b *testing.B) {
            b.ReportAllocs()
            for i := 0; i < b.N; i++ {
                _ = listValue.Sum()
            }
        })
        
        b.Run(fmt.Sprintf("Filter_%d", size), func(b *testing.B) {
            predicate := func(v types.Value) bool {
                return v.GetInt64()%2 == 0
            }
            
            b.ReportAllocs()
            for i := 0; i < b.N; i++ {
                _ = listValue.Filter(predicate)
            }
        })
    }
}

func BenchmarkTypeConversion(b *testing.B) {
    conversions := []struct {
        name string
        from types.Value
        to   types.Type
    }{
        {"Int32ToInt64", types.NewInt32(42), types.Int64Type},
        {"Int64ToDouble", types.NewInt64(42), types.DoubleType},
        {"StringToInt", types.NewString("12345"), types.Int64Type},
        {"DoubleToDecimal", types.NewDouble(123.456), types.DecimalType},
    }
    
    for _, conv := range conversions {
        b.Run(conv.name, func(b *testing.B) {
            b.ReportAllocs()
            for i := 0; i < b.N; i++ {
                _, err := conv.from.ConvertTo(conv.to)
                if err != nil {
                    b.Fatal(err)
                }
            }
        })
    }
}
```

#### Storage Engine Performance

```go
// benchmark/micro/storage_bench_test.go
package micro

import (
    "testing"
    "github.com/yourusername/dukdb-go/internal/storage"
)

func BenchmarkColumnarStorage(b *testing.B) {
    rowCounts := []int{1000, 10000, 100000, 1000000}
    
    for _, rows := range rowCounts {
        b.Run(fmt.Sprintf("Write_%d_rows", rows), func(b *testing.B) {
            b.ReportAllocs()
            b.SetBytes(int64(rows * 8 * 5)) // 5 columns, 8 bytes each
            
            for i := 0; i < b.N; i++ {
                store := storage.NewColumnarStore()
                
                // Write data
                for r := 0; r < rows; r++ {
                    row := []interface{}{
                        int64(r),
                        float64(r) * 1.1,
                        fmt.Sprintf("row_%d", r),
                        r%2 == 0,
                        time.Now(),
                    }
                    store.AppendRow(row)
                }
                
                store.Flush()
            }
        })
        
        b.Run(fmt.Sprintf("Scan_%d_rows", rows), func(b *testing.B) {
            // Setup
            store := storage.NewColumnarStore()
            for r := 0; r < rows; r++ {
                row := []interface{}{int64(r), float64(r) * 1.1, "data", true, time.Now()}
                store.AppendRow(row)
            }
            store.Flush()
            
            b.ResetTimer()
            b.ReportAllocs()
            b.SetBytes(int64(rows * 8 * 5))
            
            for i := 0; i < b.N; i++ {
                scanner := store.NewScanner()
                count := 0
                for scanner.Next() {
                    _ = scanner.Row()
                    count++
                }
                if count != rows {
                    b.Fatalf("Expected %d rows, got %d", rows, count)
                }
            }
        })
    }
}

func BenchmarkCompression(b *testing.B) {
    data := generateCompressibleData(1024 * 1024) // 1MB of data
    
    algorithms := []string{"snappy", "zstd", "lz4"}
    
    for _, algo := range algorithms {
        b.Run(algo+"_compress", func(b *testing.B) {
            compressor := storage.NewCompressor(algo)
            
            b.ReportAllocs()
            b.SetBytes(int64(len(data)))
            
            for i := 0; i < b.N; i++ {
                compressed, err := compressor.Compress(data)
                if err != nil {
                    b.Fatal(err)
                }
                _ = compressed
            }
        })
        
        b.Run(algo+"_decompress", func(b *testing.B) {
            compressor := storage.NewCompressor(algo)
            compressed, _ := compressor.Compress(data)
            
            b.ResetTimer()
            b.ReportAllocs()
            b.SetBytes(int64(len(compressed)))
            
            for i := 0; i < b.N; i++ {
                decompressed, err := compressor.Decompress(compressed)
                if err != nil {
                    b.Fatal(err)
                }
                _ = decompressed
            }
        })
    }
}
```

### 2. Query Execution Benchmarks

#### Execution Operators

```go
// benchmark/micro/operators_bench_test.go
package micro

import (
    "testing"
    "github.com/yourusername/dukdb-go/internal/execution"
)

func BenchmarkHashJoin(b *testing.B) {
    sizes := []struct {
        left  int
        right int
    }{
        {1000, 100},
        {10000, 1000},
        {100000, 10000},
        {1000000, 100000},
    }
    
    for _, size := range sizes {
        b.Run(fmt.Sprintf("%dx%d", size.left, size.right), func(b *testing.B) {
            // Create test data
            leftTable := generateTable("left", size.left, []string{"id", "value"})
            rightTable := generateTable("right", size.right, []string{"id", "data"})
            
            b.ResetTimer()
            b.ReportAllocs()
            
            for i := 0; i < b.N; i++ {
                join := execution.NewHashJoin(
                    leftTable,
                    rightTable,
                    "id", // Join column
                )
                
                result := join.Execute()
                consumeResults(result)
            }
        })
    }
}

func BenchmarkAggregation(b *testing.B) {
    rowCounts := []int{1000, 10000, 100000, 1000000}
    groupCounts := []int{10, 100, 1000}
    
    for _, rows := range rowCounts {
        for _, groups := range groupCounts {
            if groups > rows {
                continue
            }
            
            b.Run(fmt.Sprintf("rows_%d_groups_%d", rows, groups), func(b *testing.B) {
                table := generateGroupedData(rows, groups)
                
                b.ResetTimer()
                b.ReportAllocs()
                
                for i := 0; i < b.N; i++ {
                    agg := execution.NewAggregation(
                        table,
                        []string{"group_id"},              // Group by
                        []execution.AggregateFunc{         // Aggregates
                            {Func: "sum", Column: "value"},
                            {Func: "avg", Column: "value"},
                            {Func: "count", Column: "*"},
                        },
                    )
                    
                    result := agg.Execute()
                    consumeResults(result)
                }
            })
        }
    }
}

func BenchmarkSort(b *testing.B) {
    rowCounts := []int{1000, 10000, 100000}
    
    for _, rows := range rowCounts {
        b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
            table := generateRandomTable(rows, 5)
            
            b.ResetTimer()
            b.ReportAllocs()
            
            for i := 0; i < b.N; i++ {
                sort := execution.NewSort(
                    table,
                    []execution.SortKey{
                        {Column: "col1", Desc: false},
                        {Column: "col2", Desc: true},
                    },
                )
                
                result := sort.Execute()
                consumeResults(result)
            }
        })
    }
}
```

### 3. TPC-H Benchmarks

```go
// benchmark/queries/tpch_bench_test.go
package queries

import (
    "database/sql"
    "testing"
    "time"
)

type TPCHBenchmark struct {
    db    *sql.DB
    scale float64
}

func NewTPCHBenchmark(b *testing.B, impl string, scale float64) *TPCHBenchmark {
    db, err := sql.Open(impl, ":memory:")
    if err != nil {
        b.Fatal(err)
    }
    
    bench := &TPCHBenchmark{db: db, scale: scale}
    bench.loadData()
    
    return bench
}

func (t *TPCHBenchmark) loadData() {
    // Load TPC-H data at specified scale factor
    // This would load from pre-generated files or generate on the fly
}

var tpchQueries = map[string]string{
    "Q1": `-- Pricing Summary Report
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
            l_linestatus`,
    
    "Q6": `-- Forecasting Revenue Change
        SELECT
            sum(l_extendedprice * l_discount) as revenue
        FROM
            lineitem
        WHERE
            l_shipdate >= date '1994-01-01'
            AND l_shipdate < date '1994-01-01' + interval '1' year
            AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
            AND l_quantity < 24`,
    
    // Add all 22 TPC-H queries...
}

func BenchmarkTPCH(b *testing.B) {
    implementations := []string{"duckdb", "dukdb"}
    scales := []float64{0.01, 0.1, 1.0}
    
    for _, impl := range implementations {
        for _, scale := range scales {
            b.Run(fmt.Sprintf("%s/SF%.2f", impl, scale), func(b *testing.B) {
                bench := NewTPCHBenchmark(b, impl, scale)
                defer bench.db.Close()
                
                // Run each query
                for qname, query := range tpchQueries {
                    b.Run(qname, func(b *testing.B) {
                        // Warm up
                        bench.runQuery(query)
                        
                        b.ResetTimer()
                        
                        times := make([]time.Duration, b.N)
                        for i := 0; i < b.N; i++ {
                            start := time.Now()
                            bench.runQuery(query)
                            times[i] = time.Since(start)
                        }
                        
                        // Report statistics
                        b.ReportMetric(percentile(times, 50).Seconds()*1000, "p50_ms")
                        b.ReportMetric(percentile(times, 90).Seconds()*1000, "p90_ms")
                        b.ReportMetric(percentile(times, 99).Seconds()*1000, "p99_ms")
                    })
                }
            })
        }
    }
}

func (t *TPCHBenchmark) runQuery(query string) {
    rows, err := t.db.Query(query)
    if err != nil {
        panic(err)
    }
    defer rows.Close()
    
    // Consume all results
    for rows.Next() {
        // Process row
    }
}
```

### 4. Real-World Scenario Benchmarks

```go
// benchmark/queries/realworld_bench_test.go
package queries

import (
    "database/sql"
    "testing"
)

func BenchmarkRealWorldScenarios(b *testing.B) {
    scenarios := []struct {
        name  string
        setup func(*sql.DB)
        query string
    }{
        {
            name: "TimeSeries_1Hour_Aggregation",
            setup: func(db *sql.DB) {
                db.Exec(`CREATE TABLE metrics (
                    timestamp TIMESTAMP,
                    metric_name VARCHAR,
                    value DOUBLE,
                    tags MAP(VARCHAR, VARCHAR)
                )`)
                // Insert 1M time series data points
                insertTimeSeriesData(db, 1000000)
            },
            query: `
                SELECT 
                    time_bucket(INTERVAL '1 hour', timestamp) as hour,
                    metric_name,
                    avg(value) as avg_value,
                    min(value) as min_value,
                    max(value) as max_value,
                    count(*) as count
                FROM metrics
                WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
                GROUP BY hour, metric_name
                ORDER BY hour DESC, metric_name`,
        },
        {
            name: "LogAnalysis_ErrorPatterns",
            setup: func(db *sql.DB) {
                db.Exec(`CREATE TABLE logs (
                    timestamp TIMESTAMP,
                    level VARCHAR,
                    message VARCHAR,
                    metadata STRUCT(
                        user_id VARCHAR,
                        request_id VARCHAR,
                        duration_ms INTEGER
                    )
                )`)
                insertLogData(db, 1000000)
            },
            query: `
                WITH error_logs AS (
                    SELECT *
                    FROM logs
                    WHERE level = 'ERROR'
                    AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 day'
                )
                SELECT 
                    regexp_extract(message, 'Error: ([^:]+)', 1) as error_type,
                    count(*) as error_count,
                    count(DISTINCT metadata.user_id) as affected_users,
                    avg(metadata.duration_ms) as avg_duration_ms
                FROM error_logs
                GROUP BY error_type
                HAVING count(*) > 10
                ORDER BY error_count DESC
                LIMIT 20`,
        },
        {
            name: "UserAnalytics_Cohort",
            setup: func(db *sql.DB) {
                db.Exec(`CREATE TABLE user_events (
                    user_id VARCHAR,
                    event_time TIMESTAMP,
                    event_type VARCHAR,
                    properties MAP(VARCHAR, VARCHAR)
                )`)
                insertUserEventData(db, 1000000)
            },
            query: `
                WITH user_cohorts AS (
                    SELECT 
                        user_id,
                        DATE_TRUNC('week', MIN(event_time)) as cohort_week
                    FROM user_events
                    WHERE event_type = 'signup'
                    GROUP BY user_id
                ),
                weekly_active AS (
                    SELECT 
                        u.cohort_week,
                        DATE_TRUNC('week', e.event_time) as active_week,
                        COUNT(DISTINCT e.user_id) as active_users
                    FROM user_events e
                    JOIN user_cohorts u ON e.user_id = u.user_id
                    GROUP BY u.cohort_week, DATE_TRUNC('week', e.event_time)
                )
                SELECT 
                    cohort_week,
                    active_week,
                    active_users,
                    DATEDIFF('week', cohort_week, active_week) as weeks_since_signup
                FROM weekly_active
                ORDER BY cohort_week, active_week`,
        },
    }
    
    for _, impl := range []string{"duckdb", "dukdb"} {
        b.Run(impl, func(b *testing.B) {
            db, err := sql.Open(impl, ":memory:")
            if err != nil {
                b.Skip("Implementation not available:", impl)
            }
            defer db.Close()
            
            for _, scenario := range scenarios {
                b.Run(scenario.name, func(b *testing.B) {
                    // Setup
                    scenario.setup(db)
                    
                    // Warm up
                    runQuery(db, scenario.query)
                    
                    b.ResetTimer()
                    b.ReportAllocs()
                    
                    for i := 0; i < b.N; i++ {
                        runQuery(db, scenario.query)
                    }
                })
            }
        })
    }
}
```

### 5. Memory and Allocation Profiling

```go
// benchmark/memory/memory_bench_test.go
package memory

import (
    "runtime"
    "testing"
)

func BenchmarkMemoryUsage(b *testing.B) {
    scenarios := []struct {
        name string
        fn   func()
    }{
        {
            name: "ParseLargeQuery",
            fn: func() {
                parser := parser.New()
                query := generateComplexQuery(1000) // 1000 predicates
                parser.Parse(query)
            },
        },
        {
            name: "LargeResultSet",
            fn: func() {
                db, _ := sql.Open("dukdb", ":memory:")
                defer db.Close()
                
                // Create and query large dataset
                db.Exec("CREATE TABLE large AS SELECT * FROM range(1000000)")
                rows, _ := db.Query("SELECT * FROM large")
                
                for rows.Next() {
                    var i int
                    rows.Scan(&i)
                }
                rows.Close()
            },
        },
        {
            name: "ComplexAggregation",
            fn: func() {
                db, _ := sql.Open("dukdb", ":memory:")
                defer db.Close()
                
                setupComplexData(db)
                db.Query(`
                    SELECT 
                        category,
                        subcategory,
                        SUM(amount) as total,
                        AVG(amount) as average,
                        COUNT(DISTINCT user_id) as unique_users,
                        LIST(amount ORDER BY amount DESC LIMIT 10) as top_amounts
                    FROM transactions
                    GROUP BY category, subcategory
                `)
            },
        },
    }
    
    for _, scenario := range scenarios {
        b.Run(scenario.name, func(b *testing.B) {
            b.ReportAllocs()
            
            // Measure starting memory
            var startMem runtime.MemStats
            runtime.ReadMemStats(&startMem)
            
            for i := 0; i < b.N; i++ {
                scenario.fn()
            }
            
            // Force GC and measure final memory
            runtime.GC()
            var endMem runtime.MemStats
            runtime.ReadMemStats(&endMem)
            
            // Report memory metrics
            b.ReportMetric(float64(endMem.Alloc-startMem.Alloc)/float64(b.N), "bytes/op")
            b.ReportMetric(float64(endMem.TotalAlloc-startMem.TotalAlloc)/float64(b.N), "allocs/op")
            b.ReportMetric(float64(endMem.NumGC-startMem.NumGC), "gc_cycles")
        })
    }
}

func TestMemoryLeaks(t *testing.T) {
    // Run operations in a loop and check for memory growth
    iterations := 1000
    samples := 10
    
    memSamples := make([]uint64, samples)
    
    for s := 0; s < samples; s++ {
        runtime.GC()
        var m runtime.MemStats
        runtime.ReadMemStats(&m)
        startMem := m.Alloc
        
        // Run operations
        for i := 0; i < iterations; i++ {
            db, _ := sql.Open("dukdb", ":memory:")
            db.Exec("CREATE TABLE test (id INT)")
            db.Exec("INSERT INTO test VALUES (1), (2), (3)")
            db.Query("SELECT * FROM test")
            db.Close()
        }
        
        runtime.GC()
        runtime.ReadMemStats(&m)
        memSamples[s] = m.Alloc - startMem
    }
    
    // Check for memory growth trend
    if isIncreasing(memSamples) {
        t.Errorf("Potential memory leak detected: %v", memSamples)
    }
}
```

## Performance Analysis Tools

### Benchmark Comparison Tool

```go
// scripts/benchmark_compare.go
package main

import (
    "encoding/json"
    "fmt"
    "os"
    "os/exec"
)

type BenchmarkResult struct {
    Name              string
    Implementation    string
    NsPerOp           float64
    AllocsPerOp       float64
    BytesPerOp        float64
    CustomMetrics     map[string]float64
}

func main() {
    // Run benchmarks for both implementations
    cgoResults := runBenchmarks("duckdb")
    pureResults := runBenchmarks("dukdb")
    
    // Generate comparison report
    report := generateComparisonReport(cgoResults, pureResults)
    
    // Output results
    fmt.Println(report)
    
    // Save to file
    saveReport(report, "benchmark_comparison.html")
}

func runBenchmarks(impl string) []BenchmarkResult {
    cmd := exec.Command("go", "test", "-bench=.", "-benchmem", 
        "-json", "./benchmark/...", 
        fmt.Sprintf("-impl=%s", impl))
    
    output, err := cmd.Output()
    if err != nil {
        panic(err)
    }
    
    return parseBenchmarkOutput(output)
}

func generateComparisonReport(cgo, pure []BenchmarkResult) string {
    // Group results by benchmark name
    comparisons := make(map[string]struct {
        CGO  BenchmarkResult
        Pure BenchmarkResult
    })
    
    for _, r := range cgo {
        comp := comparisons[r.Name]
        comp.CGO = r
        comparisons[r.Name] = comp
    }
    
    for _, r := range pure {
        comp := comparisons[r.Name]
        comp.Pure = r
        comparisons[r.Name] = comp
    }
    
    // Generate HTML report
    html := `
<!DOCTYPE html>
<html>
<head>
    <title>DuckDB Go Implementation Benchmark Comparison</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .faster { color: green; font-weight: bold; }
        .slower { color: red; }
        .comparable { color: orange; }
    </style>
</head>
<body>
    <h1>DuckDB Go Implementation Benchmark Comparison</h1>
    <table>
        <tr>
            <th>Benchmark</th>
            <th>CGO (ns/op)</th>
            <th>Pure Go (ns/op)</th>
            <th>Ratio</th>
            <th>CGO (B/op)</th>
            <th>Pure Go (B/op)</th>
            <th>Memory Ratio</th>
        </tr>
`
    
    for name, comp := range comparisons {
        ratio := comp.Pure.NsPerOp / comp.CGO.NsPerOp
        memRatio := comp.Pure.BytesPerOp / comp.CGO.BytesPerOp
        
        ratioClass := "comparable"
        if ratio < 0.8 {
            ratioClass = "faster"
        } else if ratio > 2.0 {
            ratioClass = "slower"
        }
        
        html += fmt.Sprintf(`
        <tr>
            <td>%s</td>
            <td>%.0f</td>
            <td>%.0f</td>
            <td class="%s">%.2fx</td>
            <td>%.0f</td>
            <td>%.0f</td>
            <td>%.2fx</td>
        </tr>
`, name, comp.CGO.NsPerOp, comp.Pure.NsPerOp, ratioClass, ratio,
   comp.CGO.BytesPerOp, comp.Pure.BytesPerOp, memRatio)
    }
    
    html += `
    </table>
    <h2>Summary</h2>
    <ul>
        <li>Green: Pure Go is faster than CGO</li>
        <li>Orange: Performance within 2x</li>
        <li>Red: Pure Go is more than 2x slower</li>
    </ul>
</body>
</html>
`
    
    return html
}
```

### Performance Regression Detection

```go
// scripts/perf_regression.go
package main

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "os"
)

type PerformanceBaseline struct {
    Commit     string
    Timestamp  time.Time
    Benchmarks map[string]BenchmarkResult
}

func detectRegressions(current, baseline PerformanceBaseline) []Regression {
    var regressions []Regression
    
    for name, currentBench := range current.Benchmarks {
        if baselineBench, exists := baseline.Benchmarks[name]; exists {
            degradation := (currentBench.NsPerOp - baselineBench.NsPerOp) / baselineBench.NsPerOp
            
            if degradation > 0.1 { // 10% threshold
                regressions = append(regressions, Regression{
                    Benchmark:    name,
                    BaselineTime: baselineBench.NsPerOp,
                    CurrentTime:  currentBench.NsPerOp,
                    Degradation:  degradation * 100,
                })
            }
        }
    }
    
    return regressions
}

type Regression struct {
    Benchmark    string
    BaselineTime float64
    CurrentTime  float64
    Degradation  float64 // Percentage
}

func main() {
    // Load baseline
    baselineData, _ := ioutil.ReadFile("benchmark_baseline.json")
    var baseline PerformanceBaseline
    json.Unmarshal(baselineData, &baseline)
    
    // Run current benchmarks
    current := runCurrentBenchmarks()
    
    // Detect regressions
    regressions := detectRegressions(current, baseline)
    
    if len(regressions) > 0 {
        fmt.Println("⚠️  Performance Regressions Detected:")
        for _, r := range regressions {
            fmt.Printf("  - %s: %.1f%% slower (%.0f ns -> %.0f ns)\n",
                r.Benchmark, r.Degradation, r.BaselineTime, r.CurrentTime)
        }
        os.Exit(1)
    } else {
        fmt.Println("✅ No performance regressions detected")
    }
}
```

## Continuous Performance Monitoring

```yaml
# .github/workflows/performance.yml
name: Performance Benchmarks

on:
  push:
    branches: [main]
  pull_request:
  schedule:
    - cron: '0 0 * * *'  # Daily performance check

jobs:
  benchmark:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup Go
      uses: actions/setup-go@v4
      with:
        go-version: '1.22'
    
    - name: Install dependencies
      run: |
        go mod download
        go install github.com/marcboeker/go-duckdb@latest
    
    - name: Run benchmarks
      run: |
        # Run micro-benchmarks
        go test -bench=. -benchmem -benchtime=10s \
          -json ./benchmark/micro/... > micro_bench.json
        
        # Run query benchmarks
        go test -bench=. -benchmem -timeout=30m \
          -json ./benchmark/queries/... > query_bench.json
    
    - name: Compare with baseline
      run: |
        go run scripts/perf_regression.go
    
    - name: Generate reports
      run: |
        go run scripts/benchmark_compare.go > comparison.html
        go run scripts/generate_perf_report.go > performance_report.md
    
    - name: Upload results
      uses: actions/upload-artifact@v3
      with:
        name: benchmark-results
        path: |
          *.json
          *.html
          *.md
    
    - name: Comment on PR
      if: github.event_name == 'pull_request'
      uses: actions/github-script@v6
      with:
        script: |
          const fs = require('fs');
          const report = fs.readFileSync('performance_report.md', 'utf8');
          github.rest.issues.createComment({
            issue_number: context.issue.number,
            owner: context.repo.owner,
            repo: context.repo.repo,
            body: report
          });

  profile:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Run CPU profiling
      run: |
        go test -cpuprofile=cpu.prof -bench=BenchmarkTPCH ./benchmark/queries/...
        go tool pprof -svg cpu.prof > cpu_profile.svg
    
    - name: Run memory profiling
      run: |
        go test -memprofile=mem.prof -bench=BenchmarkMemoryUsage ./benchmark/memory/...
        go tool pprof -svg mem.prof > mem_profile.svg
    
    - name: Upload profiles
      uses: actions/upload-artifact@v3
      with:
        name: profiles
        path: |
          *.prof
          *.svg
```

## Performance Dashboard

```html
<!-- dashboard/index.html -->
<!DOCTYPE html>
<html>
<head>
    <title>DuckDB-Go Performance Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .chart-container { width: 600px; height: 400px; display: inline-block; margin: 20px; }
        .metric { background: #f5f5f5; padding: 10px; margin: 10px; display: inline-block; }
        .good { color: green; }
        .bad { color: red; }
    </style>
</head>
<body>
    <h1>DuckDB-Go Performance Dashboard</h1>
    
    <div id="metrics">
        <div class="metric">
            <h3>Parser Performance</h3>
            <p>Simple Query: <span id="parser-simple">-</span> ns/op</p>
            <p>Complex Query: <span id="parser-complex">-</span> ns/op</p>
        </div>
        
        <div class="metric">
            <h3>Execution Performance</h3>
            <p>Table Scan (1M rows): <span id="scan-1m">-</span> ms</p>
            <p>Hash Join (100k x 10k): <span id="join-100k">-</span> ms</p>
        </div>
        
        <div class="metric">
            <h3>TPC-H Performance (SF 1.0)</h3>
            <p>Q1: <span id="tpch-q1">-</span> ms</p>
            <p>Q6: <span id="tpch-q6">-</span> ms</p>
        </div>
    </div>
    
    <div class="chart-container">
        <canvas id="performanceTrend"></canvas>
    </div>
    
    <div class="chart-container">
        <canvas id="memoryUsage"></canvas>
    </div>
    
    <script>
        // Load and display performance data
        fetch('performance_data.json')
            .then(response => response.json())
            .then(data => {
                updateMetrics(data.latest);
                drawPerformanceTrend(data.history);
                drawMemoryUsage(data.memory);
            });
        
        function updateMetrics(latest) {
            document.getElementById('parser-simple').textContent = latest.parser.simple;
            document.getElementById('parser-complex').textContent = latest.parser.complex;
            // Update other metrics...
        }
        
        function drawPerformanceTrend(history) {
            const ctx = document.getElementById('performanceTrend').getContext('2d');
            new Chart(ctx, {
                type: 'line',
                data: {
                    labels: history.map(h => h.date),
                    datasets: [{
                        label: 'Parser Performance',
                        data: history.map(h => h.parser),
                        borderColor: 'blue'
                    }, {
                        label: 'Execution Performance',
                        data: history.map(h => h.execution),
                        borderColor: 'green'
                    }]
                },
                options: {
                    title: {
                        display: true,
                        text: 'Performance Trend'
                    }
                }
            });
        }
    </script>
</body>
</html>
```

This comprehensive performance benchmarking framework provides:

1. **Micro-benchmarks** for individual components
1. **Query benchmarks** using standard workloads
1. **Real-world scenarios** testing
1. **Memory profiling** and leak detection
1. **Automated regression detection**
1. **Comparison tools** between implementations
1. **Continuous monitoring** with CI integration
1. **Performance dashboard** for visualization

The framework ensures that the pure-Go DuckDB implementation maintains acceptable performance characteristics compared to the CGO version.
