# Performance Example

This example demonstrates performance testing, optimization, and monitoring capabilities of the DuckDB pure-Go driver. It includes comprehensive benchmarks and performance analysis tools.

## Features Demonstrated

- **Connection Performance**: Connection creation, pooling, and reuse optimization
- **Query Performance**: Different query patterns and their performance characteristics
- **Insert Performance**: Individual, prepared, and batch insert strategies
- **Transaction Performance**: Transaction overhead and batch optimization
- **Concurrent Performance**: Multi-goroutine access patterns and scaling
- **Memory Usage Analysis**: Memory allocation patterns and garbage collection impact

## Running the Example

```bash
# Run the performance analysis
go run main.go

# Run performance tests
go test -v

# Run benchmarks
go test -bench=. -benchmem

# Run specific benchmark categories
go test -bench=BenchmarkConnection -benchmem
go test -bench=BenchmarkQuery -benchmem
go test -bench=BenchmarkInsert -benchmem
```

## Code Structure

- `main.go` - Comprehensive performance testing suite
- `main_test.go` - Unit tests and benchmarks for performance validation
- `README.md` - This documentation

## What You'll Learn

### 1. Connection Performance

#### Connection Creation

```go
// Measure connection creation overhead
start := time.Now()
db, err := sql.Open("duckdb", ":memory:")
duration := time.Since(start)
```

#### Connection Pooling

```go
// Configure connection pool for optimal performance
db.SetMaxOpenConns(10)
db.SetMaxIdleConns(5)
db.SetConnMaxLifetime(time.Hour)
db.SetConnMaxIdleTime(30 * time.Minute)
```

### 2. Query Performance Patterns

#### Simple Queries

```sql
-- Fast analytical queries
SELECT COUNT(*) FROM large_table;
SELECT AVG(amount) FROM transactions;
```

#### Range Queries

```sql
-- Efficient with proper indexing
SELECT * FROM events WHERE created_at BETWEEN ? AND ?;
```

#### Aggregation Queries

```sql
-- DuckDB's strength: fast aggregations
SELECT category, COUNT(*), SUM(amount), AVG(amount)
FROM sales 
GROUP BY category;
```

#### Complex Analytical Queries

```sql
-- Window functions and advanced analytics
SELECT 
    name,
    amount,
    RANK() OVER (PARTITION BY category ORDER BY amount DESC) as rank,
    SUM(amount) OVER (PARTITION BY category) as category_total
FROM sales;
```

### 3. Insert Performance Strategies

#### Individual Inserts (Slowest)

```go
for i := 0; i < 1000; i++ {
    db.Exec("INSERT INTO table VALUES (?, ?)", i, value)
}
```

#### Prepared Statements (Better)

```go
stmt, _ := db.Prepare("INSERT INTO table VALUES (?, ?)")
defer stmt.Close()

for i := 0; i < 1000; i++ {
    stmt.Exec(i, value)
}
```

#### Batch Inserts (Fastest)

```go
// Build single query with multiple values
query := "INSERT INTO table VALUES (?, ?), (?, ?), (?, ?)"
db.Exec(query, val1, val2, val3, val4, val5, val6)
```

### 4. Transaction Performance

#### Transaction Patterns

```go
// Individual transactions (high overhead)
for i := 0; i < 1000; i++ {
    tx, _ := db.Begin()
    tx.Exec("INSERT INTO table VALUES (?, ?)", i, value)
    tx.Commit()
}

// Batch transactions (optimal)
tx, _ := db.Begin()
for i := 0; i < 1000; i++ {
    tx.Exec("INSERT INTO table VALUES (?, ?)", i, value)
}
tx.Commit()
```

### 5. Concurrent Performance

#### Goroutine Scaling

```go
// Test different concurrency levels
concurrencyLevels := []int{1, 2, 4, 8, 16}

for _, numGoroutines := range concurrencyLevels {
    var wg sync.WaitGroup
    start := time.Now()
    
    for g := 0; g < numGoroutines; g++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            // Perform queries...
        }()
    }
    
    wg.Wait()
    duration := time.Since(start)
    qps := float64(totalQueries) / duration.Seconds()
}
```

### 6. Memory Usage Analysis

#### Memory Monitoring

```go
var m1, m2 runtime.MemStats
runtime.GC()
runtime.ReadMemStats(&m1)

// Perform operations...

runtime.GC()
runtime.ReadMemStats(&m2)

heapUsed := m2.Alloc - m1.Alloc
totalAlloc := m2.TotalAlloc - m1.TotalAlloc
```

## Performance Optimization Tips

### 1. Connection Management

- **Pool Size**: Set appropriate `MaxOpenConns` based on workload
- **Idle Connections**: Use `MaxIdleConns` to balance memory vs latency
- **Connection Lifetime**: Set `ConnMaxLifetime` to handle long-running apps

### 2. Query Optimization

- **Prepared Statements**: Reuse for repeated queries
- **Batch Operations**: Combine multiple operations when possible
- **Result Streaming**: Use `rows.Next()` for large result sets

### 3. Data Loading

- **Batch Inserts**: Much faster than individual inserts
- **Transactions**: Use for consistency and performance
- **COPY Operations**: Consider for bulk data loading

### 4. Memory Management

- **Close Resources**: Always close rows, statements, and connections
- **GC Pressure**: Monitor garbage collection impact
- **Buffer Sizes**: Tune for your data patterns

## Benchmarking Best Practices

### 1. Environment Setup

```go
func BenchmarkQuery(b *testing.B) {
    // Setup outside timing
    db := setupTestDB()
    defer db.Close()
    
    // Reset timer before actual benchmark
    b.ResetTimer()
    
    for i := 0; i < b.N; i++ {
        // Benchmark code
    }
}
```

### 2. Memory Benchmarks

```go
// Include memory allocation stats
go test -bench=. -benchmem
```

### 3. Statistical Significance

```go
// Run multiple times for stable results
go test -bench=. -count=5
```

## Performance Monitoring

### 1. Application Metrics

- Query execution time
- Connection pool utilization
- Error rates and timeouts
- Memory usage patterns

### 2. Database Metrics

- Query throughput (QPS)
- Transaction rates (TPS)
- Cache hit ratios
- Resource utilization

### 3. System Metrics

- CPU usage
- Memory consumption
- I/O patterns
- Network latency

## Real-World Use Cases

### 1. High-Throughput Analytics

Optimizing for maximum query throughput in analytical workloads.

### 2. Real-Time Data Ingestion

Balancing insert performance with query responsiveness.

### 3. Concurrent Applications

Scaling database access across multiple goroutines and connections.

### 4. Resource-Constrained Environments

Optimizing memory usage and connection overhead.

This performance example provides the foundation for understanding, measuring, and optimizing DuckDB driver performance in production applications.
