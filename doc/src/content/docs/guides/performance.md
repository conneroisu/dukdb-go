# Performance Optimization

This guide covers performance optimization techniques for the **dukdb-go** driver, including connection management, query optimization, and memory considerations.

## Connection Pool Configuration

Proper connection pool configuration is crucial for performance.

### Basic Pool Settings

```go
db, err := sql.Open("duckdb", ":memory:")
if err != nil {
    log.Fatal(err)
}

// Configure connection pool
db.SetMaxOpenConns(10)        // Maximum concurrent connections
db.SetMaxIdleConns(5)         // Keep 5 connections idle
db.SetConnMaxLifetime(time.Hour) // Recycle connections after 1 hour
db.SetConnMaxIdleTime(10 * time.Minute) // Close idle connections after 10 minutes
```

### Optimal Pool Sizing

```go
import "runtime"

func configureOptimalPool(db *sql.DB) {
    // Rule of thumb: 2x CPU cores for mixed workloads
    maxConns := runtime.NumCPU() * 2
    
    // For CPU-intensive analytics, use fewer connections
    if isAnalyticsWorkload() {
        maxConns = runtime.NumCPU()
    }
    
    // For high-concurrency OLTP, use more connections
    if isOLTPWorkload() {
        maxConns = runtime.NumCPU() * 4
    }
    
    db.SetMaxOpenConns(maxConns)
    db.SetMaxIdleConns(maxConns / 2)
    db.SetConnMaxLifetime(30 * time.Minute)
}
```

## Statement Caching

The driver automatically caches prepared statements for performance.

### Configuring Statement Cache

```go
import "github.com/connerohnesorge/dukdb-go/driver"

// Set global cache size (default: 100)
driver.SetStatementCacheSize(500)

// Monitor cache hit rate
stats := driver.GetCacheStats()
fmt.Printf("Cache hit rate: %.2f%%\n", 
    float64(stats.Hits) / float64(stats.Total) * 100)
```

### Effective Statement Reuse

```go
// Good: Reuse prepared statements
stmt, err := db.Prepare("SELECT * FROM users WHERE age > ? AND city = ?")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute multiple times with different parameters
ages := []int{25, 30, 35}
cities := []string{"Boston", "Cambridge", "Somerville"}

for i, age := range ages {
    rows, err := stmt.Query(age, cities[i])
    if err != nil {
        log.Fatal(err)
    }
    // Process rows...
    rows.Close()
}
```

```go
// Avoid: Creating new statements repeatedly
for i := 0; i < 1000; i++ {
    // This creates a new statement each time
    rows, err := db.Query("SELECT * FROM users WHERE id = ?", i)
    // Process...
}
```

## Query Optimization

### Use Appropriate Data Types

```sql
-- Good: Use appropriate integer sizes
CREATE TABLE metrics (
    id INTEGER,
    small_value TINYINT,    -- 1 byte
    medium_value SMALLINT,  -- 2 bytes
    large_value BIGINT      -- 8 bytes
);

-- Avoid: Using oversized types
CREATE TABLE metrics_bad (
    id BIGINT,              -- Unnecessary for most IDs
    small_value BIGINT,     -- Wastes memory
    medium_value BIGINT     -- Wastes memory
);
```

### Leverage DuckDB's Columnar Engine

```go
// Good: Columnar operations are highly optimized
rows, err := db.Query(`
    SELECT 
        category,
        SUM(amount) as total,
        AVG(amount) as average,
        COUNT(*) as count
    FROM transactions 
    WHERE date >= ? 
    GROUP BY category
    ORDER BY total DESC
`, startDate)
```

### Optimize WHERE Clauses

```sql
-- Good: Selective filters first
SELECT * FROM large_table 
WHERE indexed_column = 'value' 
  AND date >= '2024-01-01'
  AND expensive_function(column) > 0;

-- Better: Use covering indexes
CREATE INDEX idx_covering ON large_table (indexed_column, date, column);
```

### Use LIMIT for Large Results

```go
// Good: Paginate large result sets
func getUsers(db *sql.DB, offset, limit int) ([]User, error) {
    rows, err := db.Query(`
        SELECT id, name, email 
        FROM users 
        ORDER BY id 
        LIMIT ? OFFSET ?
    `, limit, offset)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var users []User
    for rows.Next() {
        var user User
        err := rows.Scan(&user.ID, &user.Name, &user.Email)
        if err != nil {
            return nil, err
        }
        users = append(users, user)
    }
    return users, nil
}
```

## Memory Management

### Connection Memory Optimization

```go
// Configure memory limits per connection
db, err := sql.Open("duckdb", ":memory:?memory_limit=4GB")
if err != nil {
    log.Fatal(err)
}

// For file-based databases
db, err = sql.Open("duckdb", "/path/to/db.duckdb?memory_limit=2GB&temp_directory=/tmp")
```

### Efficient Result Processing

```go
// Good: Process rows incrementally
func processLargeResultSet(db *sql.DB) error {
    rows, err := db.Query("SELECT * FROM large_table")
    if err != nil {
        return err
    }
    defer rows.Close()
    
    // Process one row at a time
    for rows.Next() {
        var record Record
        err := rows.Scan(&record.Field1, &record.Field2)
        if err != nil {
            return err
        }
        
        // Process record immediately
        if err := processRecord(record); err != nil {
            return err
        }
    }
    return nil
}

// Avoid: Loading all results into memory
func processLargeResultSetBad(db *sql.DB) error {
    rows, err := db.Query("SELECT * FROM large_table")
    if err != nil {
        return err
    }
    defer rows.Close()
    
    // Bad: Load everything into memory
    var records []Record
    for rows.Next() {
        var record Record
        rows.Scan(&record.Field1, &record.Field2)
        records = append(records, record)
    }
    
    // Process all at once - may cause OOM
    return processAllRecords(records)
}
```

### Memory Pool for Heavy Operations

```go
import "sync"

// Reuse buffers for repeated operations
type BufferPool struct {
    pool sync.Pool
}

func NewBufferPool() *BufferPool {
    return &BufferPool{
        pool: sync.Pool{
            New: func() interface{} {
                return make([]byte, 0, 1024*1024) // 1MB buffer
            },
        },
    }
}

func (p *BufferPool) Get() []byte {
    return p.pool.Get().([]byte)[:0]
}

func (p *BufferPool) Put(b []byte) {
    p.pool.Put(b)
}

// Usage
var bufferPool = NewBufferPool()

func processBlob(data []byte) {
    buffer := bufferPool.Get()
    defer bufferPool.Put(buffer)
    
    // Use buffer for processing
    buffer = append(buffer, data...)
    // Process buffer...
}
```

## Concurrent Operations

### Safe Concurrent Access

```go
// sql.DB is safe for concurrent use
var db *sql.DB

func init() {
    var err error
    db, err = sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    
    // Configure for concurrency
    db.SetMaxOpenConns(runtime.NumCPU() * 2)
}

// Concurrent queries are safe
func concurrentQueries() {
    var wg sync.WaitGroup
    
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            
            rows, err := db.Query("SELECT * FROM table WHERE id = ?", id)
            if err != nil {
                log.Printf("Error in goroutine %d: %v", id, err)
                return
            }
            defer rows.Close()
            
            // Process results...
        }(i)
    }
    
    wg.Wait()
}
```

### Batch Operations

```go
// Good: Use transactions for batch inserts
func batchInsert(db *sql.DB, records []Record) error {
    tx, err := db.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()
    
    stmt, err := tx.Prepare("INSERT INTO records (field1, field2) VALUES (?, ?)")
    if err != nil {
        return err
    }
    defer stmt.Close()
    
    for _, record := range records {
        _, err := stmt.Exec(record.Field1, record.Field2)
        if err != nil {
            return err
        }
    }
    
    return tx.Commit()
}

// Better: Use COPY for large datasets
func bulkInsert(db *sql.DB, csvPath string) error {
    _, err := db.Exec(`
        COPY records FROM '` + csvPath + `' 
        (DELIMITER ',', HEADER)
    `)
    return err
}
```

## Context and Cancellation

### Timeout Management

```go
import "context"

// Set query timeouts
func queryWithTimeout(db *sql.DB, query string, timeout time.Duration) (*sql.Rows, error) {
    ctx, cancel := context.WithTimeout(context.Background(), timeout)
    defer cancel()
    
    return db.QueryContext(ctx, query)
}

// Usage
rows, err := queryWithTimeout(db, "SELECT * FROM large_table", 30*time.Second)
if err != nil {
    if err == context.DeadlineExceeded {
        log.Println("Query timed out")
    }
    return err
}
defer rows.Close()
```

### Cancellable Operations

```go
// Cancellable long-running operation
func cancellableQuery(ctx context.Context, db *sql.DB) error {
    rows, err := db.QueryContext(ctx, "SELECT * FROM very_large_table")
    if err != nil {
        return err
    }
    defer rows.Close()
    
    for rows.Next() {
        // Check for cancellation
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
        }
        
        // Process row
        var data string
        if err := rows.Scan(&data); err != nil {
            return err
        }
        
        // Do work with data
    }
    
    return nil
}
```

## Monitoring and Profiling

### Connection Pool Monitoring

```go
import "github.com/connerohnesorge/dukdb-go/driver"

func monitorPool(db *sql.DB) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        stats := db.Stats()
        fmt.Printf("Pool Stats - Open: %d, Idle: %d, InUse: %d\n",
            stats.OpenConnections, stats.Idle, stats.InUse)
        
        // Cache statistics
        cacheStats := driver.GetCacheStats()
        fmt.Printf("Cache Stats - Hits: %d, Misses: %d, Hit Rate: %.2f%%\n",
            cacheStats.Hits, cacheStats.Misses,
            float64(cacheStats.Hits) / float64(cacheStats.Total) * 100)
    }
}
```

### Query Performance Analysis

```go
// Measure query execution time
func timedQuery(db *sql.DB, query string, args ...interface{}) error {
    start := time.Now()
    defer func() {
        duration := time.Since(start)
        if duration > 100*time.Millisecond {
            log.Printf("Slow query (%v): %s", duration, query)
        }
    }()
    
    rows, err := db.Query(query, args...)
    if err != nil {
        return err
    }
    defer rows.Close()
    
    // Process results...
    return nil
}
```

### Memory Profiling

```go
import (
    _ "net/http/pprof"
    "net/http"
    "runtime"
)

func enableProfiling() {
    go func() {
        log.Println(http.ListenAndServe("localhost:6060", nil))
    }()
}

func logMemoryUsage() {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    
    log.Printf("Alloc = %d KB", bToKb(m.Alloc))
    log.Printf("TotalAlloc = %d KB", bToKb(m.TotalAlloc))
    log.Printf("Sys = %d KB", bToKb(m.Sys))
    log.Printf("NumGC = %v", m.NumGC)
}

func bToKb(b uint64) uint64 {
    return b / 1024
}
```

## Benchmarking

### Performance Testing

```go
// benchmark_test.go
func BenchmarkInsert(b *testing.B) {
    db := setupTestDB()
    defer db.Close()
    
    b.ResetTimer()
    
    for i := 0; i < b.N; i++ {
        _, err := db.Exec("INSERT INTO test_table (value) VALUES (?)", i)
        if err != nil {
            b.Fatal(err)
        }
    }
}

func BenchmarkBatchInsert(b *testing.B) {
    db := setupTestDB()
    defer db.Close()
    
    b.ResetTimer()
    
    for i := 0; i < b.N; i++ {
        b.StopTimer()
        tx, _ := db.Begin()
        stmt, _ := tx.Prepare("INSERT INTO test_table (value) VALUES (?)")
        b.StartTimer()
        
        for j := 0; j < 1000; j++ {
            stmt.Exec(j)
        }
        
        stmt.Close()
        tx.Commit()
    }
}
```

### Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. ./test/benchmark/...

# Run with memory profiling
go test -bench=. -benchmem ./test/benchmark/...

# Run specific benchmark
go test -bench=BenchmarkInsert ./test/benchmark/...

# Generate CPU profile
go test -bench=. -cpuprofile=cpu.prof ./test/benchmark/...

# Generate memory profile
go test -bench=. -memprofile=mem.prof ./test/benchmark/...
```

## Performance Best Practices

### General Guidelines

1. **Use Connection Pooling**: Configure appropriate pool sizes
2. **Prepare Statements**: Reuse prepared statements for repeated queries
3. **Batch Operations**: Use transactions for multiple operations
4. **Stream Results**: Process large result sets incrementally
5. **Set Timeouts**: Use context for cancellation and timeouts
6. **Monitor Performance**: Track slow queries and resource usage

### DuckDB-Specific Optimizations

1. **Leverage Columnar Storage**: Design queries for column-oriented operations
2. **Use Appropriate Types**: Choose optimal data types for your use case
3. **Create Indexes**: Index frequently queried columns
4. **Partition Data**: Use table partitioning for very large datasets
5. **Memory Configuration**: Set appropriate memory limits

### Common Performance Pitfalls

1. **Not Reusing Connections**: Creating new connections frequently
2. **Ignoring Indexes**: Not creating indexes on query columns
3. **Large Result Sets**: Loading entire result sets into memory
4. **Unnecessary Transactions**: Using transactions for single operations
5. **Poor Query Design**: Not leveraging DuckDB's analytical capabilities

## Next Steps

- [Migration Guide](/guides/migration/) - Performance considerations when migrating
- [API Reference](/reference/api/) - Performance-related API documentation
- [Benchmarking Guide](/guides/benchmarking/) - Detailed benchmarking strategies