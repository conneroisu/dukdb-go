# Troubleshooting Guide

Common issues and solutions when using the **dukdb-go** pure-Go DuckDB driver.

## Installation Issues

### Library Not Found

**Error:**
```
panic: Failed to load DuckDB library: libduckdb.so: cannot open shared object file: No such file or directory
```

**Solutions:**

1. **Set Library Path:**
   ```bash
   export DUCKDB_LIB_DIR="/usr/local/lib"
   export LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
   ```

2. **Install DuckDB Library:**
   ```bash
   # macOS
   brew install duckdb
   
   # Ubuntu/Debian
   curl -L https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-linux-amd64.zip -o libduckdb.zip
   unzip libduckdb.zip
   sudo mv libduckdb.so /usr/local/lib/
   sudo ldconfig
   ```

3. **Use Nix (Recommended):**
   ```bash
   nix develop  # Automatically provides DuckDB library
   ```

4. **Check Library Availability:**
   ```bash
   go run scripts/test-duckdb-availability.sh
   ```

### Go Version Too Old

**Error:**
```
build constraints exclude all Go files in purego package
```

**Solution:**
Upgrade to Go 1.21 or later:
```bash
go version  # Check current version
# Upgrade to Go 1.21+
```

### Module Download Issues

**Error:**
```
go: github.com/connerohnesorge/dukdb-go@v1.0.0: unrecognized import path
```

**Solutions:**
```bash
# Clear module cache
go clean -modcache

# Re-download dependencies
go mod download

# Verify go.mod
go mod tidy
```

## Runtime Issues

### Connection Failures

**Error:**
```
Failed to open database: unable to open database file
```

**Solutions:**

1. **Check File Permissions:**
   ```bash
   # Ensure directory is writable
   ls -la /path/to/database/
   chmod 755 /path/to/database/
   ```

2. **Verify Database Path:**
   ```go
   // Use absolute paths
   db, err := sql.Open("duckdb", "/absolute/path/to/database.db")
   
   // Or check current directory
   pwd, _ := os.Getwd()
   fmt.Printf("Current directory: %s\n", pwd)
   ```

3. **Test with In-Memory Database:**
   ```go
   // This should always work
   db, err := sql.Open("duckdb", ":memory:")
   if err != nil {
       // Issue is not with file access
   }
   ```

### Query Execution Errors

**Error:**
```
DuckDB error 2: SQL syntax error near "..."
```

**Solutions:**

1. **Validate SQL Syntax:**
   ```go
   // Test query in DuckDB CLI first
   // duckdb
   // D SELECT * FROM table;
   ```

2. **Check Parameter Binding:**
   ```go
   // Ensure parameter count matches placeholders
   rows, err := db.Query("SELECT * FROM table WHERE col1 = ? AND col2 = ?", 
       val1, val2) // Must provide exactly 2 values
   ```

3. **Escape Special Characters:**
   ```go
   // Use double quotes for identifiers with special characters
   _, err := db.Exec(`CREATE TABLE "my-table" (id INTEGER)`)
   ```

### Memory Issues

**Error:**
```
runtime: out of memory
```

**Solutions:**

1. **Set Memory Limits:**
   ```go
   db, err := sql.Open("duckdb", ":memory:?memory_limit=2GB")
   ```

2. **Process Results Incrementally:**
   ```go
   // Good: Process row by row
   rows, err := db.Query("SELECT * FROM large_table")
   defer rows.Close()
   
   for rows.Next() {
       var data SomeStruct
       rows.Scan(&data.Field1, &data.Field2)
       // Process immediately
       processRecord(data)
   }
   
   // Avoid: Loading all into memory
   var allData []SomeStruct // This can cause OOM
   for rows.Next() {
       var data SomeStruct
       rows.Scan(&data.Field1, &data.Field2)
       allData = append(allData, data) // Memory grows unbounded
   }
   ```

3. **Use Pagination:**
   ```go
   limit := 1000
   for offset := 0; ; offset += limit {
       rows, err := db.Query(`
           SELECT * FROM large_table 
           ORDER BY id 
           LIMIT ? OFFSET ?
       `, limit, offset)
       
       if err != nil {
           return err
       }
       
       hasRows := false
       for rows.Next() {
           hasRows = true
           // Process row
       }
       rows.Close()
       
       if !hasRows {
           break // No more data
       }
   }
   ```

## Performance Issues

### Slow Query Performance

**Symptoms:**
- Queries taking longer than expected
- High CPU usage
- Memory consumption growing

**Solutions:**

1. **Create Indexes:**
   ```sql
   -- Analyze query plan first
   EXPLAIN SELECT * FROM table WHERE column = 'value';
   
   -- Create appropriate indexes
   CREATE INDEX idx_column ON table (column);
   ```

2. **Optimize Query Structure:**
   ```sql
   -- Good: Filter early
   SELECT t1.*, t2.name 
   FROM table1 t1 
   JOIN table2 t2 ON t1.id = t2.id 
   WHERE t1.date >= '2024-01-01'  -- Filter first
   
   -- Avoid: Late filtering
   SELECT t1.*, t2.name 
   FROM table1 t1 
   JOIN table2 t2 ON t1.id = t2.id 
   WHERE EXTRACT(year FROM t1.date) = 2024  -- Expensive function
   ```

3. **Use Appropriate Data Types:**
   ```sql
   -- Good: Use appropriate sizes
   CREATE TABLE metrics (
       id INTEGER,           -- Not BIGINT if not needed
       value REAL,          -- Not DOUBLE if precision allows
       timestamp TIMESTAMP   -- Not VARCHAR for dates
   );
   ```

### Connection Pool Exhaustion

**Error:**
```
sql: connection request timed out
```

**Solutions:**

1. **Configure Pool Properly:**
   ```go
   db.SetMaxOpenConns(20)
   db.SetMaxIdleConns(10)
   db.SetConnMaxLifetime(time.Hour)
   ```

2. **Monitor Pool Usage:**
   ```go
   stats := db.Stats()
   fmt.Printf("Pool stats - Open: %d, InUse: %d, Idle: %d\n",
       stats.OpenConnections, stats.InUse, stats.Idle)
   ```

3. **Fix Connection Leaks:**
   ```go
   // Always close resources
   rows, err := db.Query("SELECT * FROM table")
   if err != nil {
       return err
   }
   defer rows.Close() // Critical!
   
   for rows.Next() {
       // Process rows
   }
   // rows.Close() called automatically by defer
   ```

### High Memory Usage

**Solutions:**

1. **Monitor Statement Cache:**
   ```go
   import "github.com/connerohnesorge/dukdb-go/driver"
   
   stats := driver.GetCacheStats()
   if stats.Size > 1000 {
       // Reduce cache size
       driver.SetStatementCacheSize(100)
   }
   ```

2. **Profile Memory Usage:**
   ```go
   import (
       _ "net/http/pprof"
       "net/http"
       "runtime"
   )
   
   func main() {
       // Enable profiling
       go http.ListenAndServe("localhost:6060", nil)
       
       // Monitor memory
       var m runtime.MemStats
       runtime.ReadMemStats(&m)
       fmt.Printf("Alloc = %d KB", m.Alloc / 1024)
   }
   ```

## Type Conversion Issues

### Complex Type Parsing Errors

**Error:**
```
json: cannot unmarshal string into Go value of type []interface{}
```

**Solutions:**

1. **Handle JSON Properly:**
   ```go
   var listJSON string
   err := rows.Scan(&listJSON)
   if err != nil {
       return err
   }
   
   // Parse JSON string
   var items []interface{}
   err = json.Unmarshal([]byte(listJSON), &items)
   if err != nil {
       // Handle invalid JSON
       fmt.Printf("Invalid JSON: %s\n", listJSON)
       return err
   }
   ```

2. **Use Type Helpers:**
   ```go
   import "github.com/connerohnesorge/dukdb-go/internal/types"
   
   // Use helper functions for complex types
   list, err := types.ParseList(listJSON)
   if err != nil {
       return fmt.Errorf("failed to parse list: %w", err)
   }
   ```

3. **Validate Type Expectations:**
   ```go
   // Check if the column actually contains the expected type
   rows, err := db.Query("SELECT typeof(column_name) FROM table LIMIT 1")
   if err != nil {
       return err
   }
   defer rows.Close()
   
   var columnType string
   if rows.Next() {
       rows.Scan(&columnType)
       fmt.Printf("Column type: %s\n", columnType)
   }
   ```

### Date/Time Parsing Issues

**Error:**
```
parsing time "..." as "2006-01-02 15:04:05": cannot parse
```

**Solutions:**

1. **Use sql.NullTime for Nullable Dates:**
   ```go
   var createdAt sql.NullTime
   err := rows.Scan(&createdAt)
   if err != nil {
       return err
   }
   
   if createdAt.Valid {
       fmt.Printf("Created: %s\n", createdAt.Time.Format(time.RFC3339))
   }
   ```

2. **Handle Different Date Formats:**
   ```go
   var dateStr string
   err := rows.Scan(&dateStr)
   if err != nil {
       return err
   }
   
   // Try multiple formats
   formats := []string{
       "2006-01-02 15:04:05",
       "2006-01-02T15:04:05Z",
       "2006-01-02",
   }
   
   var parsedTime time.Time
   for _, format := range formats {
       if t, err := time.Parse(format, dateStr); err == nil {
           parsedTime = t
           break
       }
   }
   ```

## Build and Deployment Issues

### Cross-Compilation Problems

**Error:**
```
runtime: purego not supported on GOOS/GOARCH
```

**Solutions:**

1. **Check Platform Support:**
   ```bash
   # purego supports specific platforms
   # amd64: windows, linux, darwin, freebsd
   # arm64: linux, darwin
   ```

2. **Build for Supported Platforms:**
   ```bash
   # Supported combinations
   GOOS=linux GOARCH=amd64 go build
   GOOS=darwin GOARCH=amd64 go build
   GOOS=darwin GOARCH=arm64 go build
   GOOS=windows GOARCH=amd64 go build
   ```

### Static Binary Issues

**Error:**
```
error loading shared library
```

**Solutions:**

1. **Ensure Library is Available at Runtime:**
   ```dockerfile
   # In Docker, install DuckDB library
   FROM alpine:latest
   RUN apk add --no-cache curl unzip
   RUN curl -L https://github.com/duckdb/duckdb/releases/download/v0.10.0/libduckdb-linux-amd64.zip -o libduckdb.zip && \
       unzip libduckdb.zip && \
       mv libduckdb.so /usr/local/lib/
   
   COPY myapp .
   CMD ["./myapp"]
   ```

2. **Bundle Library Path:**
   ```bash
   # Set runtime library path
   export LD_LIBRARY_PATH="/usr/local/lib:$LD_LIBRARY_PATH"
   ./myapp
   ```

## Testing Issues

### Test Database Conflicts

**Error:**
```
database is locked
```

**Solutions:**

1. **Use Unique Test Databases:**
   ```go
   func TestSomething(t *testing.T) {
       // Use unique database per test
       dbPath := fmt.Sprintf("/tmp/test_%d.db", time.Now().UnixNano())
       db, err := sql.Open("duckdb", dbPath)
       if err != nil {
           t.Fatal(err)
       }
       defer db.Close()
       defer os.Remove(dbPath)
       
       // Run test...
   }
   ```

2. **Use In-Memory Databases:**
   ```go
   func TestSomething(t *testing.T) {
       // In-memory databases are always isolated
       db, err := sql.Open("duckdb", ":memory:")
       if err != nil {
           t.Fatal(err)
       }
       defer db.Close()
       
       // Run test...
   }
   ```

### Parallel Test Issues

**Error:**
```
tests failing randomly when run in parallel
```

**Solutions:**

1. **Disable Parallel for Database Tests:**
   ```go
   func TestDatabaseOperation(t *testing.T) {
       // Disable parallel execution for this test
       // if it modifies shared state
       
       db, err := sql.Open("duckdb", ":memory:")
       // ... test code
   }
   ```

2. **Use Test-Specific Setup:**
   ```go
   func TestWithIsolation(t *testing.T) {
       t.Parallel() // This is safe with proper isolation
       
       db, err := sql.Open("duckdb", ":memory:")
       if err != nil {
           t.Fatal(err)
       }
       defer db.Close()
       
       // Each test has its own database
   }
   ```

## Debugging Techniques

### Enable Debug Logging

```go
import "log"

// Enable SQL logging (if available)
db, err := sql.Open("duckdb", ":memory:?log_level=debug")

// Add manual logging
log.Printf("Executing query: %s with args: %v", query, args)
rows, err := db.Query(query, args...)
if err != nil {
    log.Printf("Query failed: %v", err)
    return err
}
```

### Connection State Debugging

```go
func debugConnectionState(db *sql.DB) {
    stats := db.Stats()
    log.Printf("Connection pool state:")
    log.Printf("  Max open connections: %d", stats.MaxOpenConnections)
    log.Printf("  Open connections: %d", stats.OpenConnections)
    log.Printf("  In use: %d", stats.InUse)
    log.Printf("  Idle: %d", stats.Idle)
    log.Printf("  Wait count: %d", stats.WaitCount)
    log.Printf("  Wait duration: %s", stats.WaitDuration)
}
```

### Query Plan Analysis

```go
func explainQuery(db *sql.DB, query string, args ...interface{}) {
    explainQuery := "EXPLAIN " + query
    rows, err := db.Query(explainQuery, args...)
    if err != nil {
        log.Printf("Failed to explain query: %v", err)
        return
    }
    defer rows.Close()
    
    log.Printf("Query plan for: %s", query)
    for rows.Next() {
        var plan string
        rows.Scan(&plan)
        log.Printf("  %s", plan)
    }
}
```

## Getting Help

### Diagnostic Information

When reporting issues, include:

```go
func collectDiagnostics() {
    log.Printf("Go version: %s", runtime.Version())
    log.Printf("GOOS: %s, GOARCH: %s", runtime.GOOS, runtime.GOARCH)
    
    // Check if DuckDB library is available
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Printf("DuckDB connection failed: %v", err)
    } else {
        log.Printf("DuckDB connection successful")
        db.Close()
    }
    
    // Environment variables
    log.Printf("DUCKDB_LIB_DIR: %s", os.Getenv("DUCKDB_LIB_DIR"))
    log.Printf("LD_LIBRARY_PATH: %s", os.Getenv("LD_LIBRARY_PATH"))
}
```

### Minimal Reproduction

Create a minimal example that reproduces the issue:

```go
package main

import (
    "database/sql"
    "log"
    
    _ "github.com/connerohnesorge/dukdb-go/driver"
)

func main() {
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal("Failed to open database:", err)
    }
    defer db.Close()
    
    // Minimal reproduction of the issue
    _, err = db.Exec("CREATE TABLE test (id INTEGER)")
    if err != nil {
        log.Fatal("Failed to create table:", err)
    }
    
    log.Println("Test completed successfully")
}
```

### Support Channels

1. **GitHub Issues**: Report bugs and feature requests
2. **Discussions**: General questions and community support  
3. **Documentation**: Check the complete documentation
4. **Examples**: Review working examples in the repository

## Next Steps

- [Performance Guide](/guides/performance/) - Optimization techniques
- [Migration Guide](/guides/migration/) - Migrating from other drivers
- [API Reference](/reference/api/) - Complete API documentation