# API Reference

Complete API documentation for the **dukdb-go** pure-Go DuckDB driver.

## Core Driver Package

### Import Path

```go
import _ "github.com/connerohnesorge/dukdb-go/driver"
```

The driver automatically registers itself with Go's `database/sql` package when imported.

## Connection Management

### Opening a Database

```go
func sql.Open(driverName, dataSourceName string) (*sql.DB, error)
```

**Parameters:**
- `driverName`: Must be `"duckdb"`
- `dataSourceName`: Connection string (see Connection Strings section)

**Example:**
```go
db, err := sql.Open("duckdb", ":memory:")
db, err := sql.Open("duckdb", "/path/to/database.db")
db, err := sql.Open("duckdb", ":memory:?threads=4&memory_limit=1GB")
```

### Connection Strings

#### Basic Formats

```
:memory:                    # In-memory database
/path/to/file.db           # File-based database
file:///path/to/file.db    # File URL format
```

#### Connection Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `access_mode` | string | `automatic` | `read_write`, `read_only`, `automatic` |
| `memory_limit` | string | `75%` | Memory limit (e.g., `1GB`, `512MB`) |
| `threads` | int | CPU cores | Number of threads |
| `temp_directory` | string | system default | Temporary file directory |
| `default_order` | string | `asc` | Default ordering (`asc`, `desc`) |
| `null_order` | string | `nulls_last` | NULL ordering (`nulls_first`, `nulls_last`) |

**Example:**
```go
dsn := ":memory:?memory_limit=2GB&threads=8&temp_directory=/tmp"
db, err := sql.Open("duckdb", dsn)
```

## Query Execution

### Standard database/sql Interface

The driver implements the complete `database/sql/driver` interface:

```go
type DB struct {
    // Inherits all standard sql.DB methods
}

// Query execution
func (db *DB) Query(query string, args ...interface{}) (*Rows, error)
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*Rows, error)
func (db *DB) QueryRow(query string, args ...interface{}) *Row
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *Row

// Statement execution
func (db *DB) Exec(query string, args ...interface{}) (Result, error)
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error)

// Prepared statements
func (db *DB) Prepare(query string) (*Stmt, error)
func (db *DB) PrepareContext(ctx context.Context, query string) (*Stmt, error)

// Transactions
func (db *DB) Begin() (*Tx, error)
func (db *DB) BeginTx(ctx context.Context, opts *TxOptions) (*Tx, error)
```

### Context Support

All operations support Go's context for cancellation and timeouts:

```go
// Query with timeout
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

rows, err := db.QueryContext(ctx, "SELECT * FROM large_table")
if err == context.DeadlineExceeded {
    // Query timed out
}

// Cancellable operation
ctx, cancel := context.WithCancel(context.Background())
go func() {
    time.Sleep(5 * time.Second)
    cancel() // Cancel after 5 seconds
}()

err := db.ExecContext(ctx, "INSERT INTO table SELECT * FROM another_large_table")
```

## Data Types

### Basic Types

| DuckDB Type | Go Type | Notes |
|-------------|---------|-------|
| `BOOLEAN` | `bool` | |
| `TINYINT` | `int8` | |
| `SMALLINT` | `int16` | |
| `INTEGER` | `int32` | |
| `BIGINT` | `int64` | |
| `UTINYINT` | `uint8` | |
| `USMALLINT` | `uint16` | |
| `UINTEGER` | `uint32` | |
| `UBIGINT` | `uint64` | |
| `REAL` | `float32` | |
| `DOUBLE` | `float64` | |
| `VARCHAR` | `string` | |
| `BLOB` | `[]byte` | |
| `DATE` | `time.Time` | |
| `TIME` | `time.Time` | |
| `TIMESTAMP` | `time.Time` | |
| `TIMESTAMPTZ` | `time.Time` | With timezone |

### Complex Types

Complex types are returned as JSON strings by default:

| DuckDB Type | Go Type | JSON Format |
|-------------|---------|-------------|
| `LIST(T)` | `string` | `["item1", "item2"]` |
| `STRUCT(...)` | `string` | `{"field1": "value1"}` |
| `MAP(K,V)` | `string` | `{"key1": "value1"}` |
| `UUID` | `string` | `"550e8400-e29b-41d4-a716-446655440000"` |
| `ENUM` | `string` | `"enum_value"` |

#### Working with Complex Types

```go
// Scanning LIST type
var listJSON string
err := rows.Scan(&listJSON)
if err != nil {
    return err
}

// Parse JSON
var items []string
err = json.Unmarshal([]byte(listJSON), &items)

// Using helper functions
import "github.com/connerohnesorge/dukdb-go/internal/types"

list, err := types.ParseList(listJSON)
struct, err := types.ParseStruct(structJSON)
mapData, err := types.ParseMap(mapJSON)
```

## Driver Configuration

### Statement Cache

```go
import "github.com/connerohnesorge/dukdb-go/driver"

// Configure statement cache size (default: 100)
driver.SetStatementCacheSize(500)

// Get cache statistics
stats := driver.GetCacheStats()
type CacheStats struct {
    Hits   int64
    Misses int64
    Total  int64
    Size   int
}
```

### Connection Pool

```go
// Configure connection pool
db.SetMaxOpenConns(25)           // Maximum open connections
db.SetMaxIdleConns(10)           // Maximum idle connections
db.SetConnMaxLifetime(time.Hour) // Connection lifetime
db.SetConnMaxIdleTime(10 * time.Minute) // Idle timeout

// Monitor pool statistics
stats := db.Stats()
type DBStats struct {
    MaxOpenConnections int
    OpenConnections    int
    InUse              int
    Idle               int
    WaitCount          int64
    WaitDuration       time.Duration
    MaxIdleClosed      int64
    MaxIdleTimeClosed  int64
    MaxLifetimeClosed  int64
}
```

## Error Handling

### Driver Errors

```go
import "github.com/connerohnesorge/dukdb-go/driver"

type Error struct {
    Code    int    // DuckDB error code
    Message string // Error message
}

func (e *Error) Error() string {
    return fmt.Sprintf("DuckDB error %d: %s", e.Code, e.Message)
}

// Usage
_, err := db.Exec("INVALID SQL")
if err != nil {
    if duckdbErr, ok := err.(*driver.Error); ok {
        log.Printf("DuckDB Error Code: %d", duckdbErr.Code)
        log.Printf("DuckDB Error Message: %s", duckdbErr.Message)
    }
}
```

### Common Error Codes

| Code | Meaning | Description |
|------|---------|-------------|
| 1 | `DUCKDB_ERROR_GENERIC` | Generic error |
| 2 | `DUCKDB_ERROR_SYNTAX` | SQL syntax error |
| 3 | `DUCKDB_ERROR_CONSTRAINT` | Constraint violation |
| 4 | `DUCKDB_ERROR_CATALOG` | Catalog error |
| 5 | `DUCKDB_ERROR_PARSER` | Parser error |
| 6 | `DUCKDB_ERROR_BINDER` | Binder error |
| 7 | `DUCKDB_ERROR_OPTIMIZER` | Optimizer error |
| 8 | `DUCKDB_ERROR_EXECUTOR` | Executor error |

## Transactions

### Basic Transactions

```go
// Begin transaction
tx, err := db.Begin()
if err != nil {
    return err
}
defer tx.Rollback() // Rollback if not committed

// Execute operations
_, err = tx.Exec("INSERT INTO table1 VALUES (?, ?)", val1, val2)
if err != nil {
    return err
}

_, err = tx.Exec("UPDATE table2 SET column = ? WHERE id = ?", newVal, id)
if err != nil {
    return err
}

// Commit transaction
err = tx.Commit()
if err != nil {
    return err
}
```

### Transaction Options

```go
// Begin with isolation level
tx, err := db.BeginTx(ctx, &sql.TxOptions{
    Isolation: sql.LevelSerializable,
    ReadOnly:  false,
})

// Read-only transaction
tx, err := db.BeginTx(ctx, &sql.TxOptions{
    ReadOnly: true,
})
```

### Savepoints

```go
// Create savepoint
_, err := tx.Exec("SAVEPOINT sp1")

// Do some work
_, err = tx.Exec("INSERT INTO table VALUES (?)", value)

// Rollback to savepoint
_, err = tx.Exec("ROLLBACK TO sp1")

// Release savepoint
_, err = tx.Exec("RELEASE sp1")
```

## Prepared Statements

### Creating and Using Statements

```go
// Prepare statement
stmt, err := db.Prepare("SELECT * FROM users WHERE age > ? AND city = ?")
if err != nil {
    return err
}
defer stmt.Close()

// Execute with different parameters
rows, err := stmt.Query(25, "Boston")
if err != nil {
    return err
}
defer rows.Close()

// Execute again with different parameters
rows, err = stmt.Query(30, "Cambridge")
```

### Statement Context Support

```go
// Prepare with context
stmt, err := db.PrepareContext(ctx, "SELECT * FROM table WHERE column = ?")
if err != nil {
    return err
}
defer stmt.Close()

// Execute with context
rows, err := stmt.QueryContext(ctx, value)
```

## Row Processing

### Scanning Results

```go
rows, err := db.Query("SELECT id, name, email, created_at FROM users")
if err != nil {
    return err
}
defer rows.Close()

for rows.Next() {
    var id int
    var name, email string
    var createdAt time.Time
    
    err := rows.Scan(&id, &name, &email, &createdAt)
    if err != nil {
        return err
    }
    
    // Process row data
    fmt.Printf("User %d: %s (%s) - %s\n", id, name, email, createdAt)
}

// Check for iteration errors
if err := rows.Err(); err != nil {
    return err
}
```

### Column Information

```go
rows, err := db.Query("SELECT * FROM users")
if err != nil {
    return err
}
defer rows.Close()

// Get column names
columns, err := rows.Columns()
if err != nil {
    return err
}
fmt.Printf("Columns: %v\n", columns)

// Get column types (if supported)
columnTypes, err := rows.ColumnTypes()
if err != nil {
    return err
}

for _, colType := range columnTypes {
    fmt.Printf("Column: %s, Type: %s\n", 
        colType.Name(), colType.DatabaseTypeName())
}
```

### Nullable Scanning

```go
var id int
var name sql.NullString
var age sql.NullInt64
var score sql.NullFloat64

err := rows.Scan(&id, &name, &age, &score)
if err != nil {
    return err
}

if name.Valid {
    fmt.Printf("Name: %s\n", name.String)
}
if age.Valid {
    fmt.Printf("Age: %d\n", age.Int64)
}
```

## Type Helpers

### List Type Helper

```go
import "github.com/connerohnesorge/dukdb-go/internal/types"

// Create LIST from Go slice
list := types.NewList([]string{"apple", "banana", "cherry"})
fmt.Println(list.String()) // ["apple", "banana", "cherry"]

// Parse LIST from JSON string
listData, err := types.ParseList(`["apple", "banana", "cherry"]`)
if err != nil {
    return err
}

// Access elements
for i, item := range listData {
    fmt.Printf("Item %d: %s\n", i, item)
}
```

### Struct Type Helper

```go
// Create STRUCT from Go map
data := map[string]interface{}{
    "name": "Alice",
    "age":  30,
    "active": true,
}
structData := types.NewStruct(data)
fmt.Println(structData.String()) // {"name": "Alice", "age": 30, "active": true}

// Parse STRUCT from JSON string
structData, err := types.ParseStruct(`{"name": "Alice", "age": 30}`)
if err != nil {
    return err
}

// Access fields
name, ok := structData["name"].(string)
if ok {
    fmt.Printf("Name: %s\n", name)
}
```

### Map Type Helper

```go
// Create MAP from Go map
mapData := types.NewMap(map[string]string{
    "color": "red",
    "size":  "large",
})
fmt.Println(mapData.String()) // {"color": "red", "size": "large"}

// Parse MAP from JSON string
mapData, err := types.ParseMap(`{"color": "red", "size": "large"}`)
if err != nil {
    return err
}

// Access values
color, ok := mapData["color"].(string)
if ok {
    fmt.Printf("Color: %s\n", color)
}
```

### UUID Helper

```go
import "github.com/google/uuid"

// Generate UUID
id := uuid.New()

// Insert UUID
_, err := db.Exec("INSERT INTO table (id) VALUES (?)", id.String())

// Scan UUID
var uuidStr string
err = rows.Scan(&uuidStr)
if err != nil {
    return err
}

// Parse back to UUID
parsedUUID, err := uuid.Parse(uuidStr)
if err != nil {
    return err
}
```

## Performance Monitoring

### Connection Statistics

```go
// Get current pool statistics
stats := db.Stats()
fmt.Printf("Open connections: %d\n", stats.OpenConnections)
fmt.Printf("In use: %d\n", stats.InUse)
fmt.Printf("Idle: %d\n", stats.Idle)
fmt.Printf("Wait count: %d\n", stats.WaitCount)
fmt.Printf("Wait duration: %s\n", stats.WaitDuration)
```

### Cache Statistics

```go
import "github.com/connerohnesorge/dukdb-go/driver"

stats := driver.GetCacheStats()
fmt.Printf("Cache size: %d\n", stats.Size)
fmt.Printf("Cache hits: %d\n", stats.Hits)
fmt.Printf("Cache misses: %d\n", stats.Misses)
fmt.Printf("Hit rate: %.2f%%\n", 
    float64(stats.Hits) / float64(stats.Total) * 100)
```

## Advanced Features

### Raw Connection Access

For advanced use cases, you can access the underlying connection:

```go
import "github.com/connerohnesorge/dukdb-go/driver"

// This is typically not needed for standard applications
conn := db.Driver().(*driver.Driver)
// Access driver-specific functionality
```

### Library Information

```go
import "github.com/connerohnesorge/dukdb-go/internal/purego"

// Get DuckDB library version (if available)
version := purego.GetLibraryVersion()
fmt.Printf("DuckDB version: %s\n", version)
```

## Best Practices

### Connection Management

```go
// Good: Configure pool appropriately
db.SetMaxOpenConns(runtime.NumCPU() * 2)
db.SetMaxIdleConns(runtime.NumCPU())
db.SetConnMaxLifetime(time.Hour)

// Good: Always close resources
defer db.Close()
defer rows.Close()
defer stmt.Close()
```

### Error Handling

```go
// Good: Handle specific error types
if err != nil {
    if duckdbErr, ok := err.(*driver.Error); ok {
        switch duckdbErr.Code {
        case 2: // Syntax error
            return fmt.Errorf("SQL syntax error: %s", duckdbErr.Message)
        case 3: // Constraint violation
            return fmt.Errorf("constraint violation: %s", duckdbErr.Message)
        default:
            return fmt.Errorf("database error: %s", duckdbErr.Message)
        }
    }
    return err
}
```

### Statement Reuse

```go
// Good: Prepare once, execute many times
stmt, err := db.Prepare("INSERT INTO table (col1, col2) VALUES (?, ?)")
if err != nil {
    return err
}
defer stmt.Close()

for _, item := range items {
    _, err := stmt.Exec(item.Col1, item.Col2)
    if err != nil {
        return err
    }
}
```

## Next Steps

- [Performance Guide](/guides/performance/) - Optimization techniques
- [Complex Types Guide](/guides/complex-types/) - Working with advanced types
- [Examples](/examples/) - Complete code examples