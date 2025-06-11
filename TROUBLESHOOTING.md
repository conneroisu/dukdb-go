# DuckDB-Go Troubleshooting Guide

## Common Error Messages and Solutions

### Type Mismatch Errors

#### "expected float64, got int64"
**Cause**: Type inference mismatch, often with timestamp columns in aggregates

**Solution**:
1. Check aggregate function type inference in `planner.go`:
```go
case "order_date", "created_at", "timestamp":
    inputType = storage.LogicalType{ID: storage.TypeTimestamp}
```

2. Verify storage type handling in `vector.go`
3. Update driver conversion in `rows.go`:
```go
if colType.ID == storage.TypeTimestamp {
    if intVal, ok := val.(int64); ok {
        return time.UnixMicro(intVal)
    }
}
```

#### "expected string, got int32"
**Cause**: Column type mismatch, often in WHERE or HAVING clauses

**Solution**:
1. Add proper column mapping in `inferExpressionType()`
2. Check schema column count and positions
3. Verify context propagation in operators

### Column Resolution Errors

#### "column X not found in columns: [...]"
**Cause**: Column name resolution failure

**Solution**:
1. Check if using qualified names (table.column)
2. Verify column mapping in `evaluateExpressionWithContext()`
3. For JOINs, ensure proper table offset calculation:
```go
if e.Table == "orders" {
    leftTableCols := 4  // customers has 4 columns
    colIdx = leftTableCols + columnPosition
}
```

#### "qualified column X.Y not found"
**Cause**: Table alias not properly resolved

**Solution**:
1. Check JOIN column mapping logic
2. Verify table alias handling in parser
3. Ensure context includes all column names

### Parser Errors

#### "near 'UNION': syntax error"
**Cause**: UNION syntax not implemented in parser

**Status**: Known limitation - requires parser extension

**Workaround**: Use separate queries and combine results in application

#### "subquery in FROM must have an alias"
**Cause**: Derived table support limited

**Workaround**: Rewrite query without subquery or use CTEs when implemented

### Execution Errors

#### "execution error: filter evaluation error"
**Cause**: Expression evaluation failure

**Solution**:
1. Check expression parsing in `parseComplexExpression()`
2. Verify binary operator handling
3. Ensure proper parameter binding for prepared statements

#### "planning error: table not found"
**Cause**: Table name resolution or catalog lookup failure

**Solution**:
1. Verify table exists with `SELECT * FROM table_name`
2. Check case sensitivity
3. Ensure proper schema handling

### Performance Issues

#### Slow JOIN Queries
**Cause**: Nested loop join implementation

**Solution**:
1. Reduce data size with WHERE clauses
2. Add indexes (when implemented)
3. Consider query restructuring

**Future**: Hash join and merge join implementations planned

#### Memory Usage
**Cause**: Full materialization of results

**Solution**:
1. Use LIMIT clauses
2. Process results in chunks
3. Close result sets promptly

## Debugging Techniques

### 1. Enable Query Logging
Add logging to trace query execution:
```go
// In engine/connection.go
func (c *Connection) Query(ctx context.Context, sql string) (*QueryResult, error) {
    log.Printf("Executing query: %s", sql)
    // ... rest of implementation
}
```

### 2. Trace Type Information
Add type debugging in operators:
```go
// In engine/operators.go
fmt.Printf("Column %d: expected %v, got %T (value: %v)\n", 
    i, schema[i], value, value)
```

### 3. Dump Schema Information
Print schema details during planning:
```go
// In engine/planner.go
fmt.Printf("Table schema: %+v\n", schema)
fmt.Printf("Column names: %v\n", columnNames)
```

### 4. Parameter Binding Debug
Trace parameter substitution:
```go
// In engine/connection.go
func (ps *PreparedStatement) bindExpression(expr Expression) Expression {
    fmt.Printf("Binding expression: %+v\n", expr)
    // ... rest of implementation
}
```

## Test Case Templates

### Minimal Type Mismatch Reproduction
```go
db, _ := sql.Open("duckdb", ":memory:")
db.Exec(`CREATE TABLE test (id INTEGER, ts TIMESTAMP)`)
db.Exec(`INSERT INTO test VALUES (1, ?)`, time.Now())

// This query should work:
rows, err := db.Query(`SELECT MAX(ts) FROM test`)
if err != nil {
    log.Printf("Type error: %v", err)
}
```

### JOIN Column Resolution Test
```go
db.Exec(`CREATE TABLE t1 (id INTEGER, name VARCHAR)`)
db.Exec(`CREATE TABLE t2 (id INTEGER, t1_id INTEGER)`)

// Test qualified columns:
rows, err := db.Query(`
    SELECT t1.id, t1.name, t2.id 
    FROM t1 
    JOIN t2 ON t1.id = t2.t1_id
`)
```

## Known Limitations

1. **No UNION/UNION ALL support** - Parser limitation
2. **No subquery support** - Parser limitation
3. **No window functions** - Not implemented
4. **Limited JOIN optimization** - Only nested loop joins
5. **No indexes** - Full table scans only
6. **No EXPLAIN PLAN** - Cannot inspect query plans

## Getting Help

1. **Check existing issues**: Look for similar problems in GitHub issues
2. **Create minimal reproduction**: Isolate the problem
3. **Include version info**: Go version, OS, commit hash
4. **Provide full error**: Include stack traces if available

## Useful Commands

```bash
# Run with race detector
go test -race ./...

# Generate CPU profile
go test -cpuprofile cpu.prof -bench=BenchmarkSimpleJoin ./benchmarks/analytical/
go tool pprof cpu.prof

# Check memory usage
go test -memprofile mem.prof -bench=. ./benchmarks/analytical/
go tool pprof mem.prof

# Trace execution
go test -trace trace.out ./...
go tool trace trace.out
```