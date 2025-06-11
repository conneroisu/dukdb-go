# DuckDB-Go Development Guide

## Overview

This guide provides comprehensive information for developers working on the pure-Go DuckDB implementation. It covers the architecture, key components, and development workflows.

## Architecture Overview

### Core Components

```
┌─────────────────────────────────────────────────────┐
│                   SQL Interface                      │
│                 (database/sql)                       │
└─────────────────────┬───────────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────────┐
│                  Driver Layer                        │
│              (driver package)                        │
│  - Conn, Stmt, Rows, Result implementations        │
└─────────────────────┬───────────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────────┐
│                 Engine Layer                         │
│              (engine package)                        │
│  ┌─────────────┐ ┌─────────────┐ ┌──────────────┐ │
│  │   Parser    │ │   Planner   │ │   Executor   │ │
│  └─────────────┘ └─────────────┘ └──────────────┘ │
│  ┌─────────────┐ ┌─────────────┐ ┌──────────────┐ │
│  │  Catalog    │ │ Aggregates  │ │  Operators   │ │
│  └─────────────┘ └─────────────┘ └──────────────┘ │
└─────────────────────┬───────────────────────────────┘
                      │
┌─────────────────────┴───────────────────────────────┐
│                Storage Layer                         │
│             (storage package)                        │
│  - Vectors, DataChunks, Type System                │
└─────────────────────────────────────────────────────┘
```

### Query Execution Flow

1. **SQL Parsing** (`parser.go`)
   - Converts SQL text to AST (Abstract Syntax Tree)
   - Handles SELECT, INSERT, UPDATE, DELETE, CREATE TABLE
   - Parameter placeholder (?) recognition

2. **Query Planning** (`planner.go`)
   - Transforms AST to logical plan
   - Type inference and validation
   - Aggregate function creation

3. **Query Execution** (`executor.go`)
   - Executes physical plan
   - Manages transaction context
   - Returns QueryResult

4. **Vectorized Operations** (`operators.go`)
   - Filter, Project, Sort, Limit operators
   - Expression evaluation
   - Type-safe operations on DataChunks

## Key Data Structures

### Storage Types

```go
// Logical type system (storage/vector.go)
type LogicalType struct {
    ID        TypeID
    Width     uint8  // For DECIMAL types
    Scale     uint8  // For DECIMAL types
    Collation string // For STRING types
}

// Supported types
- TypeBoolean, TypeInteger, TypeBigInt
- TypeFloat, TypeDouble
- TypeVarchar, TypeTimestamp
- TypeDate, TypeTime
```

### Expression Types

```go
// Expression hierarchy (engine/parser.go)
type Expression interface{}

- ColumnExpr{Table, Column}      // Column reference
- ConstantExpr{Value}            // Literal value
- ParameterExpr{Index}           // Placeholder (?)
- BinaryExpr{Left, Op, Right}    // Binary operation
- FunctionExpr{Name, Args}       // Function call
```

### Operator Types

```go
// Vectorized operators (engine/operators.go)
- FilterOperator     // WHERE clause
- ProjectOperator    // SELECT columns
- AggregateOperator  // GROUP BY/aggregates
- SortOperator       // ORDER BY
- LimitOperator      // LIMIT/OFFSET
```

## Development Workflows

### Adding a New SQL Function

1. **Define the function** in `engine/aggregates.go`:
```go
type MyAggregate struct {
    inputType storage.LogicalType
}

func (m *MyAggregate) Initialize() AggregateState { ... }
func (m *MyAggregate) Update(state AggregateState, value interface{}) error { ... }
func (m *MyAggregate) Finalize(state AggregateState) (interface{}, error) { ... }
func (m *MyAggregate) GetResultType() storage.LogicalType { ... }
```

2. **Register in CreateAggregateFunction**:
```go
case "MYFUNCTION":
    return &MyAggregate{inputType: inputType}, nil
```

3. **Add type inference** in `planner.go`:
```go
case "MYFUNCTION":
    return storage.LogicalType{ID: storage.TypeDouble}
```

### Adding a New Operator

1. **Implement VectorizedOperator interface**:
```go
type MyOperator struct {
    schema []storage.LogicalType
}

func (m *MyOperator) Execute(ctx context.Context, input *storage.DataChunk) (*storage.DataChunk, error) { ... }
func (m *MyOperator) GetOutputSchema() []storage.LogicalType { ... }
```

2. **Integrate into planner** in `planner.go`

3. **Add tests** in appropriate test files

### Fixing Type Mismatches

1. **Check type inference** in `inferExpressionType()` (operators.go)
2. **Verify storage conversion** in `SetValue()/GetValue()` (vector.go)
3. **Update driver conversion** in `convertToDriverValue()` (rows.go)
4. **Ensure planner type detection** in aggregate creation (planner.go)

## Testing

### Unit Tests
```bash
# Run all tests
go test ./...

# Run specific package tests
go test ./internal/engine/...
go test ./internal/storage/...

# Run with coverage
go test -cover ./...
```

### Benchmark Tests
```bash
# Run analytical benchmarks
cd benchmarks
go test -bench=. ./analytical/

# Run specific benchmark
go test -bench=BenchmarkSimpleAggregation ./analytical/
```

### Integration Tests
- Located in `test/` directory
- Test end-to-end SQL functionality
- Include compatibility tests

## Common Issues and Solutions

### "column not found" Errors
- Check column mapping in `evaluateExpressionWithContext()`
- Verify qualified column handling (table.column)
- Ensure proper context propagation

### Type Conversion Errors
- Check type inference in planner
- Verify storage type conversions
- Update driver type mappings

### Performance Issues
- Profile with `go test -cpuprofile`
- Check for unnecessary allocations
- Consider vectorized operations

## Code Style Guidelines

1. **Error Handling**
   - Always wrap errors with context
   - Use `fmt.Errorf("context: %w", err)`

2. **Testing**
   - Table-driven tests preferred
   - Include edge cases
   - Test error conditions

3. **Documentation**
   - Document public APIs
   - Include examples for complex functions
   - Update this guide for significant changes

## Debugging Tips

### Enable Verbose Logging
```go
// Add debug prints in key locations
fmt.Printf("DEBUG: Planning query: %+v\n", ast)
fmt.Printf("DEBUG: Column types: %+v\n", schema)
```

### Trace Query Execution
1. Add breakpoints in `Execute()` methods
2. Print intermediate results
3. Verify type conversions

### Common Breakpoint Locations
- `parser.go`: ParseSQL()
- `planner.go`: CreatePlan()
- `executor.go`: Execute()
- `operators.go`: evaluateExpressionWithContext()

## Contributing

1. **Fork and Clone**
   ```bash
   git clone https://github.com/yourusername/dukdb-go
   cd dukdb-go
   ```

2. **Create Feature Branch**
   ```bash
   git checkout -b feature/my-feature
   ```

3. **Make Changes**
   - Write tests first
   - Implement feature
   - Update documentation

4. **Run Tests**
   ```bash
   go test ./...
   cd benchmarks && go test -bench=. ./analytical/
   ```

5. **Submit PR**
   - Clear description
   - Link related issues
   - Include test results

## Resources

- [DuckDB Documentation](https://duckdb.org/docs/)
- [Go database/sql](https://golang.org/pkg/database/sql/)
- [Go SQL Driver Tutorial](https://github.com/golang/go/wiki/SQLDrivers)

## Current Status

As of June 2025:
- ✅ Core SQL functionality working
- ✅ 5/8 analytical benchmarks passing
- 🚧 UNION/Subqueries need parser extensions
- ⚡ JOIN performance needs optimization

See `benchmarks/ANALYTICAL_BENCHMARK_STATUS.md` for detailed status.