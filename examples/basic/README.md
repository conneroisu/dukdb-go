# Basic Example

This example demonstrates the fundamental operations of the DuckDB pure-Go driver:

## Features Demonstrated

- **Connection Management**: Opening and closing database connections
- **Table Operations**: CREATE, INSERT, SELECT, UPDATE, DELETE
- **Prepared Statements**: Using parameterized queries for better performance and security
- **Transactions**: BEGIN, COMMIT, ROLLBACK operations
- **Error Handling**: Proper error handling patterns

## Running the Example

```bash
# Run the example
go run main.go

# Run tests
go test -v

# Run benchmarks
go test -bench=. -benchmem
```

## Code Structure

- `main.go` - Main example showing basic CRUD operations
- `main_test.go` - Comprehensive tests covering all basic functionality
- `README.md` - This documentation

## What You'll Learn

1. **Database Connection**: How to connect to an in-memory DuckDB database
2. **Table Management**: Creating tables with proper column types
3. **Data Insertion**: Inserting single and multiple rows
4. **Data Querying**: Using SELECT statements with proper result handling
5. **Prepared Statements**: Performance optimization with parameter binding
6. **Data Modification**: UPDATE and DELETE operations
7. **Transaction Handling**: Ensuring data consistency with transactions

## Key Concepts

### Connection String
```go
db, err := sql.Open("duckdb", ":memory:")
```
- `:memory:` creates an in-memory database
- You can also use file paths: `"/path/to/database.db"`

### Error Handling
```go
if err != nil {
    log.Fatal(err)
}
```
- Always check for errors from database operations
- Use appropriate error handling for your use case

### Resource Cleanup
```go
defer db.Close()
defer rows.Close()
defer stmt.Close()
```
- Always close database resources to prevent memory leaks

This example provides a solid foundation for understanding the DuckDB driver and serves as a starting point for more complex applications.