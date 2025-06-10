# Advanced Example

This example demonstrates advanced features of the DuckDB pure-Go driver, showcasing the analytical capabilities that make DuckDB powerful for data processing tasks.

## Features Demonstrated

- **Date/Time Operations**: Working with DATE, TIME, and TIMESTAMP types
- **BLOB Handling**: Storing and retrieving binary data
- **Analytical Queries**: Aggregations, GROUP BY, and statistical functions
- **Window Functions**: RANK(), ROW_NUMBER(), PARTITION BY
- **Complex Data Types**: DECIMAL for financial calculations
- **Advanced SQL**: Subqueries, CTEs, and analytical expressions

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

- `main.go` - Main example showing advanced DuckDB features
- `main_test.go` - Comprehensive tests for all advanced functionality
- `README.md` - This documentation

## What You'll Learn

### 1. Date and Time Handling

```sql
CREATE TABLE events (
    event_date DATE,
    event_time TIME,
    event_timestamp TIMESTAMP
);
```

- Working with different temporal types
- Date arithmetic and functions
- Timezone handling

### 2. Binary Data (BLOBs)

```sql
CREATE TABLE files (
    filename VARCHAR,
    data BLOB
);
```

- Storing binary files in the database
- Retrieving and manipulating binary data
- Handling null bytes and special characters

### 3. Analytical Queries

```sql
SELECT 
    category,
    COUNT(*) as total_items,
    SUM(amount) as total_sales,
    AVG(amount) as avg_sale,
    MIN(amount) as min_sale,
    MAX(amount) as max_sale
FROM sales 
GROUP BY category;
```

### 4. Window Functions

```sql
SELECT 
    name,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
    AVG(salary) OVER (PARTITION BY department) as dept_avg
FROM employees;
```

## Key Concepts

### Analytical Processing

DuckDB excels at analytical workloads:

- **Columnar Storage**: Efficient for analytical queries
- **Vectorized Execution**: Fast aggregations and transformations
- **Advanced SQL**: Complete SQL standard support

### Data Types

```go
// Go types mapping to DuckDB types
var date time.Time     // DATE
var timestamp time.Time // TIMESTAMP
var blob []byte        // BLOB
var decimal float64    // DECIMAL
```

### Performance Optimization

- Use appropriate data types for your use case
- Leverage DuckDB's analytical functions
- Consider partitioning for large datasets

### Error Handling

```go
// Always handle potential errors
rows, err := db.Query(complexQuery)
if err != nil {
    return fmt.Errorf("query failed: %w", err)
}
defer rows.Close()
```

## Use Cases

This example is perfect for:

- **Business Intelligence**: Reporting and analytics
- **Data Warehousing**: ETL operations and data processing
- **Financial Applications**: Precise decimal calculations
- **Log Analysis**: Time-series data processing
- **File Management**: Binary data storage and retrieval

The advanced features demonstrated here showcase DuckDB's power for analytical workloads while maintaining the simplicity of the standard Go database/sql interface.
