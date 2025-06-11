# DuckDB Pure-Go Driver Examples

This directory contains comprehensive examples demonstrating the capabilities of the DuckDB pure-Go driver. Each example is organized in its own folder with working code, tests, and documentation.

## 📁 Example Structure

Each example folder contains:

- `main.go` - Runnable example demonstrating specific features
- `main_test.go` - Comprehensive test suite for the example
- `README.md` - Detailed documentation and learning guide

## 📚 Available Examples

### [Basic](./basic/) - Fundamental Operations

Get started with the essential database operations.

**Features:**

- Database connections and management
- Basic CRUD operations (CREATE, INSERT, SELECT, UPDATE, DELETE)
- Prepared statements and parameter binding
- Transaction handling (BEGIN, COMMIT, ROLLBACK)
- Error handling patterns

**Perfect for:** First-time users, basic database operations

```bash
cd basic/
go run main.go
go test -v
```

### [Advanced](./advanced/) - Analytical Features

Explore DuckDB's analytical capabilities and advanced SQL features.

**Features:**

- Date/Time types and operations
- BLOB handling for binary data
- Complex analytical queries
- Window functions and aggregations
- Advanced SQL constructs

**Perfect for:** Data analysis, business intelligence, reporting

```bash
cd advanced/
go run main.go
go test -v
```

### [Complex Types](./complex-types/) - Modern Data Types

Learn about DuckDB's sophisticated type system for modern applications.

**Features:**

- UUID type for unique identifiers
- LIST/ARRAY types for collections
- STRUCT types for composite data
- MAP types for key-value data
- Nested complex types and compositions

**Perfect for:** Modern applications, JSON-like data, document storage

```bash
cd complex-types/
go run main.go
go test -v
```

### [Performance](./performance/) - Optimization & Benchmarking

Master performance testing, optimization, and monitoring.

**Features:**

- Connection performance and pooling
- Query optimization strategies
- Insert performance patterns
- Transaction performance analysis
- Concurrent access patterns
- Memory usage monitoring

**Perfect for:** Performance tuning, benchmarking, production optimization

```bash
cd performance/
go run main.go
go test -v
go test -bench=. -benchmem
```

### [Parquet Integration](./parquet-examples/) - Working with Parquet Files

Learn how to integrate DuckDB with parquet-go for efficient columnar storage.

**Features:**

- Reading parquet files into DuckDB tables
- Writing query results to parquet format
- Data aggregation and analytics on parquet data
- Simulated join operations between parquet files
- Complex analytics with window function simulation

**Examples Included:**

1. **01_basic_read.go** - Basic parquet file reading and querying
2. **02_write_parquet.go** - Writing query results to parquet files
3. **03_aggregation.go** - Aggregation queries on parquet data
4. **04_joins.go** - Join operations between multiple parquet files
5. **05_window_functions.go** - Complex analytics with simulated window functions

**Perfect for:** Data lake integration, columnar analytics, ETL pipelines

```bash
cd parquet-examples/
go run 01_basic_read.go
go run 02_write_parquet.go
# etc.
```

## 🚀 Quick Start

### Run All Examples

```bash
# From the project root
make run-basic
make run-adv
make run-complex
make run-perf

# Run parquet examples
cd examples/parquet-examples/
for f in *.go; do go run $f; done
```

### Test All Examples

```bash
# Run all example tests
make test-examples

# Run all example benchmarks
make bench-examples
```

### Individual Examples

```bash
# Navigate to any example
cd examples/basic/

# Run the example
go run main.go

# Run tests
go test -v

# Run benchmarks (where available)
go test -bench=. -benchmem
```

## 📖 Learning Path

### 1. Start with **Basic**

- Learn fundamental concepts
- Understand the database/sql interface
- Master connection and transaction handling

### 2. Explore **Advanced**

- Discover DuckDB's analytical power
- Learn complex SQL features
- Work with different data types

### 3. Master **Complex Types**

- Understand modern data modeling
- Work with nested structures
- Handle semi-structured data

### 4. Optimize with **Performance**

- Learn benchmarking techniques
- Optimize for your use case
- Monitor production performance

### 5. Integrate with **Parquet**

- Work with columnar storage
- Build analytics pipelines
- Process large datasets efficiently

## 🎯 Use Case Mapping

### Web Applications

- **Basic** - User management, content storage
- **Advanced** - Analytics, reporting, time-series data
- **Complex Types** - User preferences, configuration data

### Data Analytics

- **Basic** - Data loading and basic queries
- **Advanced** - Statistical analysis, aggregations
- **Performance** - Large dataset processing

### Microservices

- **Basic** - Service-specific data storage
- **Complex Types** - Event sourcing, configuration management
- **Performance** - High-throughput scenarios

### ETL/Data Processing

- **Advanced** - Data transformations, analytics
- **Complex Types** - Schema evolution, nested data
- **Performance** - Bulk operations, concurrent processing

## 🛠 Development Tips

### Environment Setup

Each example works with the Nix development environment:

```bash
# Enter development shell
nix develop

# All dependencies and DuckDB are available
go run examples/basic/main.go
```

### Testing Patterns

All examples follow consistent testing patterns:

- Comprehensive test coverage
- Error handling validation
- Performance benchmarks where applicable
- Skip tests gracefully if DuckDB unavailable

### Code Organization

Examples demonstrate best practices:

- Proper resource cleanup (`defer` statements)
- Error handling at each step
- Transaction management
- Connection pooling considerations

## 🔧 Troubleshooting

### Common Issues

1. **Connection Errors**

   - Ensure DuckDB library is available
   - Use `nix develop` for automatic setup
   - Check `DUCKDB_LIB_DIR` environment variable

1. **Test Skipping**

   - Tests gracefully skip if DuckDB unavailable
   - This is expected behavior in CI environments
   - All tests pass when DuckDB is properly configured

1. **Performance Variations**

   - Performance results vary by system
   - Use benchmarks for relative comparisons
   - Consider system load and available resources

### Getting Help

- Review individual example READMEs for specific guidance
- Check the main project README for setup instructions
- Review test files for usage patterns

## 📈 Next Steps

After working through these examples:

1. **Read the Architecture Documents** - Understand the driver's internal design
1. **Explore the Source Code** - Learn from the implementation
1. **Build Your Application** - Apply the patterns to your use case
1. **Contribute** - Help improve the driver and examples

Each example builds upon the previous ones, creating a comprehensive learning journey through the DuckDB pure-Go driver's capabilities.
