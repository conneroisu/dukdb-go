# DuckDB Pure-Go Implementation: CGO-to-Purego Conversion Task

## Objective

Convert the existing CGO-based DuckDB Go driver (github.com/marcboeker/go-duckdb) to a pure-Go implementation using purego for initial C library interfacing, then progressively replace C dependencies with native Go code.

## Current State

- **Existing**: CGO-based DuckDB driver requiring C toolchain
- **Goal**: Pure-Go implementation with no CGO dependencies
- **Strategy**: Use purego as transitional FFI layer while implementing native Go components

## Implementation Approach

### Phase 1: Purego Wrapper (Weeks 1-2)

Create a purego-based wrapper that maintains API compatibility with the existing CGO driver:

1. **Setup purego interface**

   ```go
   // internal/purego/duckdb.go
   package purego

   import "github.com/ebitengine/purego"

   type DuckDB struct {
       lib    uintptr
       handle uintptr
   }

   func Open(path string) (*DuckDB, error) {
       lib, err := purego.Dlopen(getDuckDBLibrary(), purego.RTLD_NOW)
       // Register functions and initialize
   }
   ```

1. **Map essential C functions**

   - duckdb_open/close
   - duckdb_connect/disconnect
   - duckdb_query/prepare
   - Result fetching functions
   - Type conversion functions

1. **Implement database/sql driver interface**

   - Use purego wrapper internally
   - Maintain exact same API as CGO version
   - Enable compatibility testing

### Phase 2: Core Go Components (Weeks 3-6)

Begin replacing C dependencies with Go implementations:

1. **Type System** (Week 3)

   - Implement DuckDB types in pure Go (HUGEINT, LIST, STRUCT)
   - Create type conversion layer
   - Remove C type dependencies

1. **SQL Parser** (Week 4)

   - Generate Goyacc parser for DuckDB SQL
   - Parse to native Go AST
   - Remove C parser dependency

1. **Storage Engine** (Week 5-6)

   - Implement columnar storage in Go
   - Add Parquet support via parquet-go
   - Create in-memory storage backend

### Phase 3: Query Execution (Weeks 7-10)

Replace C execution engine:

1. **Volcano Iterator Model**

   - Implement basic operators (Scan, Filter, Join)
   - Create execution framework
   - Add parallel execution support

1. **Aggregation Framework**

   - Implement common aggregates
   - Add GROUP BY support
   - Window function framework

1. **Optimization Layer**

   - Rule-based optimizer
   - Predicate pushdown
   - Join reordering

### Phase 4: Advanced Features (Weeks 11-12)

Complete remaining functionality:

1. **Complex Types**

   - Full LIST/STRUCT operations
   - MAP type support
   - Nested type handling

1. **Analytics Functions**

   - Window functions
   - Statistical aggregates
   - Time series operations

## Key Files to Analyze

From CGO driver (github.com/marcboeker/go-duckdb):

- `duckdb.go` - Main driver interface
- `connector.go` - Connection management
- `statement.go` - Query execution
- `rows.go` - Result set handling
- `types.go` - Type conversions

## Testing Strategy

1. **Compatibility Tests**

   - Run identical queries on both implementations
   - Compare results byte-for-byte
   - Benchmark performance differences

1. **SQL Logic Tests**

   - Port DuckDB's test suite
   - Use .test file format
   - Ensure behavioral compatibility

1. **Performance Benchmarks**

   - TPC-H queries
   - Memory usage profiling
   - CGO vs purego vs pure-Go comparison

## Critical Considerations

### Purego Limitations

- Manual struct alignment required
- Float support only on 64-bit platforms
- Callback limitations on Linux
- Maximum 2000 callbacks per process

### Memory Management

- Track C allocations when using purego
- Implement proper cleanup/defer patterns
- Consider Go GC pressure with large datasets

### Platform Support

- Test on Linux, macOS, Windows
- Verify cross-compilation works
- Handle missing shared libraries gracefully

## Success Criteria

1. **Functional**: Pass 95% of DuckDB SQL logic tests
1. **Compatible**: database/sql interface works identically to CGO version
1. **Performance**: Within 2x of CGO version for common queries
1. **Deployment**: Single binary with no C dependencies
1. **Cross-platform**: Builds for all major OS/arch combinations

## Getting Started

1. Fork the repository
1. Set up purego wrapper in `internal/purego/`
1. Create compatibility test harness
1. Begin incremental replacement of C functions
1. Track progress in implementation checklist

## Resources

- DuckDB C API: https://duckdb.org/docs/api/c/overview
- Purego docs: https://github.com/ebitengine/purego
- CGO driver: https://github.com/marcboeker/go-duckdb
- SQL Logic Tests: https://www.sqlite.org/sqllogictest/

## Questions to Address

1. Which DuckDB version to target initially?
1. How to handle DuckDB extensions?
1. Minimum Go version requirement?
1. License compatibility (GPL v3.0 implications)?

Begin by creating the purego wrapper and establishing the test harness for compatibility validation.

______________________________________________________________________

## Original Research: Go SQL Database Driver Implementation Analysis

# Comprehensive Go SQL Database Drivers by Implementation Type

## Summary Table

| Database Type | Pure-Go Drivers | CGO-based Drivers |
|--------------|-----------------|-------------------|
| **PostgreSQL** | • [lib/pq](https://github.com/lib/pq) *(maintenance mode)*<br>• [pgx](https://github.com/jackc/pgx) *(recommended, actively maintained)* | None found - all major drivers are pure-Go |
| **MySQL/MariaDB** | • [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) *(most popular)*<br>• [ziutek/mymysql](https://github.com/ziutek/mymysql)<br>• [go-mysql-org/go-mysql](https://github.com/go-mysql-org/go-mysql) *(includes replication)* | None found - all major drivers are pure-Go |
| **SQLite** | • [modernc.org/sqlite](https://gitlab.com/cznic/sqlite) *(transpiled C)*<br>• [ncruces/go-sqlite3](https://github.com/ncruces/go-sqlite3) *(WebAssembly-based)*<br>• [glebarez/go-sqlite](https://github.com/glebarez/go-sqlite) | • [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) *(most popular, requires gcc)* |
| **Oracle** | • [sijms/go-ora](https://github.com/sijms/go-ora) *(no Oracle Client needed)* | • [godror/godror](https://github.com/godror/godror) *(recommended by Oracle, requires Instant Client)*<br>• [mattn/go-oci8](https://github.com/mattn/go-oci8) *(requires Oracle Client)*<br>• [rana/ora](https://gopkg.in/rana/ora.v4) |
| **SQL Server** | • [microsoft/go-mssqldb](https://github.com/microsoft/go-mssqldb) *(official Microsoft driver)*<br>• [denisenkom/go-mssqldb](https://github.com/denisenkom/go-mssqldb) *(original, now superseded)* | • [minus5/gofreetds](https://github.com/minus5/gofreetds) *(requires FreeTDS)* |
| **IBM DB2** | None found | • [ibmdb/go_ibm_db](https://github.com/ibmdb/go_ibm_db) *(requires DB2 CLI driver)*<br>• [asifjalil/cli](https://github.com/asifjalil/cli) *(Linux/Unix only)* |
| **Firebird** | • [nakagami/firebirdsql](https://github.com/nakagami/firebirdsql) | None found |
| **SAP HANA** | • [SAP/go-hdb](https://github.com/SAP/go-hdb) *(official SAP driver)* | • SAP ODBC driver via cgo wrapper |
| **Teradata** | None found | • Teradata GoSQL Driver *(plugin-based, limited compatibility)* |
| **Amazon Redshift** | • Use PostgreSQL drivers ([lib/pq](https://github.com/lib/pq) or [pgx](https://github.com/jackc/pgx)) | • Amazon ODBC/JDBC drivers via cgo wrappers |
| **Google Cloud Spanner** | • [googleapis/go-sql-spanner](https://github.com/googleapis/go-sql-spanner) *(official)*<br>• [rakyll/go-sql-driver-spanner](https://github.com/rakyll/go-sql-driver-spanner) | None found |
| **CockroachDB** | • Use PostgreSQL drivers ([lib/pq](https://github.com/lib/pq) or [pgx](https://github.com/jackc/pgx))<br>• [cockroachdb/cockroach-go](https://github.com/cockroachdb/cockroach-go) *(helper library)* | None found |
| **YugabyteDB** | • Use PostgreSQL drivers for YSQL<br>• [yugabyte/pgx](https://github.com/yugabyte/pgx) *(smart driver)*<br>• [yugabyte/gocql](https://github.com/yugabyte/gocql) *(for YCQL)* | None found |
| **ClickHouse** | • [ClickHouse/clickhouse-go](https://github.com/ClickHouse/clickhouse-go)<br>• [mailru/go-clickhouse](https://github.com/mailru/go-clickhouse) *(HTTP-based)* | None found |
| **Snowflake** | • [snowflakedb/gosnowflake](https://github.com/snowflakedb/gosnowflake) | None found |
| **DuckDB** | None found | • [marcboeker/go-duckdb](https://github.com/marcboeker/go-duckdb) |
| **Exasol** | • [exasol/exasol-driver-go](https://github.com/exasol/exasol-driver-go) | None found |
| **Databricks** | • [databricks/databricks-sql-go](https://github.com/databricks/databricks-sql-go) | None found |
| **Vertica** | • [vertica/vertica-sql-go](https://github.com/vertica/vertica-sql-go) | None found |
| **Trino** | • [trinodb/trino-go-client](https://github.com/trinodb/trino-go-client) | None found |

## Key Insights

### Pure-Go Dominance

The Go ecosystem has matured significantly, with **pure-Go drivers now available for most major databases**. This trend reflects the community's preference for deployment simplicity and cross-platform compatibility.

### Databases with Only Pure-Go Options

- PostgreSQL, MySQL/MariaDB, Firebird, SAP HANA, Google Cloud Spanner, CockroachDB, YugabyteDB, ClickHouse, Snowflake, Exasol, Databricks, Vertica, and Trino all have mature pure-Go implementations without requiring CGO alternatives.

### Databases Requiring CGO

- **IBM DB2** and **Teradata** only have CGO-based drivers due to proprietary protocols
- **DuckDB** currently only offers a CGO driver
- **Apache Derby/JavaDB** has no Go driver (Java-based database)

### PostgreSQL Wire Protocol Success

PostgreSQL's open wire protocol has enabled pure-Go implementations for PostgreSQL itself and all PostgreSQL-compatible databases (CockroachDB, YugabyteDB YSQL, Amazon Redshift).

### Deployment Considerations

- **Pure-Go advantages**: Static binary deployment, no external dependencies, cross-compilation support, better debugging
- **CGO requirements**: Database client libraries at runtime, platform-specific builds, potential licensing complexity

### Recommendations

1. **For new projects**: Always prefer pure-Go drivers when available
1. **For PostgreSQL-family databases**: Use pgx (actively maintained) over lib/pq (maintenance mode)
1. **For MySQL**: go-sql-driver/mysql is the de facto standard
1. **For SQLite**: Choose based on deployment needs - mattn/go-sqlite3 for features, pure-Go alternatives for simplicity
1. **For Oracle**: go-ora for pure-Go needs, godror for maximum compatibility
1. **For SQL Server**: Use the official Microsoft driver (pure-Go)
