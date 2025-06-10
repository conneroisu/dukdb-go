The goal of this repository is rather simple: 
Map out the ways used to implement SQL databases in Go comparing CGO-based drivers to pure-Go drivers and use that to implement duckdb in pure go.

Essentially, to provide a comprehensive list of Go SQL database drivers by implementation type (pure-Go, CGO-based), to provide a summary of the key insights and recommendations, and 

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
2. **For PostgreSQL-family databases**: Use pgx (actively maintained) over lib/pq (maintenance mode)
3. **For MySQL**: go-sql-driver/mysql is the de facto standard
4. **For SQLite**: Choose based on deployment needs - mattn/go-sqlite3 for features, pure-Go alternatives for simplicity
5. **For Oracle**: go-ora for pure-Go needs, godror for maximum compatibility
6. **For SQL Server**: Use the official Microsoft driver (pure-Go)
