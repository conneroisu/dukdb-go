package benchmarks

import (
	"database/sql"
	_ "github.com/connerohnesorge/dukdb-go" // Pure-Go DuckDB driver
)

// OpenPureGoDatabase opens a pure-Go DuckDB database connection
func OpenPureGoDatabase(dsn string) (*sql.DB, error) {
	return sql.Open("duckdb", dsn)
}