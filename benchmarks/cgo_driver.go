// +build cgo

package benchmarks

import (
	"database/sql"
	_ "github.com/marcboeker/go-duckdb" // Original CGO DuckDB driver
)

// RegisterCGODriver registers the CGO DuckDB driver for benchmarking
func init() {
	// The driver is automatically registered by the import
}

// OpenCGODatabase opens a CGO DuckDB database connection
func OpenCGODatabase(dsn string) (*sql.DB, error) {
	return sql.Open("duckdb", dsn)
}