// +build !cgo

package benchmarks

import (
	"database/sql"
	"errors"
)

// OpenCGODatabase returns an error when CGO is disabled
func OpenCGODatabase(dsn string) (*sql.DB, error) {
	return nil, errors.New("CGO DuckDB driver not available: build with CGO_ENABLED=1")
}