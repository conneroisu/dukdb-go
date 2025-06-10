package driver

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/connerohnesorge/dukdb-go/internal/purego"
)

// Stmt implements the database/sql/driver.Stmt interface
type Stmt struct {
	conn   *Conn
	duckdb *purego.DuckDB
	stmt   purego.PreparedStatement
	query  string
	closed bool
}

// Close closes the statement
func (s *Stmt) Close() error {
	if !s.closed {
		s.duckdb.DestroyPrepared(s.stmt)
		s.closed = true
	}
	return nil
}

// NumInput returns the number of placeholder parameters
func (s *Stmt) NumInput() int {
	// TODO: Implement parameter counting
	// For now, return -1 to indicate unknown
	return -1
}

// Exec executes a statement that doesn't return rows
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

// ExecContext executes a statement with context
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Clear any previous bindings
	if err := s.duckdb.ClearBindings(s.stmt); err != nil {
		return nil, err
	}

	// Bind parameters
	for _, arg := range args {
		idx := uint64(arg.Ordinal - 1) // Convert to 0-based index
		if err := s.duckdb.BindValue(s.stmt, idx, arg.Value); err != nil {
			return nil, fmt.Errorf("failed to bind parameter %d: %w", arg.Ordinal, err)
		}
	}

	result, err := s.duckdb.ExecutePrepared(s.stmt)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	return &Result{
		rowsAffected: int64(result.RowCount()),
	}, nil
}

// Query executes a query that returns rows
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

// QueryContext executes a query with context
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// Clear any previous bindings
	if err := s.duckdb.ClearBindings(s.stmt); err != nil {
		return nil, err
	}

	// Bind parameters
	for _, arg := range args {
		idx := uint64(arg.Ordinal - 1) // Convert to 0-based index
		if err := s.duckdb.BindValue(s.stmt, idx, arg.Value); err != nil {
			return nil, fmt.Errorf("failed to bind parameter %d: %w", arg.Ordinal, err)
		}
	}

	result, err := s.duckdb.ExecutePrepared(s.stmt)
	if err != nil {
		return nil, err
	}

	return &Rows{
		duckdb: s.duckdb,
		result: result,
		cols:   result.Columns(),
		stmt:   s,
	}, nil
}

// valuesToNamedValues converts []driver.Value to []driver.NamedValue
func valuesToNamedValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		named[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return named
}
