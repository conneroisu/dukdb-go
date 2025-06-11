package driver

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/connerohnesorge/dukdb-go/internal/engine"
)

// Stmt implements the database/sql/driver.Stmt interface
type Stmt struct {
	conn       *Conn
	engineStmt *engine.PreparedStatement
	query      string
	closed     bool
}

// Close closes the statement
func (s *Stmt) Close() error {
	if !s.closed {
		s.closed = true
		// Pure Go implementation doesn't require explicit statement cleanup
	}
	return nil
}

// NumInput returns the number of placeholder parameters
func (s *Stmt) NumInput() int {
	// Our pure Go implementation doesn't track parameter count yet
	// Return -1 to indicate unknown
	return -1
}

// Exec executes a query that doesn't return rows
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

// ExecContext executes a query that doesn't return rows with context
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed {
		return nil, fmt.Errorf("statement is closed")
	}

	// Convert args to parameter map
	params := make(map[int]interface{})
	for _, arg := range args {
		if arg.Name != "" {
			// Named parameters not supported yet
			return nil, fmt.Errorf("named parameters not supported")
		}
		params[arg.Ordinal] = arg.Value
	}

	// Set parameters on the prepared statement
	s.engineStmt.SetParams(params)

	// Execute the prepared statement (not the raw query)
	result, err := s.engineStmt.Execute(ctx)
	if err != nil {
		return nil, err
	}
	// Close the result since we don't need it for Exec
	result.Close()

	return &Result{}, nil
}

// Query executes a query that returns rows
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

// QueryContext executes a query that returns rows with context
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.closed {
		return nil, fmt.Errorf("statement is closed")
	}

	// Convert args to parameter map
	params := make(map[int]interface{})
	for _, arg := range args {
		if arg.Name != "" {
			// Named parameters not supported yet
			return nil, fmt.Errorf("named parameters not supported")
		}
		params[arg.Ordinal] = arg.Value
	}

	// Set parameters on the prepared statement
	s.engineStmt.SetParams(params)

	// Execute the prepared statement (not the raw query)
	result, err := s.engineStmt.Execute(ctx)
	if err != nil {
		return nil, err
	}

	return &Rows{
		engineResult: result,
		stmt:         s,
		ctx:          ctx,
	}, nil
}

// valuesToNamedValues converts []driver.Value to []driver.NamedValue
func valuesToNamedValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		named[i] = driver.NamedValue{
			Ordinal: i + 1, // 1-based indexing
			Value:   arg,
		}
	}
	return named
}
