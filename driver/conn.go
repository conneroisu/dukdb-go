package driver

import (
	"context"
	"database/sql/driver"

	"github.com/connerohnesorge/dukdb-go/internal/engine"
)

// Conn implements the database/sql/driver.Conn interface
type Conn struct {
	engineConn *engine.Connection
	db         *engine.Database
}

// Prepare returns a prepared statement
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	prepared, err := c.engineConn.Prepare(query)
	if err != nil {
		return nil, err
	}

	return &Stmt{
		conn:       c,
		engineStmt: prepared,
		query:      query,
	}, nil
}

// Close closes the connection
func (c *Conn) Close() error {
	return c.engineConn.Close()
}

// Begin starts a transaction
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a transaction with options
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// Execute BEGIN statement
	if err := c.engineConn.Execute(ctx, "BEGIN"); err != nil {
		return nil, err
	}

	return &Tx{conn: c}, nil
}

// ExecContext executes a query that doesn't return rows
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Check context cancellation before starting
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if len(args) > 0 {
		// Prepare and execute with parameters
		stmt, err := c.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()

		return stmt.(*Stmt).ExecContext(ctx, args)
	}

	// Direct execution
	err := c.engineConn.Execute(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Result{}, nil
}

// QueryContext executes a query that returns rows
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Check context cancellation before starting
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if len(args) > 0 {
		// Prepare and execute with parameters
		stmt, err := c.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		// Don't close stmt here - it will be closed when rows are closed

		return stmt.(*Stmt).QueryContext(ctx, args)
	}

	// Direct query execution
	result, err := c.engineConn.Query(ctx, query)
	if err != nil {
		return nil, err
	}

	return &Rows{
		engineResult: result,
		ctx:          ctx,
	}, nil
}

// Ping verifies the connection
func (c *Conn) Ping(ctx context.Context) error {
	// Execute a simple query to verify connection
	result, err := c.engineConn.Query(ctx, "SELECT 1")
	if err != nil {
		return err
	}
	result.Close()
	return nil
}

// CheckNamedValue is called to check named values
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	// Our pure Go implementation handles type conversions internally
	return nil
}

// Result implements driver.Result
type Result struct {
	lastInsertId int64
	rowsAffected int64
}

// LastInsertId returns the last insert ID
func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

// RowsAffected returns the number of affected rows
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}