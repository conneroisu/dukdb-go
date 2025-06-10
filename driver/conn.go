package driver

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/connerohnesorge/dukdb-go/internal/purego"
)

// Conn implements the database/sql/driver.Conn interface
type Conn struct {
	duckdb *purego.DuckDB
	db     purego.Database
	conn   purego.Connection
}

// Prepare returns a prepared statement
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.duckdb.Prepare(c.conn, query)
	if err != nil {
		return nil, err
	}
	
	return &Stmt{
		conn:   c,
		duckdb: c.duckdb,
		stmt:   stmt,
		query:  query,
	}, nil
}

// Close closes the connection
func (c *Conn) Close() error {
	c.duckdb.Disconnect(c.conn)
	c.duckdb.CloseDatabase(c.db)
	return nil
}

// Begin starts a transaction
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a transaction with options
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// Execute BEGIN statement
	if err := c.duckdb.Execute(c.conn, "BEGIN TRANSACTION"); err != nil {
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
	
	// Direct execution with context support
	done := make(chan error, 1)
	
	go func() {
		done <- c.duckdb.Execute(c.conn, query)
	}()
	
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-done:
		if err != nil {
			return nil, err
		}
		return &Result{}, nil
	}
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
	
	// Direct query execution with context support
	done := make(chan struct{})
	var result *purego.QueryResult
	var err error
	
	go func() {
		defer close(done)
		result, err = c.duckdb.Query(c.conn, query)
	}()
	
	select {
	case <-ctx.Done():
		// Context cancelled - we can't easily cancel the C operation
		// but we can return the error immediately
		return nil, ctx.Err()
	case <-done:
		if err != nil {
			return nil, err
		}
		
		return &Rows{
			duckdb: c.duckdb,
			result: result,
			cols:   result.Columns(),
			ctx:    ctx,
		}, nil
	}
}

// Ping verifies the connection
func (c *Conn) Ping(ctx context.Context) error {
	// Execute a simple query to verify connection
	result, err := c.duckdb.Query(c.conn, "SELECT 1")
	if err != nil {
		return err
	}
	result.Close()
	return nil
}

// CheckNamedValue is called to check named values
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	// Convert time.Time to int64 for DuckDB
	if t, ok := nv.Value.(time.Time); ok {
		// Store as microseconds since epoch for timestamp types
		nv.Value = t.UnixNano() / 1000
	}
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