package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

// TransactionState represents the state of a transaction
type TransactionState int

const (
	TxnStateNone TransactionState = iota
	TxnStateAuto
	TxnStateManual
)

// Connection represents a connection to the database
type Connection struct {
	id       uint64
	database *Database
	txnState TransactionState
	txn      *Transaction
	mu       sync.Mutex
	closed   bool
}

// Query executes a SQL query and returns the result
func (c *Connection) Query(ctx context.Context, sql string) (*QueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return nil, fmt.Errorf("connection is closed")
	}
	
	// Parse the SQL
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	
	// Plan the query
	planner := NewPlanner(c.database.catalog)
	plan, err := planner.CreatePlan(stmt)
	if err != nil {
		return nil, fmt.Errorf("planning error: %w", err)
	}
	
	// Execute the plan
	executor := NewExecutor(c)
	result, err := executor.Execute(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}
	
	return result, nil
}

// Execute executes a SQL statement (no result expected)
func (c *Connection) Execute(ctx context.Context, sql string) error {
	result, err := c.Query(ctx, sql)
	if err != nil {
		return err
	}
	_ = result.Close() // Closing errors not critical for Execute
	return nil
}

// Prepare prepares a SQL statement
func (c *Connection) Prepare(sql string) (*PreparedStatement, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return nil, fmt.Errorf("connection is closed")
	}
	
	// Parse the SQL
	stmt, err := ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	
	// Create prepared statement
	prepared := &PreparedStatement{
		connection: c,
		sql:        sql,
		ast:        stmt,
		params:     make(map[int]interface{}),
	}
	
	return prepared, nil
}

// BeginTransaction starts a new transaction
func (c *Connection) BeginTransaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	return c.beginTransactionInternal()
}

// beginTransactionInternal is the internal version that doesn't lock
func (c *Connection) beginTransactionInternal() error {
	if c.txnState != TxnStateNone {
		return fmt.Errorf("transaction already in progress")
	}
	
	c.txn = &Transaction{
		id:         generateTransactionID(),
		connection: c,
		state:      TxnStateActive,
	}
	c.txnState = TxnStateManual
	
	return nil
}

// Commit commits the current transaction
func (c *Connection) Commit() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	return c.commitInternal()
}

// commitInternal is the internal version that doesn't lock
func (c *Connection) commitInternal() error {
	if c.txnState != TxnStateManual {
		return fmt.Errorf("no manual transaction in progress")
	}
	
	if c.txn != nil {
		_ = c.txn.Commit() // Commit errors handled at transaction level
	}
	
	c.txn = nil
	c.txnState = TxnStateNone
	
	return nil
}

// Rollback rolls back the current transaction
func (c *Connection) Rollback() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	return c.rollbackInternal()
}

// rollbackInternal is the internal version that doesn't lock
func (c *Connection) rollbackInternal() error {
	if c.txnState != TxnStateManual {
		return fmt.Errorf("no manual transaction in progress")
	}
	
	if c.txn != nil {
		_ = c.txn.Rollback() // Rollback errors are not critical
	}
	
	c.txn = nil
	c.txnState = TxnStateNone
	
	return nil
}

// Close closes the connection
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.closed {
		return nil
	}
	
	// Rollback any active transaction
	if c.txnState == TxnStateManual && c.txn != nil {
		_ = c.txn.Rollback() // Rollback errors are not critical
	}
	
	c.closed = true
	c.database.connections.Delete(c.id)
	
	return nil
}

// QueryResult represents the result of a query
type QueryResult struct {
	chunks       []*storage.DataChunk
	current      int
	columns      []Column
	closed       bool
	mu           sync.Mutex
	rowsAffected int64 // For UPDATE/DELETE operations
}

// Column represents column metadata
type Column struct {
	Name      string
	Type      storage.LogicalType
	Nullable  bool
	Precision uint8
	Scale     uint8
}

// Columns returns the column information
func (qr *QueryResult) Columns() []Column {
	return qr.columns
}

// Next advances to the next chunk
func (qr *QueryResult) Next() bool {
	qr.mu.Lock()
	defer qr.mu.Unlock()
	
	if qr.closed || qr.current >= len(qr.chunks)-1 {
		return false
	}
	
	qr.current++
	return true
}

// GetChunk returns the current data chunk
func (qr *QueryResult) GetChunk() *storage.DataChunk {
	qr.mu.Lock()
	defer qr.mu.Unlock()
	
	if qr.closed || qr.current >= len(qr.chunks) {
		return nil
	}
	
	return qr.chunks[qr.current]
}

// Close closes the query result
func (qr *QueryResult) Close() error {
	qr.mu.Lock()
	defer qr.mu.Unlock()
	
	qr.closed = true
	qr.chunks = nil
	return nil
}

// GetRowsAffected returns the number of rows affected by UPDATE/DELETE
func (qr *QueryResult) GetRowsAffected() int64 {
	qr.mu.Lock()
	defer qr.mu.Unlock()
	
	return qr.rowsAffected
}

// PreparedStatement represents a prepared SQL statement
type PreparedStatement struct {
	connection *Connection
	sql        string
	ast        Statement
	params     map[int]interface{}
	mu         sync.Mutex
}

// Bind binds a value to a parameter
func (ps *PreparedStatement) Bind(index int, value interface{}) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	ps.params[index] = value
	return nil
}

// SetParams sets all parameters at once
func (ps *PreparedStatement) SetParams(params map[int]interface{}) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	ps.params = params
}

// Execute executes the prepared statement
func (ps *PreparedStatement) Execute(ctx context.Context) (*QueryResult, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	// Create a copy of the AST with bound parameters
	boundAST := ps.bindParameters(ps.ast)
	
	// Plan the query
	planner := NewPlanner(ps.connection.database.catalog)
	plan, err := planner.CreatePlan(boundAST)
	if err != nil {
		return nil, fmt.Errorf("planning error: %w", err)
	}
	
	// Execute the plan
	executor := NewExecutor(ps.connection)
	result, err := executor.Execute(ctx, plan)
	if err != nil {
		return nil, fmt.Errorf("execution error: %w", err)
	}
	
	return result, nil
}

// bindParameters creates a copy of the AST with parameters bound
func (ps *PreparedStatement) bindParameters(stmt Statement) Statement {
	switch s := stmt.(type) {
	case *InsertStatement:
		// Create a copy with bound values
		newValues := make([][]Expression, len(s.Values))
		for i, row := range s.Values {
			newRow := make([]Expression, len(row))
			for j, expr := range row {
				if param, ok := expr.(*ParameterExpr); ok {
					// Replace parameter with actual value
					if val, exists := ps.params[param.Index]; exists {
						newRow[j] = &ConstantExpr{Value: val}
					} else {
						// Parameter not bound - use NULL
						newRow[j] = &ConstantExpr{Value: nil}
					}
				} else {
					newRow[j] = expr
				}
			}
			newValues[i] = newRow
		}
		
		return &InsertStatement{
			Table:   s.Table,
			Columns: s.Columns,
			Values:  newValues,
		}
		
	case *SelectStatement:
		// Create a copy with bound WHERE clause and JOINs
		return &SelectStatement{
			From:       s.From,
			Joins:      s.Joins,  // Include JOINs in the copy!
			Columns:    s.Columns,
			Where:      ps.bindExpression(s.Where),
			GroupBy:    s.GroupBy,
			Having:     ps.bindExpression(s.Having),
			OrderBy:    s.OrderBy,
			Limit:      s.Limit,
			Offset:     s.Offset,
		}
		
	case *UpdateStatement:
		// Create a copy with bound values
		newSets := make([]SetClause, len(s.Sets))
		for i, set := range s.Sets {
			newSets[i] = SetClause{
				Column: set.Column,
				Value:  ps.bindExpression(set.Value),
			}
		}
		
		return &UpdateStatement{
			Table: s.Table,
			Sets:  newSets,
			Where: ps.bindExpression(s.Where),
		}
		
	case *DeleteStatement:
		// Create a copy with bound WHERE clause
		return &DeleteStatement{
			Table: s.Table,
			Where: ps.bindExpression(s.Where),
		}
		
	default:
		// For other statement types, return as-is for now
		return stmt
	}
}

// bindExpression recursively binds parameters in an expression
func (ps *PreparedStatement) bindExpression(expr Expression) Expression {
	if expr == nil {
		return nil
	}
	
	switch e := expr.(type) {
	case *ParameterExpr:
		// Replace parameter with actual value
		if val, exists := ps.params[e.Index]; exists {
			return &ConstantExpr{Value: val}
		}
		// Parameter not bound - use NULL
		return &ConstantExpr{Value: nil}
		
	case *BinaryExpr:
		// Recursively bind left and right expressions
		return &BinaryExpr{
			Left:  ps.bindExpression(e.Left),
			Op:    e.Op,
			Right: ps.bindExpression(e.Right),
		}
		
	default:
		// Other expression types don't contain parameters
		return expr
	}
}

// ClearBindings clears all parameter bindings
func (ps *PreparedStatement) ClearBindings() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	
	ps.params = make(map[int]interface{})
}