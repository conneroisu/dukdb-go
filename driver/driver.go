package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/connerohnesorge/dukdb-go/internal/ast"
	"github.com/connerohnesorge/dukdb-go/internal/parser"
)

// Driver implements the database/sql/driver.Driver interface
type Driver struct{}

// Open returns a new connection to the database
func (d *Driver) Open(name string) (driver.Conn, error) {
	return NewConnection(name)
}

func init() {
	sql.Register("duckdb", &Driver{})
}

// Connection implements driver.Conn interface
type Connection struct {
	dsn    string
	engine *Engine
	mu     sync.RWMutex
	closed bool
}

// NewConnection creates a new connection
func NewConnection(dsn string) (*Connection, error) {
	engine := NewEngine()
	return &Connection{
		dsn:    dsn,
		engine: engine,
	}, nil
}

// Prepare returns a prepared statement, bound to this connection
func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if c.closed {
		return nil, driver.ErrBadConn
	}

	stmt, err := parser.ParseSQL(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	return &Statement{
		conn:  c,
		query: query,
		stmt:  stmt,
	}, nil
}

// Close invalidates and potentially stops any current prepared statements and transactions
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.closed = true
	return nil
}

// Begin starts and returns a new transaction
func (c *Connection) Begin() (driver.Tx, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	if c.closed {
		return nil, driver.ErrBadConn
	}

	return &Transaction{conn: c}, nil
}

// Statement implements driver.Stmt interface
type Statement struct {
	conn  *Connection
	query string
	stmt  ast.Statement
}

// Close closes the statement
func (s *Statement) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters
func (s *Statement) NumInput() int {
	// For now, we don't support parameters
	return 0
}

// Exec executes a query that doesn't return rows
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedValuesToValues(args))
}

// Query executes a query that may return rows
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), namedValuesToValues(args))
}

// ExecContext executes a query with context
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	result, err := s.conn.engine.Execute(ctx, s.stmt)
	if err != nil {
		return nil, err
	}

	return &Result{
		lastInsertID: 0,
		rowsAffected: result.RowsAffected,
	}, nil
}

// QueryContext executes a query with context that returns rows
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	result, err := s.conn.engine.Execute(ctx, s.stmt)
	if err != nil {
		return nil, err
	}

	return &Rows{
		columns: result.Columns,
		data:    result.Data,
		pos:     0,
	}, nil
}

// Result implements driver.Result interface
type Result struct {
	lastInsertID int64
	rowsAffected int64
}

// LastInsertId returns the database's auto-generated ID
func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns the number of rows affected by the query
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Rows implements driver.Rows interface
type Rows struct {
	columns []string
	data    [][]interface{}
	pos     int
}

// Columns returns the names of the columns
func (r *Rows) Columns() []string {
	return r.columns
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	return nil
}

// Next is called to populate the next row of data
func (r *Rows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}

	row := r.data[r.pos]
	for i, val := range row {
		if i < len(dest) {
			dest[i] = val
		}
	}
	r.pos++
	return nil
}

// Transaction implements driver.Tx interface
type Transaction struct {
	conn *Connection
}

// Commit commits the transaction
func (tx *Transaction) Commit() error {
	return nil
}

// Rollback aborts the transaction
func (tx *Transaction) Rollback() error {
	return nil
}

// Helper functions

func namedValuesToValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return named
}

// ExecutionResult represents the result of query execution
type ExecutionResult struct {
	Columns      []string
	Data         [][]interface{}
	RowsAffected int64
}

// Engine represents a simple in-memory database engine
type Engine struct {
	tables map[string]*Table
	mu     sync.RWMutex
}

// NewEngine creates a new database engine
func NewEngine() *Engine {
	return &Engine{
		tables: make(map[string]*Table),
	}
}

// Table represents a database table
type Table struct {
	Name    string
	Columns []Column
	Rows    [][]interface{}
	mu      sync.RWMutex
}

// Column represents a table column
type Column struct {
	Name string
	Type ast.DataType
}

// Execute executes a parsed statement
func (e *Engine) Execute(ctx context.Context, stmt ast.Statement) (*ExecutionResult, error) {
	switch s := stmt.(type) {
	case *ast.SelectStmt:
		return e.executeSelect(ctx, s)
	case *ast.InsertStmt:
		return e.executeInsert(ctx, s)
	case *ast.UpdateStmt:
		return e.executeUpdate(ctx, s)
	case *ast.DeleteStmt:
		return e.executeDelete(ctx, s)
	case *ast.CreateTableStmt:
		return e.executeCreateTable(ctx, s)
	case *ast.DropTableStmt:
		return e.executeDropTable(ctx, s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

func (e *Engine) executeSelect(ctx context.Context, stmt *ast.SelectStmt) (*ExecutionResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Simple implementation for basic SELECT queries
	if len(stmt.From) == 0 {
		// Handle SELECT without FROM (e.g., SELECT 1+1)
		columns := make([]string, len(stmt.SelectList))
		data := make([]interface{}, len(stmt.SelectList))
		
		for i, item := range stmt.SelectList {
			if item.Alias != "" {
				columns[i] = item.Alias
			} else {
				columns[i] = item.Expression.String()
			}
			
			// Simple literal evaluation
			data[i] = e.evaluateExpression(item.Expression)
		}
		
		return &ExecutionResult{
			Columns: columns,
			Data:    [][]interface{}{data},
		}, nil
	}

	// Handle SELECT from table
	tableName := e.getTableNameFromRef(stmt.From[0])
	table, exists := e.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	table.mu.RLock()
	defer table.mu.RUnlock()

	// Get column names for SELECT *
	var columns []string
	if len(stmt.SelectList) == 1 && stmt.SelectList[0].Expression.String() == "*" {
		for _, col := range table.Columns {
			columns = append(columns, col.Name)
		}
	} else {
		for _, item := range stmt.SelectList {
			if item.Alias != "" {
				columns = append(columns, item.Alias)
			} else {
				columns = append(columns, item.Expression.String())
			}
		}
	}

	// Simple row filtering
	var filteredRows [][]interface{}
	for _, row := range table.Rows {
		if stmt.Where == nil || e.evaluateWhereCondition(stmt.Where, table.Columns, row) {
			if len(stmt.SelectList) == 1 && stmt.SelectList[0].Expression.String() == "*" {
				filteredRows = append(filteredRows, row)
			} else {
				newRow := make([]interface{}, len(stmt.SelectList))
				for i, item := range stmt.SelectList {
					newRow[i] = e.evaluateSelectExpression(item.Expression, table.Columns, row)
				}
				filteredRows = append(filteredRows, newRow)
			}
		}
	}

	return &ExecutionResult{
		Columns: columns,
		Data:    filteredRows,
	}, nil
}

func (e *Engine) executeInsert(ctx context.Context, stmt *ast.InsertStmt) (*ExecutionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tableName := stmt.Table.Name
	table, exists := e.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	var rowsAffected int64 = 0

	for _, valueRow := range stmt.Values {
		row := make([]interface{}, len(table.Columns))
		
		if len(stmt.Columns) > 0 {
			// Insert with specific columns
			for i, colName := range stmt.Columns {
				colIndex := e.findColumnIndex(table.Columns, colName)
				if colIndex >= 0 && i < len(valueRow) {
					row[colIndex] = e.evaluateExpression(valueRow[i])
				}
			}
		} else {
			// Insert all columns in order
			for i, expr := range valueRow {
				if i < len(row) {
					row[i] = e.evaluateExpression(expr)
				}
			}
		}
		
		table.Rows = append(table.Rows, row)
		rowsAffected++
	}

	return &ExecutionResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (e *Engine) executeUpdate(ctx context.Context, stmt *ast.UpdateStmt) (*ExecutionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tableName := stmt.Table.Name
	table, exists := e.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	var rowsAffected int64 = 0

	for i, row := range table.Rows {
		if stmt.Where == nil || e.evaluateWhereCondition(stmt.Where, table.Columns, row) {
			for _, assignment := range stmt.Set {
				colIndex := e.findColumnIndex(table.Columns, assignment.Column)
				if colIndex >= 0 {
					table.Rows[i][colIndex] = e.evaluateExpression(assignment.Value)
				}
			}
			rowsAffected++
		}
	}

	return &ExecutionResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (e *Engine) executeDelete(ctx context.Context, stmt *ast.DeleteStmt) (*ExecutionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tableName := stmt.Table.Name
	table, exists := e.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	var newRows [][]interface{}
	var rowsAffected int64 = 0

	for _, row := range table.Rows {
		if stmt.Where == nil || e.evaluateWhereCondition(stmt.Where, table.Columns, row) {
			rowsAffected++
		} else {
			newRows = append(newRows, row)
		}
	}

	table.Rows = newRows

	return &ExecutionResult{
		RowsAffected: rowsAffected,
	}, nil
}

func (e *Engine) executeCreateTable(ctx context.Context, stmt *ast.CreateTableStmt) (*ExecutionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tableName := stmt.Name.Name

	if _, exists := e.tables[tableName]; exists && !stmt.IfExists {
		return nil, fmt.Errorf("table %s already exists", tableName)
	}

	if _, exists := e.tables[tableName]; exists && stmt.IfExists {
		return &ExecutionResult{}, nil
	}

	columns := make([]Column, len(stmt.Columns))
	for i, colDef := range stmt.Columns {
		columns[i] = Column{
			Name: colDef.Name,
			Type: colDef.Type,
		}
	}

	e.tables[tableName] = &Table{
		Name:    tableName,
		Columns: columns,
		Rows:    make([][]interface{}, 0),
	}

	return &ExecutionResult{}, nil
}

func (e *Engine) executeDropTable(ctx context.Context, stmt *ast.DropTableStmt) (*ExecutionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tableName := stmt.Name.Name

	if _, exists := e.tables[tableName]; !exists && !stmt.IfExists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	delete(e.tables, tableName)

	return &ExecutionResult{}, nil
}

// Helper methods

func (e *Engine) getTableNameFromRef(ref ast.TableRef) string {
	switch t := ref.(type) {
	case *ast.TableName:
		return t.Name
	default:
		return ""
	}
}

func (e *Engine) findColumnIndex(columns []Column, name string) int {
	for i, col := range columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

func (e *Engine) evaluateExpression(expr ast.Expression) interface{} {
	switch e := expr.(type) {
	case *ast.LiteralExpr:
		return e.Value
	case *ast.ColumnRef:
		// For simple cases, return the column name
		return e.Column
	case *ast.BinaryExpr:
		left := e.evaluateExpression(e.Left)
		right := e.evaluateExpression(e.Right)
		return e.evaluateBinaryOperation(e.Operator, left, right)
	default:
		return nil
	}
}

func (e *Engine) evaluateSelectExpression(expr ast.Expression, columns []Column, row []interface{}) interface{} {
	switch e := expr.(type) {
	case *ast.LiteralExpr:
		return e.Value
	case *ast.ColumnRef:
		colIndex := e.findColumnIndex(columns, e.Column)
		if colIndex >= 0 && colIndex < len(row) {
			return row[colIndex]
		}
		return nil
	case *ast.BinaryExpr:
		left := e.evaluateSelectExpression(e.Left, columns, row)
		right := e.evaluateSelectExpression(e.Right, columns, row)
		return e.evaluateBinaryOperation(e.Operator, left, right)
	default:
		return nil
	}
}

func (e *Engine) evaluateWhereCondition(expr ast.Expression, columns []Column, row []interface{}) bool {
	result := e.evaluateSelectExpression(expr, columns, row)
	if b, ok := result.(bool); ok {
		return b
	}
	return false
}

func (e *Engine) evaluateBinaryOperation(op ast.BinaryOperator, left, right interface{}) interface{} {
	switch op {
	case ast.Equal:
		return e.compareValues(left, right) == 0
	case ast.NotEqual:
		return e.compareValues(left, right) != 0
	case ast.LessThan:
		return e.compareValues(left, right) < 0
	case ast.LessThanOrEqual:
		return e.compareValues(left, right) <= 0
	case ast.GreaterThan:
		return e.compareValues(left, right) > 0
	case ast.GreaterThanOrEqual:
		return e.compareValues(left, right) >= 0
	case ast.Add:
		return e.addValues(left, right)
	case ast.Subtract:
		return e.subtractValues(left, right)
	case ast.Multiply:
		return e.multiplyValues(left, right)
	case ast.Divide:
		return e.divideValues(left, right)
	default:
		return nil
	}
}

func (e *Engine) compareValues(left, right interface{}) int {
	// Simple string/number comparison
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)
	
	if leftStr == rightStr {
		return 0
	} else if leftStr < rightStr {
		return -1
	}
	return 1
}

func (e *Engine) addValues(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			return l + r
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(float64); ok {
			return l + r
		}
	}
	// Try string conversion
	if leftStr, err := strconv.ParseFloat(fmt.Sprintf("%v", left), 64); err == nil {
		if rightStr, err := strconv.ParseFloat(fmt.Sprintf("%v", right), 64); err == nil {
			return leftStr + rightStr
		}
	}
	return nil
}

func (e *Engine) subtractValues(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			return l - r
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(float64); ok {
			return l - r
		}
	}
	return nil
}

func (e *Engine) multiplyValues(left, right interface{}) interface{} {
	if l, ok := left.(int64); ok {
		if r, ok := right.(int64); ok {
			return l * r
		}
	}
	if l, ok := left.(float64); ok {
		if r, ok := right.(float64); ok {
			return l * r
		}
	}
	return nil
}

func (e *Engine) divideValues(left, right interface{}) interface{} {
	if l, ok := left.(float64); ok {
		if r, ok := right.(float64); ok && r != 0 {
			return l / r
		}
	}
	return nil
}