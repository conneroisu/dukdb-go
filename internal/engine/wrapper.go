package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"unsafe"

	"github.com/connerohnesorge/dukdb-go/internal/purego"
	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

// PureGoWrapper wraps the pure-Go engine to implement the purego interfaces
type PureGoWrapper struct {
	database *Database
}

// NewPureGoWrapper creates a new pure-Go wrapper
func NewPureGoWrapper() *PureGoWrapper {
	return &PureGoWrapper{}
}

// Open opens a database connection
func (w *PureGoWrapper) Open(path string) (purego.Database, error) {
	db, err := NewDatabase(path)
	if err != nil {
		return 0, err
	}
	
	w.database = db
	// Return a fake handle
	return purego.Database(1), nil
}

// CloseDatabase closes a database
func (w *PureGoWrapper) CloseDatabase(db purego.Database) {
	if w.database != nil {
		w.database.Close()
		w.database = nil
	}
}

// Connect creates a new connection to the database
func (w *PureGoWrapper) Connect(db purego.Database) (purego.Connection, error) {
	if w.database == nil {
		return 0, fmt.Errorf("database not open")
	}
	
	conn, err := w.database.Connect()
	if err != nil {
		return 0, err
	}
	
	// Store connection and return ID
	return purego.Connection(conn.id), nil
}

// Disconnect closes a connection
func (w *PureGoWrapper) Disconnect(conn purego.Connection) {
	// Find and close connection
	w.database.connections.Range(func(key, value interface{}) bool {
		if c, ok := value.(*Connection); ok && c.id == uint64(conn) {
			c.Close()
			return false
		}
		return true
	})
}

// Query executes a SQL query and returns the result
func (w *PureGoWrapper) Query(conn purego.Connection, query string) (*purego.QueryResult, error) {
	// Find connection
	var connection *Connection
	w.database.connections.Range(func(key, value interface{}) bool {
		if c, ok := value.(*Connection); ok && c.id == uint64(conn) {
			connection = c
			return false
		}
		return true
	})
	
	if connection == nil {
		return nil, fmt.Errorf("connection not found")
	}
	
	// Execute query
	result, err := connection.Query(context.Background(), query)
	if err != nil {
		return nil, err
	}
	
	// Convert to purego QueryResult
	return convertToPuregoResult(result), nil
}

// Execute executes a SQL statement (no result expected)
func (w *PureGoWrapper) Execute(conn purego.Connection, query string) error {
	// Find connection
	var connection *Connection
	w.database.connections.Range(func(key, value interface{}) bool {
		if c, ok := value.(*Connection); ok && c.id == uint64(conn) {
			connection = c
			return false
		}
		return true
	})
	
	if connection == nil {
		return fmt.Errorf("connection not found")
	}
	
	return connection.Execute(context.Background(), query)
}

// Prepare prepares a SQL statement
func (w *PureGoWrapper) Prepare(conn purego.Connection, query string) (purego.PreparedStatement, error) {
	// Find connection
	var connection *Connection
	w.database.connections.Range(func(key, value interface{}) bool {
		if c, ok := value.(*Connection); ok && c.id == uint64(conn) {
			connection = c
			return false
		}
		return true
	})
	
	if connection == nil {
		return 0, fmt.Errorf("connection not found")
	}
	
	stmt, err := connection.Prepare(query)
	if err != nil {
		return 0, err
	}
	
	// Store statement reference (simplified)
	return purego.PreparedStatement(uintptr(unsafe.Pointer(stmt))), nil
}

// convertToPuregoResult converts our QueryResult to purego.QueryResult
func convertToPuregoResult(result *QueryResult) *purego.QueryResult {
	if result == nil || len(result.chunks) == 0 {
		return &purego.QueryResult{}
	}
	
	// Convert columns
	columns := make([]purego.Column, len(result.columns))
	for i, col := range result.columns {
		columns[i] = purego.Column{
			Name:      col.Name,
			Type:      uint32(convertLogicalTypeToPurego(col.Type)),
			Precision: col.Precision,
			Scale:     col.Scale,
		}
	}
	
	// Create a result wrapper that can iterate through chunks
	return &purego.QueryResult{
		// This would need proper implementation
		// For now, return empty result
	}
}

// convertLogicalTypeToPurego converts storage.LogicalType to purego type
func convertLogicalTypeToPurego(lt storage.LogicalType) int {
	switch lt.ID {
	case storage.TypeBoolean:
		return purego.TypeBoolean
	case storage.TypeTinyInt:
		return purego.TypeTinyint
	case storage.TypeSmallInt:
		return purego.TypeSmallint
	case storage.TypeInteger:
		return purego.TypeInteger
	case storage.TypeBigInt:
		return purego.TypeBigint
	case storage.TypeFloat:
		return purego.TypeFloat
	case storage.TypeDouble:
		return purego.TypeDouble
	case storage.TypeVarchar:
		return purego.TypeVarchar
	case storage.TypeDecimal:
		return purego.TypeDecimal
	case storage.TypeDate:
		return purego.TypeDate
	case storage.TypeTimestamp:
		return purego.TypeTimestamp
	default:
		return purego.TypeInvalid
	}
}

// Additional methods needed for driver.Rows implementation
type PureGoRows struct {
	result      *QueryResult
	currentRow  int
	chunkIndex  int
	currentChunk *storage.DataChunk
}

// Columns returns the column names
func (r *PureGoRows) Columns() []string {
	names := make([]string, len(r.result.columns))
	for i, col := range r.result.columns {
		names[i] = col.Name
	}
	return names
}

// Close closes the rows iterator
func (r *PureGoRows) Close() error {
	return r.result.Close()
}

// Next advances to the next row
func (r *PureGoRows) Next(dest []driver.Value) error {
	// Check if we need to advance to next chunk
	if r.currentChunk == nil || r.currentRow >= r.currentChunk.Size() {
		if !r.result.Next() {
			return io.EOF
		}
		r.currentChunk = r.result.GetChunk()
		r.currentRow = 0
		r.chunkIndex++
	}
	
	// Read values from current row
	for i := range dest {
		val, err := r.currentChunk.GetValue(i, r.currentRow)
		if err != nil {
			return err
		}
		dest[i] = val
	}
	
	r.currentRow++
	return nil
}