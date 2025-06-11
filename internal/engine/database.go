package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Database represents a pure-Go DuckDB database instance
type Database struct {
	path         string
	isMemory     bool
	connections  sync.Map // map[uint64]*Connection
	nextConnID   atomic.Uint64
	catalog      *Catalog
	closed       atomic.Bool
	mu           sync.RWMutex
}

// NewDatabase creates a new database instance
func NewDatabase(path string) (*Database, error) {
	db := &Database{
		path:     path,
		isMemory: path == ":memory:" || path == "",
		catalog:  NewCatalog(),
	}
	
	// File-based storage not yet implemented
	_ = db.isMemory // Acknowledge the field is used for future file storage
	
	return db, nil
}

// Connect creates a new connection to the database
func (db *Database) Connect() (*Connection, error) {
	if db.closed.Load() {
		return nil, fmt.Errorf("database is closed")
	}
	
	connID := db.nextConnID.Add(1)
	conn := &Connection{
		id:       connID,
		database: db,
		txnState: TxnStateNone,
	}
	
	db.connections.Store(connID, conn)
	return conn, nil
}

// Close closes the database and all its connections
func (db *Database) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return fmt.Errorf("database already closed")
	}
	
	// Close all connections
	var closeErrors []error
	db.connections.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*Connection); ok {
			if err := conn.Close(); err != nil {
				closeErrors = append(closeErrors, err)
			}
		}
		return true
	})
	
	if len(closeErrors) > 0 {
		return fmt.Errorf("errors closing connections: %v", closeErrors)
	}
	
	return nil
}

// GetCatalog returns the database catalog
func (db *Database) GetCatalog() *Catalog {
	return db.catalog
}