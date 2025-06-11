package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/connerohnesorge/dukdb-go/internal/engine"
)

func init() {
	sql.Register("duckdb", &Driver{})
}

// Driver implements the database/sql/driver.Driver interface
type Driver struct {
	mu       sync.Mutex
	databases map[string]*engine.Database
}

// Open returns a new connection to the database
func (d *Driver) Open(name string) (driver.Conn, error) {
	d.mu.Lock()
	if d.databases == nil {
		d.databases = make(map[string]*engine.Database)
	}
	d.mu.Unlock()

	// Get or create database
	d.mu.Lock()
	db, exists := d.databases[name]
	if !exists {
		var err error
		db, err = engine.NewDatabase(name)
		if err != nil {
			d.mu.Unlock()
			return nil, fmt.Errorf("failed to open database: %w", err)
		}
		d.databases[name] = db
	}
	d.mu.Unlock()

	// Create connection
	conn, err := db.Connect()
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	return &Conn{
		engineConn: conn,
		db:         db,
	}, nil
}

// OpenConnector returns a new connector
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return &Connector{
		driver: d,
		name:   name,
	}, nil
}
