package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"

	"github.com/connerohnesorge/dukdb-go/internal/purego"
)

func init() {
	sql.Register("duckdb", &Driver{})
}

// Driver implements the database/sql/driver.Driver interface
type Driver struct {
	mu       sync.Mutex
	duckdb   *purego.DuckDB
	initOnce sync.Once
}

// Open returns a new connection to the database
func (d *Driver) Open(name string) (driver.Conn, error) {
	// Initialize DuckDB library once
	var initErr error
	d.initOnce.Do(func() {
		d.duckdb, initErr = purego.New()
	})

	if initErr != nil {
		return nil, fmt.Errorf("failed to initialize DuckDB: %w", initErr)
	}

	// Open database
	db, err := d.duckdb.Open(name)
	if err != nil {
		return nil, err
	}

	// Create connection
	conn, err := d.duckdb.Connect(db)
	if err != nil {
		d.duckdb.CloseDatabase(db)
		return nil, err
	}

	return &Conn{
		duckdb: d.duckdb,
		db:     db,
		conn:   conn,
	}, nil
}

// OpenConnector returns a new connector
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	// Initialize DuckDB library once
	var initErr error
	d.initOnce.Do(func() {
		d.duckdb, initErr = purego.New()
	})

	if initErr != nil {
		return nil, fmt.Errorf("failed to initialize DuckDB: %w", initErr)
	}

	return &Connector{
		driver: d,
		name:   name,
	}, nil
}
