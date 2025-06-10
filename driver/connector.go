package driver

import (
	"context"
	"database/sql/driver"
)

// Connector implements the database/sql/driver.Connector interface
type Connector struct {
	driver *Driver
	name   string
}

// Connect returns a new connection
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.driver.Open(c.name)
}

// Driver returns the underlying driver
func (c *Connector) Driver() driver.Driver {
	return c.driver
}
