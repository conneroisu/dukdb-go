package driver

import (
	"context"
	"database/sql/driver"
)

// Tx implements the database/sql/driver.Tx interface
type Tx struct {
	conn     *Conn
	finished bool
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if tx.finished {
		return driver.ErrBadConn
	}

	err := tx.conn.engineConn.Execute(context.Background(), "COMMIT")
	tx.finished = true
	return err
}

// Rollback rolls back the transaction
func (tx *Tx) Rollback() error {
	if tx.finished {
		return driver.ErrBadConn
	}

	err := tx.conn.engineConn.Execute(context.Background(), "ROLLBACK")
	tx.finished = true
	return err
}