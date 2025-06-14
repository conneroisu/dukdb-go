package test

import (
	"database/sql"
	"testing"

	_ "github.com/connerohnesorge/dukdb-go/driver"
)

func TestUpdateTransaction(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:test_update_tx")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE counter (value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial data
	_, err = db.Exec("INSERT INTO counter VALUES (0)")
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Test rollback
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec("UPDATE counter SET value = value + 1")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to update: %v", err)
	}

	// Rollback
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Check value didn't change
	var value int
	err = db.QueryRow("SELECT value FROM counter").Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query value after rollback: %v", err)
	}
	if value != 0 {
		t.Errorf("Expected value 0 after rollback, got %d", value)
	}

	// Test commit
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	_, err = tx.Exec("UPDATE counter SET value = value + 1")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to update in second transaction: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Check value changed
	err = db.QueryRow("SELECT value FROM counter").Scan(&value)
	if err != nil {
		t.Fatalf("Failed to query value after commit: %v", err)
	}
	if value != 1 {
		t.Errorf("Expected value 1 after commit, got %d", value)
	}
}