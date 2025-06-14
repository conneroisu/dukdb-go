package test

import (
	"database/sql"
	"testing"

	_ "github.com/connerohnesorge/dukdb-go/driver"
)

func TestSimpleInsertTransaction(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec("CREATE TABLE test_tx (id INTEGER, value VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test rollback
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	_, err = tx.Exec("INSERT INTO test_tx VALUES (1, 'test')")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert: %v", err)
	}

	// Rollback
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Check no rows exist
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_tx").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 rows after rollback, got %d", count)
	}

	// Test commit
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin second transaction: %v", err)
	}

	_, err = tx.Exec("INSERT INTO test_tx VALUES (2, 'committed')")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert in second transaction: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Check row exists
	err = db.QueryRow("SELECT COUNT(*) FROM test_tx").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows after commit: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 row after commit, got %d", count)
	}
}