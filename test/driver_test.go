package test

import (
	"database/sql"
	"testing"

	_ "github.com/connerohnesorge/dukdb-go"
)

func TestPureGoDriver(t *testing.T) {
	// Test that we can open a database using the pure Go implementation
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test basic ping
	err = db.Ping()
	if err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	// Create a table
	_, err = db.Exec("CREATE TABLE test (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec("INSERT INTO test VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Query data
	var id int
	var name string
	err = db.QueryRow("SELECT id, name FROM test").Scan(&id, &name)
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}

	if id != 1 || name != "Alice" {
		t.Errorf("Unexpected result: id=%d, name=%s", id, name)
	}

	t.Log("Pure Go driver test passed!")
}