package test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/connerohnesorge/dukdb-go"
)

func TestBasicConnection(t *testing.T) {
	// Test in-memory database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test ping
	err = db.Ping()
	if err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}
}

func TestCreateTable(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
}

func TestInsertAndQuery(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`
		CREATE TABLE test (
			id INTEGER,
			value VARCHAR
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec("INSERT INTO test VALUES (1, 'hello'), (2, 'world')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Query data
	rows, err := db.Query("SELECT id, value FROM test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	// Check results
	expected := []struct {
		id    int
		value string
	}{
		{1, "hello"},
		{2, "world"},
	}

	i := 0
	for rows.Next() {
		var id int
		var value string
		err := rows.Scan(&id, &value)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("Too many rows returned")
		}

		if id != expected[i].id || value != expected[i].value {
			t.Errorf("Row %d: expected (%d, %s), got (%d, %s)",
				i, expected[i].id, expected[i].value, id, value)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), i)
	}
}

func TestTransaction(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE counter (value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert initial value
	_, err = db.Exec("INSERT INTO counter VALUES (0)")
	if err != nil {
		t.Fatalf("Failed to insert initial value: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Update value in transaction
	_, err = tx.Exec("UPDATE counter SET value = value + 1")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to update value: %v", err)
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
		t.Fatalf("Failed to query value: %v", err)
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
		t.Fatalf("Failed to update value in second transaction: %v", err)
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

func TestColumnTypes(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with various types
	_, err = db.Exec(`
		CREATE TABLE types_test (
			col_bool BOOLEAN,
			col_tinyint TINYINT,
			col_smallint SMALLINT,
			col_int INTEGER,
			col_bigint BIGINT,
			col_float FLOAT,
			col_double DOUBLE,
			col_varchar VARCHAR,
			col_date DATE,
			col_timestamp TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Query column types
	rows, err := db.Query("SELECT * FROM types_test WHERE 1=0")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	expectedCols := []string{
		"col_bool", "col_tinyint", "col_smallint", "col_int", "col_bigint",
		"col_float", "col_double", "col_varchar", "col_date", "col_timestamp",
	}

	if len(cols) != len(expectedCols) {
		t.Fatalf("Expected %d columns, got %d", len(expectedCols), len(cols))
	}

	for i, col := range cols {
		if col != expectedCols[i] {
			t.Errorf("Column %d: expected %s, got %s", i, expectedCols[i], col)
		}
	}
}

// BenchmarkSimpleQuery benchmarks a simple query
func BenchmarkSimpleQuery(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create and populate table
	db.Exec("CREATE TABLE bench (id INTEGER, value INTEGER)")
	for i := 0; i < 1000; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO bench VALUES (%d, %d)", i, i*2))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT SUM(value) FROM bench")
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}
