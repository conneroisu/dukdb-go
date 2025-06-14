package test

import (
	"database/sql"
	"testing"
	_ "github.com/connerohnesorge/dukdb-go"
)

func TestSimpleJoinProjection(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(`
		CREATE TABLE customers (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			amount DOUBLE
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO customers (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 100.0), (2, 1, 200.0)")
	if err != nil {
		t.Fatal(err)
	}

	// Test projection with qualified names
	rows, err := db.Query(`
		SELECT c.name, o.amount
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		WHERE c.id = 1
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var amount float64
		err := rows.Scan(&name, &amount)
		if err != nil {
			t.Fatalf("Scan error: %v", err)
		}
		t.Logf("Customer: %s, Amount: %.2f", name, amount)
	}
}