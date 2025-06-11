package main

import (
	"database/sql"
	"fmt"
	_ "github.com/connerohnesorge/dukdb-go"
)

func main() {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(`
		CREATE TABLE customers (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			region VARCHAR(50)
		)
	`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			amount DOUBLE
		)
	`)
	if err != nil {
		panic(err)
	}

	// Insert test data
	_, err = db.Exec("INSERT INTO customers (id, name, region) VALUES (1, 'Alice', 'North'), (2, 'Bob', 'South')")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0)")
	if err != nil {
		panic(err)
	}

	// Test simple join
	fmt.Println("Testing simple join...")
	rows, err := db.Query(`
		SELECT c.id, c.name, o.id, o.amount
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
	`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid, oid int
		var name string
		var amount float64
		err := rows.Scan(&cid, &name, &oid, &amount)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Customer %d (%s) - Order %d: $%.2f\n", cid, name, oid, amount)
	}

	// Test join with WHERE clause
	fmt.Println("\nTesting join with WHERE clause...")
	rows2, err := db.Query(`
		SELECT c.name, o.amount
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		WHERE c.region = ?
	`, "North")
	if err != nil {
		panic(err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var name string
		var amount float64
		err := rows2.Scan(&name, &amount)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s: $%.2f\n", name, amount)
	}

	fmt.Println("All tests passed!")
}