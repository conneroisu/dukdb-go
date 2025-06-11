package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/connerohnesorge/dukdb-go"
)

func main() {
	db, err := sql.Open("dukdb-go", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Setup simple test tables
	setupTestTables(db)

	// Test simple join first
	fmt.Println("=== Testing simple 2-table join ===")
	testSimpleJoin(db)

	// Test multi-table join
	fmt.Println("\n=== Testing multi-table join ===")
	testMultiTableJoin(db)
}

func setupTestTables(db *sql.DB) {
	// Create customers table
	_, err := db.Exec(`
		CREATE TABLE customers (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			region VARCHAR(50)
		)
	`)
	if err != nil {
		log.Fatal("Create customers:", err)
	}

	// Create orders table  
	_, err = db.Exec(`
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			amount DOUBLE
		)
	`)
	if err != nil {
		log.Fatal("Create orders:", err)
	}

	// Create products table
	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			category VARCHAR(50)
		)
	`)
	if err != nil {
		log.Fatal("Create products:", err)
	}

	// Create order_items table
	_, err = db.Exec(`
		CREATE TABLE order_items (
			id INTEGER PRIMARY KEY,
			order_id INTEGER,
			product_id INTEGER,
			quantity INTEGER,
			unit_price DOUBLE
		)
	`)
	if err != nil {
		log.Fatal("Create order_items:", err)
	}

	// Insert test data
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	// Insert customers
	for i := 0; i < 10; i++ {
		_, err := tx.Exec(
			"INSERT INTO customers (id, name, region) VALUES (?, ?, ?)",
			i, fmt.Sprintf("Customer_%d", i), "North",
		)
		if err != nil {
			log.Fatal("Insert customer:", err)
		}
	}

	// Insert orders
	for i := 0; i < 20; i++ {
		customerId := i % 10
		amount := float64(i) * 10.5
		
		_, err := tx.Exec(
			"INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
			i, customerId, amount,
		)
		if err != nil {
			log.Fatal("Insert order:", err)
		}
	}

	// Insert products
	for i := 0; i < 5; i++ {
		_, err := tx.Exec(
			"INSERT INTO products (id, name, category) VALUES (?, ?, ?)",
			i, fmt.Sprintf("Product_%d", i), "Electronics",
		)
		if err != nil {
			log.Fatal("Insert product:", err)
		}
	}

	// Insert order_items
	for i := 0; i < 40; i++ {
		orderId := i % 20
		productId := i % 5
		quantity := (i % 3) + 1
		unitPrice := float64(i%10) * 5.99 + 9.99
		
		_, err := tx.Exec(
			"INSERT INTO order_items (id, order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?, ?)",
			i, orderId, productId, quantity, unitPrice,
		)
		if err != nil {
			log.Fatal("Insert order_item:", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal("Commit:", err)
	}
}

func testSimpleJoin(db *sql.DB) {
	start := time.Now()
	rows, err := db.Query(`
		SELECT c.name as customer_name, c.region
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		LIMIT 5
	`)
	if err != nil {
		log.Fatal("Simple join query:", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Get columns:", err)
	}
	fmt.Printf("Columns: %v\n", columns)

	count := 0
	for rows.Next() {
		var customerName, region string
		err := rows.Scan(&customerName, &region)
		if err != nil {
			log.Fatal("Scan simple join:", err)
		}
		fmt.Printf("Row %d: %s, %s\n", count, customerName, region)
		count++
	}
	fmt.Printf("Simple join took: %v, returned %d rows\n", time.Since(start), count)
}

func testMultiTableJoin(db *sql.DB) {
	start := time.Now()
	rows, err := db.Query(`
		SELECT 
			c.name as customer_name,
			p.name as product_name,
			oi.quantity,
			oi.unit_price,
			oi.quantity * oi.unit_price as line_total
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		JOIN order_items oi ON o.id = oi.order_id
		JOIN products p ON oi.product_id = p.id
		WHERE c.region = ? AND p.category = ?
		LIMIT 10
	`, "North", "Electronics")
	if err != nil {
		log.Fatal("Multi-table join query:", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Fatal("Get columns:", err)
	}
	fmt.Printf("Columns: %v\n", columns)

	count := 0
	for rows.Next() {
		var customerName, productName string
		var quantity int
		var unitPrice, lineTotal float64
		
		err := rows.Scan(&customerName, &productName, &quantity, &unitPrice, &lineTotal)
		if err != nil {
			log.Fatal("Scan multi-table join:", err)
		}
		fmt.Printf("Row %d: %s, %s, %d, %.2f, %.2f\n", count, customerName, productName, quantity, unitPrice, lineTotal)
		count++
	}
	fmt.Printf("Multi-table join took: %v, returned %d rows\n", time.Since(start), count)
}