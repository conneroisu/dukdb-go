package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/connerohnesorge/dukdb-go"
)

func main() {
	// Open database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Example 1: Working with Date/Time types
	fmt.Println("=== Date/Time Types Example ===")
	demonstrateDateTimeTypes(db)

	// Example 2: Working with prepared statements and parameters
	fmt.Println("\n=== Prepared Statements Example ===")
	demonstratePreparedStatements(db)

	// Example 3: Working with transactions
	fmt.Println("\n=== Transactions Example ===")
	demonstrateTransactions(db)

	// Example 4: Working with BLOB data
	fmt.Println("\n=== BLOB Data Example ===")
	demonstrateBlobData(db)

	// Example 5: Complex queries with analytics
	fmt.Println("\n=== Analytics Example ===")
	demonstrateAnalytics(db)
}

func demonstrateDateTimeTypes(db *sql.DB) {
	// Create table
	_, err := db.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			event_name VARCHAR,
			event_date DATE,
			event_time TIME,
			created_at TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatal("Failed to create events table:", err)
	}

	// Insert events
	now := time.Now()
	eventDate := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	eventTime := time.Date(1970, 1, 1, 14, 30, 0, 0, time.UTC)

	_, err = db.Exec(`
		INSERT INTO events VALUES 
			(1, 'Conference', ?, ?, ?),
			(2, 'Meeting', ?, ?, ?)
	`, eventDate, eventTime, now, eventDate.AddDate(0, 0, 1), eventTime.Add(2*time.Hour), now.Add(24*time.Hour))
	if err != nil {
		log.Fatal("Failed to insert events:", err)
	}

	// Query events
	rows, err := db.Query("SELECT * FROM events ORDER BY id")
	if err != nil {
		log.Fatal("Failed to query events:", err)
	}
	defer rows.Close()

	fmt.Println("Events:")
	for rows.Next() {
		var id int
		var name string
		var date, timeVal, timestamp time.Time
		err := rows.Scan(&id, &name, &date, &timeVal, &timestamp)
		if err != nil {
			log.Fatal("Failed to scan event:", err)
		}
		fmt.Printf("  %d: %s on %s at %s (created: %s)\n",
			id, name,
			date.Format("2006-01-02"),
			timeVal.Format("15:04:05"),
			timestamp.Format("2006-01-02 15:04:05"))
	}
}

func demonstratePreparedStatements(db *sql.DB) {
	// Create table
	_, err := db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			price DECIMAL(10,2),
			stock INTEGER
		)
	`)
	if err != nil {
		log.Fatal("Failed to create products table:", err)
	}

	// Prepare insert statement
	insertStmt, err := db.Prepare("INSERT INTO products VALUES (?, ?, ?, ?)")
	if err != nil {
		log.Fatal("Failed to prepare insert:", err)
	}
	defer insertStmt.Close()

	// Insert multiple products
	products := []struct {
		id    int
		name  string
		price float64
		stock int
	}{
		{1, "Laptop", 999.99, 10},
		{2, "Mouse", 29.99, 50},
		{3, "Keyboard", 79.99, 30},
		{4, "Monitor", 299.99, 15},
	}

	for _, p := range products {
		_, err = insertStmt.Exec(p.id, p.name, p.price, p.stock)
		if err != nil {
			log.Fatal("Failed to insert product:", err)
		}
	}

	// Prepare query statement
	queryStmt, err := db.Prepare("SELECT name, price FROM products WHERE price < ? ORDER BY price DESC")
	if err != nil {
		log.Fatal("Failed to prepare query:", err)
	}
	defer queryStmt.Close()

	// Query products under $100
	rows, err := queryStmt.Query(100.0)
	if err != nil {
		log.Fatal("Failed to query products:", err)
	}
	defer rows.Close()

	fmt.Println("Products under $100:")
	for rows.Next() {
		var name string
		var price float64
		err := rows.Scan(&name, &price)
		if err != nil {
			log.Fatal("Failed to scan product:", err)
		}
		fmt.Printf("  %s: $%.2f\n", name, price)
	}
}

func demonstrateTransactions(db *sql.DB) {
	// Create accounts table
	_, err := db.Exec(`
		CREATE TABLE accounts (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			balance DECIMAL(10,2)
		)
	`)
	if err != nil {
		log.Fatal("Failed to create accounts table:", err)
	}

	// Insert initial accounts
	_, err = db.Exec(`
		INSERT INTO accounts VALUES 
			(1, 'Alice', 1000.00),
			(2, 'Bob', 500.00)
	`)
	if err != nil {
		log.Fatal("Failed to insert accounts:", err)
	}

	// Perform a transfer in a transaction
	transferAmount := 250.00
	fmt.Printf("Transferring $%.2f from Alice to Bob...\n", transferAmount)

	tx, err := db.Begin()
	if err != nil {
		log.Fatal("Failed to begin transaction:", err)
	}

	// Deduct from Alice
	_, err = tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = 1", transferAmount)
	if err != nil {
		tx.Rollback()
		log.Fatal("Failed to deduct from Alice:", err)
	}

	// Add to Bob
	_, err = tx.Exec("UPDATE accounts SET balance = balance + ? WHERE id = 2", transferAmount)
	if err != nil {
		tx.Rollback()
		log.Fatal("Failed to add to Bob:", err)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit transaction:", err)
	}

	// Check final balances
	rows, err := db.Query("SELECT name, balance FROM accounts ORDER BY id")
	if err != nil {
		log.Fatal("Failed to query balances:", err)
	}
	defer rows.Close()

	fmt.Println("Final balances:")
	for rows.Next() {
		var name string
		var balance float64
		err := rows.Scan(&name, &balance)
		if err != nil {
			log.Fatal("Failed to scan balance:", err)
		}
		fmt.Printf("  %s: $%.2f\n", name, balance)
	}
}

func demonstrateBlobData(db *sql.DB) {
	// Create table with BLOB
	_, err := db.Exec(`
		CREATE TABLE documents (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			content BLOB,
			size INTEGER
		)
	`)
	if err != nil {
		log.Fatal("Failed to create documents table:", err)
	}

	// Insert some binary data
	data1 := []byte("This is a test document with some binary data: \x00\x01\x02\x03")
	data2 := []byte("{\"type\":\"json\",\"data\":[1,2,3,4,5]}")

	_, err = db.Exec("INSERT INTO documents VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
		1, "binary.dat", data1, len(data1),
		2, "data.json", data2, len(data2))
	if err != nil {
		log.Fatal("Failed to insert documents:", err)
	}

	// Query and display
	rows, err := db.Query("SELECT name, content, size FROM documents ORDER BY id")
	if err != nil {
		log.Fatal("Failed to query documents:", err)
	}
	defer rows.Close()

	fmt.Println("Documents:")
	for rows.Next() {
		var name string
		var content []byte
		var size int
		err := rows.Scan(&name, &content, &size)
		if err != nil {
			log.Fatal("Failed to scan document:", err)
		}
		fmt.Printf("  %s (%d bytes): %q\n", name, size, content)
	}
}

func demonstrateAnalytics(db *sql.DB) {
	// Create sales data
	_, err := db.Exec(`
		CREATE TABLE sales (
			id INTEGER,
			product VARCHAR,
			category VARCHAR,
			amount DECIMAL(10,2),
			sale_date DATE
		)
	`)
	if err != nil {
		log.Fatal("Failed to create sales table:", err)
	}

	// Insert sample data
	_, err = db.Exec(`
		INSERT INTO sales VALUES 
			(1, 'Laptop', 'Electronics', 999.99, '2024-01-01'),
			(2, 'Mouse', 'Electronics', 29.99, '2024-01-01'),
			(3, 'Desk', 'Furniture', 299.99, '2024-01-02'),
			(4, 'Chair', 'Furniture', 199.99, '2024-01-02'),
			(5, 'Monitor', 'Electronics', 399.99, '2024-01-03'),
			(6, 'Keyboard', 'Electronics', 79.99, '2024-01-03'),
			(7, 'Lamp', 'Furniture', 49.99, '2024-01-03')
	`)
	if err != nil {
		log.Fatal("Failed to insert sales data:", err)
	}

	// Category summary
	fmt.Println("Sales by Category:")
	rows, err := db.Query(`
		SELECT 
			category,
			COUNT(*) as item_count,
			SUM(amount) as total_sales,
			AVG(amount) as avg_price,
			MIN(amount) as min_price,
			MAX(amount) as max_price
		FROM sales
		GROUP BY category
		ORDER BY total_sales DESC
	`)
	if err != nil {
		log.Fatal("Failed to query category summary:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var category string
		var count int
		var total, avg, min, max float64
		err := rows.Scan(&category, &count, &total, &avg, &min, &max)
		if err != nil {
			log.Fatal("Failed to scan summary:", err)
		}
		fmt.Printf("  %s: %d items, $%.2f total (avg: $%.2f, range: $%.2f-$%.2f)\n",
			category, count, total, avg, min, max)
	}

	// Daily sales
	fmt.Println("\nDaily Sales:")
	rows2, err := db.Query(`
		SELECT 
			sale_date,
			COUNT(*) as transactions,
			SUM(amount) as daily_total
		FROM sales
		GROUP BY sale_date
		ORDER BY sale_date
	`)
	if err != nil {
		log.Fatal("Failed to query daily sales:", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var date time.Time
		var count int
		var total float64
		err := rows2.Scan(&date, &count, &total)
		if err != nil {
			log.Fatal("Failed to scan daily sales:", err)
		}
		fmt.Printf("  %s: %d sales, $%.2f\n",
			date.Format("2006-01-02"), count, total)
	}
}