// Example 2: Writing query results to parquet files
// This example demonstrates how to query data and write results to parquet files

package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/connerohnesorge/dukdb-go"
	"github.com/parquet-go/parquet-go"
)

// Customer represents a customer record
type Customer struct {
	ID      int32   `parquet:"id"`
	Name    string  `parquet:"name"`
	Email   string  `parquet:"email"`
	Balance float64 `parquet:"balance"`
}

// Order represents an order record
type Order struct {
	OrderID    int32   `parquet:"order_id"`
	CustomerID int32   `parquet:"customer_id"`
	Product    string  `parquet:"product"`
	Amount     float64 `parquet:"amount"`
}

// CustomerSummary for analysis results
type CustomerSummary struct {
	CustomerID   int32   `parquet:"customer_id"`
	CustomerName string  `parquet:"customer_name"`
	TotalOrders  int32   `parquet:"total_orders"`
	TotalAmount  float64 `parquet:"total_amount"`
}

func main() {
	// Clean up any existing files
	os.Remove("customers.parquet")
	os.Remove("orders.parquet")
	os.Remove("customer_summary.parquet")

	// Create sample data files
	if err := createSampleData(); err != nil {
		log.Fatal("Failed to create sample data:", err)
	}

	// Open database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Create tables
	if err := createTables(db); err != nil {
		log.Fatal("Failed to create tables:", err)
	}

	// Load data from parquet files
	if err := loadData(db); err != nil {
		log.Fatal("Failed to load data:", err)
	}

	// Query data and write results to parquet
	fmt.Println("Generating customer summary report...")
	if err := generateCustomerSummary(db); err != nil {
		log.Fatal("Failed to generate summary:", err)
	}

	// Verify the output
	fmt.Println("\nReading generated parquet file:")
	if err := readSummaryFile(); err != nil {
		log.Fatal("Failed to read summary:", err)
	}

	fmt.Println("\nExample completed successfully!")
}

func createSampleData() error {
	// Create customers
	customers := []Customer{
		{1, "Alice Johnson", "alice@email.com", 1500.50},
		{2, "Bob Smith", "bob@email.com", 2300.75},
		{3, "Charlie Brown", "charlie@email.com", 500.00},
		{4, "Diana Prince", "diana@email.com", 3200.25},
		{5, "Eve Wilson", "eve@email.com", 1800.00},
	}

	file, err := os.Create("customers.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := parquet.NewGenericWriter[Customer](file)
	if _, err := writer.Write(customers); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	// Create orders
	orders := []Order{
		{101, 1, "Laptop", 999.99},
		{102, 1, "Mouse", 29.99},
		{103, 2, "Monitor", 299.99},
		{104, 2, "Keyboard", 79.99},
		{105, 2, "Webcam", 89.99},
		{106, 3, "Headphones", 149.99},
		{107, 4, "Tablet", 599.99},
		{108, 4, "Stylus", 49.99},
		{109, 5, "SSD", 199.99},
		{110, 5, "RAM", 149.99},
		{111, 1, "USB Hub", 39.99},
	}

	file2, err := os.Create("orders.parquet")
	if err != nil {
		return err
	}
	defer file2.Close()

	writer2 := parquet.NewGenericWriter[Order](file2)
	if _, err := writer2.Write(orders); err != nil {
		return err
	}
	return writer2.Close()
}

func createTables(db *sql.DB) error {
	// Create customers table
	_, err := db.Exec(`
		CREATE TABLE customers (
			id INTEGER,
			name VARCHAR,
			email VARCHAR,
			balance DOUBLE
		)
	`)
	if err != nil {
		return err
	}

	// Create orders table
	_, err = db.Exec(`
		CREATE TABLE orders (
			order_id INTEGER,
			customer_id INTEGER,
			product VARCHAR,
			amount DOUBLE
		)
	`)
	return err
}

func loadData(db *sql.DB) error {
	// Load customers
	if err := loadCustomers(db); err != nil {
		return fmt.Errorf("loading customers: %w", err)
	}

	// Load orders
	if err := loadOrders(db); err != nil {
		return fmt.Errorf("loading orders: %w", err)
	}

	return nil
}

func loadCustomers(db *sql.DB) error {
	file, err := os.Open("customers.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[Customer](file)
	defer reader.Close()

	customers := make([]Customer, 10)
	for {
		n, err := reader.Read(customers)
		if err != nil && err.Error() == "EOF" {
			if n > 0 {
				goto ProcessRecords
			}
			break
		}
		if n == 0 {
			break
		}

	ProcessRecords:
		for i := 0; i < n; i++ {
			c := customers[i]
			sql := fmt.Sprintf("INSERT INTO customers VALUES (%d, '%s', '%s', %f)",
				c.ID, c.Name, c.Email, c.Balance)
			if _, err := db.Exec(sql); err != nil {
				return err
			}
		}
		
		if err != nil && err.Error() == "EOF" {
			break
		}
	}

	return nil
}

func loadOrders(db *sql.DB) error {
	file, err := os.Open("orders.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[Order](file)
	defer reader.Close()

	orders := make([]Order, 20)
	for {
		n, err := reader.Read(orders)
		if err != nil && err.Error() == "EOF" {
			if n > 0 {
				goto ProcessRecords
			}
			break
		}
		if n == 0 {
			break
		}

	ProcessRecords:
		for i := 0; i < n; i++ {
			o := orders[i]
			sql := fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, '%s', %f)",
				o.OrderID, o.CustomerID, o.Product, o.Amount)
			if _, err := db.Exec(sql); err != nil {
				return err
			}
		}
		
		if err != nil && err.Error() == "EOF" {
			break
		}
	}

	return nil
}

func generateCustomerSummary(db *sql.DB) error {
	// Query to generate customer summary
	// Note: Our parser doesn't support JOINs yet, so we'll do it in two steps
	
	// First, get all customers
	rows, err := db.Query("SELECT id, name FROM customers")
	if err != nil {
		return fmt.Errorf("query customers: %w", err)
	}
	defer rows.Close()

	customers := make(map[int32]string)
	for rows.Next() {
		var id int32
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return err
		}
		customers[id] = name
	}

	// Then get order summaries
	summaries := make([]CustomerSummary, 0)
	
	for customerID, customerName := range customers {
		// Get orders for this customer
		orderRows, err := db.Query(fmt.Sprintf("SELECT order_id, amount FROM orders WHERE customer_id = %d", customerID))
		if err != nil {
			return fmt.Errorf("query orders for customer %d: %w", customerID, err)
		}
		
		var totalOrders int32
		var totalAmount float64
		
		for orderRows.Next() {
			var orderID int32
			var amount float64
			if err := orderRows.Scan(&orderID, &amount); err != nil {
				orderRows.Close()
				return err
			}
			totalOrders++
			totalAmount += amount
		}
		orderRows.Close()
		
		if totalOrders > 0 {
			summaries = append(summaries, CustomerSummary{
				CustomerID:   customerID,
				CustomerName: customerName,
				TotalOrders:  totalOrders,
				TotalAmount:  totalAmount,
			})
		}
	}

	// Write summaries to parquet
	file, err := os.Create("customer_summary.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := parquet.NewGenericWriter[CustomerSummary](file)
	if _, err := writer.Write(summaries); err != nil {
		return err
	}

	return writer.Close()
}

func readSummaryFile() error {
	file, err := os.Open("customer_summary.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[CustomerSummary](file)
	defer reader.Close()

	fmt.Printf("%-12s %-20s %-15s %-15s\n", "Customer ID", "Customer Name", "Total Orders", "Total Amount")
	fmt.Println(string(make([]byte, 65)))

	summaries := make([]CustomerSummary, 10)
	for {
		n, err := reader.Read(summaries)
		if err != nil && err.Error() == "EOF" {
			if n > 0 {
				goto ProcessRecords
			}
			break
		}
		if n == 0 {
			break
		}

	ProcessRecords:
		for i := 0; i < n; i++ {
			s := summaries[i]
			fmt.Printf("%-12d %-20s %-15d $%-14.2f\n", 
				s.CustomerID, s.CustomerName, s.TotalOrders, s.TotalAmount)
		}
		
		if err != nil && err.Error() == "EOF" {
			break
		}
	}

	return nil
}