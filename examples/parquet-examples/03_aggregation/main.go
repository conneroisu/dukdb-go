// Example 3: Parquet file aggregation and analytics
// This example demonstrates aggregation queries on parquet data

package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/connerohnesorge/dukdb-go"
	"github.com/parquet-go/parquet-go"
)

// Transaction represents a financial transaction
type Transaction struct {
	ID         int32   `parquet:"id"`
	CustomerID int32   `parquet:"customer_id"`
	Type       string  `parquet:"type"`
	Amount     float64 `parquet:"amount"`
	Date       int32   `parquet:"date,date"`
	Status     string  `parquet:"status"`
}

func main() {
	// Clean up any existing files
	os.Remove("transactions.parquet")
	os.Remove("monthly_summary.parquet")

	// Create sample transaction data
	if err := createTransactionData(); err != nil {
		log.Fatal("Failed to create transaction data:", err)
	}
	defer os.Remove("transactions.parquet")

	// Open database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Create table and load data
	if err := setupDatabase(db); err != nil {
		log.Fatal("Failed to setup database:", err)
	}

	// Run aggregation queries
	fmt.Println("=== Transaction Analytics ===")

	// Query 1: Total amounts by type
	fmt.Println("1. Total amounts by transaction type:")
	if err := analyzeByType(db); err != nil {
		log.Fatal("Query 1 failed:", err)
	}

	// Query 2: Customer transaction summary
	fmt.Println("\n2. Top 5 customers by transaction volume:")
	if err := topCustomers(db); err != nil {
		log.Fatal("Query 2 failed:", err)
	}

	// Query 3: Status breakdown
	fmt.Println("\n3. Transaction status breakdown:")
	if err := statusBreakdown(db); err != nil {
		log.Fatal("Query 3 failed:", err)
	}

	fmt.Println("\nExample completed successfully!")
}

func createTransactionData() error {
	baseDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	
	transactions := []Transaction{
		{1, 101, "deposit", 1000.00, int32(baseDate.Unix() / 86400), "completed"},
		{2, 101, "withdrawal", 200.00, int32(baseDate.AddDate(0, 0, 2).Unix() / 86400), "completed"},
		{3, 102, "deposit", 1500.00, int32(baseDate.AddDate(0, 0, 3).Unix() / 86400), "completed"},
		{4, 103, "transfer", 500.00, int32(baseDate.AddDate(0, 0, 5).Unix() / 86400), "completed"},
		{5, 101, "transfer", 300.00, int32(baseDate.AddDate(0, 0, 7).Unix() / 86400), "completed"},
		{6, 104, "deposit", 2000.00, int32(baseDate.AddDate(0, 0, 10).Unix() / 86400), "completed"},
		{7, 102, "withdrawal", 100.00, int32(baseDate.AddDate(0, 0, 12).Unix() / 86400), "completed"},
		{8, 103, "deposit", 750.00, int32(baseDate.AddDate(0, 0, 15).Unix() / 86400), "completed"},
		{9, 105, "transfer", 1200.00, int32(baseDate.AddDate(0, 0, 18).Unix() / 86400), "pending"},
		{10, 101, "withdrawal", 500.00, int32(baseDate.AddDate(0, 0, 20).Unix() / 86400), "completed"},
		{11, 102, "transfer", 800.00, int32(baseDate.AddDate(0, 0, 22).Unix() / 86400), "failed"},
		{12, 104, "withdrawal", 300.00, int32(baseDate.AddDate(0, 0, 25).Unix() / 86400), "completed"},
		{13, 103, "deposit", 1000.00, int32(baseDate.AddDate(0, 0, 27).Unix() / 86400), "completed"},
		{14, 105, "deposit", 500.00, int32(baseDate.AddDate(0, 0, 28).Unix() / 86400), "completed"},
		{15, 101, "transfer", 600.00, int32(baseDate.AddDate(0, 1, 2).Unix() / 86400), "completed"},
		{16, 102, "deposit", 900.00, int32(baseDate.AddDate(0, 1, 5).Unix() / 86400), "completed"},
		{17, 104, "transfer", 400.00, int32(baseDate.AddDate(0, 1, 8).Unix() / 86400), "completed"},
		{18, 103, "withdrawal", 200.00, int32(baseDate.AddDate(0, 1, 10).Unix() / 86400), "completed"},
		{19, 105, "deposit", 1500.00, int32(baseDate.AddDate(0, 1, 12).Unix() / 86400), "completed"},
		{20, 101, "deposit", 800.00, int32(baseDate.AddDate(0, 1, 15).Unix() / 86400), "completed"},
	}

	file, err := os.Create("transactions.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := parquet.NewGenericWriter[Transaction](file)
	if _, err := writer.Write(transactions); err != nil {
		return err
	}

	return writer.Close()
}

func setupDatabase(db *sql.DB) error {
	// Create table
	_, err := db.Exec(`
		CREATE TABLE transactions (
			id INTEGER,
			customer_id INTEGER,
			type VARCHAR,
			amount DOUBLE,
			date DATE,
			status VARCHAR
		)
	`)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Load data from parquet
	return loadTransactionData(db)
}

func loadTransactionData(db *sql.DB) error {
	file, err := os.Open("transactions.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[Transaction](file)
	defer reader.Close()

	transactions := make([]Transaction, 30)
	for {
		n, err := reader.Read(transactions)
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
			t := transactions[i]
			sql := fmt.Sprintf("INSERT INTO transactions VALUES (%d, %d, '%s', %f, %d, '%s')",
				t.ID, t.CustomerID, t.Type, t.Amount, t.Date, t.Status)
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

func analyzeByType(db *sql.DB) error {
	// Note: Our parser doesn't support GROUP BY yet, so we'll simulate it
	types := []string{"deposit", "withdrawal", "transfer"}
	
	for _, txType := range types {
		rows, err := db.Query(fmt.Sprintf("SELECT amount FROM transactions WHERE type = '%s'", txType))
		if err != nil {
			return err
		}
		
		var total float64
		var count int
		for rows.Next() {
			var amount float64
			if err := rows.Scan(&amount); err != nil {
				rows.Close()
				return err
			}
			total += amount
			count++
		}
		rows.Close()
		
		fmt.Printf("  %-12s: $%10.2f (count: %d)\n", txType, total, count)
	}
	
	return nil
}

func topCustomers(db *sql.DB) error {
	// Get all completed transactions
	rows, err := db.Query("SELECT customer_id, amount FROM transactions WHERE status = 'completed'")
	if err != nil {
		return err
	}
	defer rows.Close()
	
	// Aggregate in memory
	customerTotals := make(map[int32]float64)
	for rows.Next() {
		var customerID int32
		var amount float64
		if err := rows.Scan(&customerID, &amount); err != nil {
			return err
		}
		customerTotals[customerID] += amount
	}
	
	// Sort and display top 5
	type customerTotal struct {
		ID    int32
		Total float64
	}
	
	var totals []customerTotal
	for id, total := range customerTotals {
		totals = append(totals, customerTotal{id, total})
	}
	
	// Simple bubble sort for top 5
	for i := 0; i < len(totals); i++ {
		for j := i + 1; j < len(totals); j++ {
			if totals[j].Total > totals[i].Total {
				totals[i], totals[j] = totals[j], totals[i]
			}
		}
	}
	
	for i := 0; i < 5 && i < len(totals); i++ {
		fmt.Printf("  Customer %3d: $%10.2f\n", totals[i].ID, totals[i].Total)
	}
	
	return nil
}

func statusBreakdown(db *sql.DB) error {
	statuses := []string{"completed", "pending", "failed"}
	
	for _, status := range statuses {
		rows, err := db.Query(fmt.Sprintf("SELECT id FROM transactions WHERE status = '%s'", status))
		if err != nil {
			return err
		}
		
		count := 0
		for rows.Next() {
			var id int32
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			count++
		}
		rows.Close()
		
		fmt.Printf("  %-10s: %d transactions\n", status, count)
	}
	
	return nil
}