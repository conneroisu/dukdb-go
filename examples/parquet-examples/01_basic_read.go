// Example 1: Basic parquet file reading and querying
// This example demonstrates how to read parquet files and query them using DuckDB-go

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

func main() {
	// First, let's create a sample parquet file
	if err := createSampleParquetFile("sales_data.parquet"); err != nil {
		log.Fatal("Failed to create sample parquet file:", err)
	}
	defer os.Remove("sales_data.parquet")

	// Open DuckDB connection
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Create a table from the parquet file
	// Note: In a real implementation, we'd need to implement COPY FROM or external table support
	// For now, we'll create the table manually and load data
	_, err = db.Exec(`
		CREATE TABLE sales (
			id INTEGER,
			product VARCHAR,
			quantity INTEGER,
			price DOUBLE,
			sale_date DATE
		)
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Load data from parquet file
	if err := loadParquetData(db, "sales_data.parquet"); err != nil {
		log.Fatal("Failed to load parquet data:", err)
	}

	// Query 1: Basic SELECT
	fmt.Println("Query 1: All sales")
	rows, err := db.Query("SELECT * FROM sales")
	if err != nil {
		log.Fatal("Query failed:", err)
	}
	printResults(rows)
	rows.Close()

	// Query 2: Simple query to verify data
	fmt.Println("\nQuery 2: First few sales")
	rows, err = db.Query("SELECT * FROM sales LIMIT 3")
	if err != nil {
		log.Fatal("Limited query failed:", err)
	}
	printResults(rows)
	rows.Close()

	// Query 3: Filtering
	fmt.Println("\nQuery 3: High-value sales (price > 100)")
	rows, err = db.Query("SELECT * FROM sales WHERE price > 100")
	if err != nil {
		log.Fatal("Filter query failed:", err)
	}
	printResults(rows)
	rows.Close()
}

// SalesRecord represents a sales record
type SalesRecord struct {
	ID       int32     `parquet:"id"`
	Product  string    `parquet:"product"`
	Quantity int32     `parquet:"quantity"`
	Price    float64   `parquet:"price"`
	SaleDate int32     `parquet:"sale_date,date"`
}

func createSampleParquetFile(filename string) error {
	// Create sample data
	records := []SalesRecord{
		{1, "Laptop", 2, 999.99, int32(time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{2, "Mouse", 5, 29.99, int32(time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{3, "Keyboard", 3, 79.99, int32(time.Date(2024, 1, 16, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{4, "Monitor", 1, 299.99, int32(time.Date(2024, 1, 17, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{5, "Laptop", 1, 1299.99, int32(time.Date(2024, 1, 18, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{6, "Mouse", 10, 19.99, int32(time.Date(2024, 1, 18, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{7, "Headphones", 4, 149.99, int32(time.Date(2024, 1, 19, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
		{8, "Webcam", 2, 89.99, int32(time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC).Unix() / 86400)},
	}

	// Create parquet file
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := parquet.NewGenericWriter[SalesRecord](file)
	_, err = writer.Write(records)
	if err != nil {
		return err
	}

	return writer.Close()
}

func loadParquetData(db *sql.DB, filename string) error {
	// Open parquet file
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	// Get file info
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}
	fmt.Printf("Parquet file size: %d bytes\n", info.Size())

	// Read parquet file
	reader := parquet.NewGenericReader[SalesRecord](file)
	defer reader.Close()

	// For now, insert data directly without prepared statements
	// since our parser doesn't handle placeholders yet
	records := make([]SalesRecord, 100)
	totalRecords := 0
	
	for {
		n, err := reader.Read(records)
		fmt.Printf("Read attempt: n=%d, err=%v\n", n, err)
		
		if err != nil {
			// parquet-go returns io.EOF
			if err.Error() == "EOF" {
				// Process any remaining records
				if n > 0 {
					goto ProcessRecords
				}
				break
			}
			return fmt.Errorf("read records: %w", err)
		}
		if n == 0 {
			break
		}
		
	ProcessRecords:

		// Insert records using direct SQL
		for i := 0; i < n; i++ {
			r := records[i]
			sql := fmt.Sprintf("INSERT INTO sales (id, product, quantity, price, sale_date) VALUES (%d, '%s', %d, %f, %d)",
				r.ID, r.Product, r.Quantity, r.Price, r.SaleDate)
			_, err = db.Exec(sql)
			if err != nil {
				return fmt.Errorf("insert record %d: %w", i, err)
			}
			totalRecords++
		}
	}
	
	fmt.Printf("Loaded %d records from parquet file\n", totalRecords)
	return nil
}

func printResults(rows *sql.Rows) {
	cols, _ := rows.Columns()
	fmt.Printf("%-5s %-15s %-10s %-10s %-12s\n", cols[0], cols[1], cols[2], cols[3], cols[4])
	fmt.Println(string(make([]byte, 60)))

	for rows.Next() {
		var id, quantity, saleDate int32
		var product string
		var price float64
		
		err := rows.Scan(&id, &product, &quantity, &price, &saleDate)
		if err != nil {
			log.Fatal("Scan error:", err)
		}
		
		fmt.Printf("%-5d %-15s %-10d %-10.2f %-12d\n", id, product, quantity, price, saleDate)
	}
}

func printAggregateResults(rows *sql.Rows) {
	fmt.Printf("%-15s %-15s %-15s\n", "Product", "Total Quantity", "Total Revenue")
	fmt.Println(string(make([]byte, 50)))

	for rows.Next() {
		var product string
		var totalQuantity sql.NullInt64
		var totalRevenue sql.NullFloat64
		
		err := rows.Scan(&product, &totalQuantity, &totalRevenue)
		if err != nil {
			log.Fatal("Scan error:", err)
		}
		
		fmt.Printf("%-15s %-15d %-15.2f\n", product, totalQuantity.Int64, totalRevenue.Float64)
	}
}