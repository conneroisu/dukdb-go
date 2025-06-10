package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
)

// Person represents a simple record structure for demonstration
type Person struct {
	ID        int64     `parquet:"id"`
	Name      string    `parquet:"name"`
	Email     string    `parquet:"email"`
	Age       int32     `parquet:"age"`
	Salary    float64   `parquet:"salary"`
	IsActive  bool      `parquet:"is_active"`
	CreatedAt time.Time `parquet:"created_at"`
}

// SalesRecord represents a more complex record with nested data
type SalesRecord struct {
	OrderID     int64     `parquet:"order_id"`
	CustomerID  int64     `parquet:"customer_id"`
	ProductName string    `parquet:"product_name"`
	Quantity    int32     `parquet:"quantity"`
	UnitPrice   float64   `parquet:"unit_price"`
	TotalAmount float64   `parquet:"total_amount"`
	OrderDate   time.Time `parquet:"order_date"`
	Tags        []string  `parquet:"tags,list"`
	// Simplified metadata as separate fields instead of map
	Warranty string `parquet:"warranty"`
	Color    string `parquet:"color"`
	Brand    string `parquet:"brand"`
}

func main() {
	fmt.Println("=== Parquet-Go Example ===")

	// Example 1: Basic read/write operations
	if err := basicExample(); err != nil {
		log.Fatalf("Basic example failed: %v", err)
	}

	// Example 2: Advanced features with complex data types
	if err := advancedExample(); err != nil {
		log.Fatalf("Advanced example failed: %v", err)
	}

	// Example 3: Reading and querying data
	if err := queryExample(); err != nil {
		log.Fatalf("Query example failed: %v", err)
	}

	fmt.Println("✅ All examples completed successfully!")
}

func basicExample() error {
	fmt.Println("\n--- Basic Example: Writing and Reading Simple Records ---")

	filename := "people.parquet"
	defer os.Remove(filename) // Clean up

	// Create sample data
	people := []Person{
		{ID: 1, Name: "Alice Johnson", Email: "alice@example.com", Age: 30, Salary: 75000.0, IsActive: true, CreatedAt: time.Now()},
		{ID: 2, Name: "Bob Smith", Email: "bob@example.com", Age: 25, Salary: 65000.0, IsActive: true, CreatedAt: time.Now().Add(-24 * time.Hour)},
		{ID: 3, Name: "Carol Davis", Email: "carol@example.com", Age: 35, Salary: 85000.0, IsActive: false, CreatedAt: time.Now().Add(-48 * time.Hour)},
		{ID: 4, Name: "David Wilson", Email: "david@example.com", Age: 28, Salary: 70000.0, IsActive: true, CreatedAt: time.Now().Add(-72 * time.Hour)},
		{ID: 5, Name: "Eve Brown", Email: "eve@example.com", Age: 32, Salary: 80000.0, IsActive: true, CreatedAt: time.Now().Add(-96 * time.Hour)},
	}

	// Write data to parquet file
	if err := writeParquetFile(filename, people); err != nil {
		return fmt.Errorf("failed to write parquet file: %w", err)
	}
	fmt.Printf("✅ Written %d records to %s\n", len(people), filename)

	// Read data back from parquet file
	readPeople, err := readParquetFile[Person](filename)
	if err != nil {
		return fmt.Errorf("failed to read parquet file: %w", err)
	}
	fmt.Printf("✅ Read %d records from %s\n", len(readPeople), filename)

	// Display some results
	fmt.Println("Sample records:")
	for i, person := range readPeople {
		if i >= 3 {
			break
		}
		fmt.Printf("  %d: %s (Age: %d, Salary: $%.2f, Active: %v)\n",
			person.ID, person.Name, person.Age, person.Salary, person.IsActive)
	}

	return nil
}

func advancedExample() error {
	fmt.Println("\n--- Advanced Example: Complex Data Types and Compression ---")

	filename := "sales.parquet"
	defer os.Remove(filename) // Clean up

	// Create sample sales data with complex types
	sales := []SalesRecord{
		{
			OrderID:     1001,
			CustomerID:  501,
			ProductName: "Laptop Pro",
			Quantity:    2,
			UnitPrice:   1299.99,
			TotalAmount: 2599.98,
			OrderDate:   time.Now(),
			Tags:        []string{"electronics", "computers", "premium"},
			Warranty:    "2-years",
			Color:       "silver",
			Brand:       "TechCorp",
		},
		{
			OrderID:     1002,
			CustomerID:  502,
			ProductName: "Wireless Mouse",
			Quantity:    3,
			UnitPrice:   29.99,
			TotalAmount: 89.97,
			OrderDate:   time.Now().Add(-time.Hour),
			Tags:        []string{"electronics", "accessories"},
			Warranty:    "1-year",
			Color:       "black",
			Brand:       "Logitech",
		},
		{
			OrderID:     1003,
			CustomerID:  503,
			ProductName: "Office Chair",
			Quantity:    1,
			UnitPrice:   399.99,
			TotalAmount: 399.99,
			OrderDate:   time.Now().Add(-2 * time.Hour),
			Tags:        []string{"furniture", "office", "ergonomic"},
			Warranty:    "5-years",
			Color:       "black",
			Brand:       "ErgoTech",
		},
	}

	// Write with Snappy compression
	if err := writeCompressedParquetFile(filename, sales); err != nil {
		return fmt.Errorf("failed to write compressed parquet file: %w", err)
	}
	fmt.Printf("✅ Written %d sales records to %s (with Snappy compression)\n", len(sales), filename)

	// Read back and verify
	readSales, err := readParquetFile[SalesRecord](filename)
	if err != nil {
		return fmt.Errorf("failed to read compressed parquet file: %w", err)
	}
	fmt.Printf("✅ Read %d sales records from compressed file\n", len(readSales))

	// Display complex data
	fmt.Println("Sample sales records:")
	for i, sale := range readSales {
		if i >= 2 {
			break
		}
		fmt.Printf("  Order %d: %s (Qty: %d, Total: $%.2f)\n",
			sale.OrderID, sale.ProductName, sale.Quantity, sale.TotalAmount)
		fmt.Printf("    Tags: %v\n", sale.Tags)
		fmt.Printf("    Details: %s %s, %s warranty\n", sale.Color, sale.Brand, sale.Warranty)
	}

	return nil
}

func queryExample() error {
	fmt.Println("\n--- Query Example: Filtering and Statistics ---")

	filename := "people.parquet"
	defer os.Remove(filename)

	// Create larger dataset for querying
	people := make([]Person, 100)
	for i := 0; i < 100; i++ {
		people[i] = Person{
			ID:        int64(i + 1),
			Name:      fmt.Sprintf("Person %d", i+1),
			Email:     fmt.Sprintf("person%d@example.com", i+1),
			Age:       int32(20 + (i % 40)), // Ages 20-59
			Salary:    float64(40000 + (i * 1000)),
			IsActive:  i%3 != 0, // 2/3 active
			CreatedAt: time.Now().Add(-time.Duration(i) * time.Hour),
		}
	}

	if err := writeParquetFile(filename, people); err != nil {
		return fmt.Errorf("failed to write query example data: %w", err)
	}

	// Read and filter data
	allPeople, err := readParquetFile[Person](filename)
	if err != nil {
		return fmt.Errorf("failed to read query example data: %w", err)
	}

	// Query 1: Filter by age and salary
	highEarners := filterPeople(allPeople, func(p Person) bool {
		return p.Age >= 30 && p.Salary >= 70000 && p.IsActive
	})
	fmt.Printf("✅ High earners (Age >= 30, Salary >= $70k, Active): %d records\n", len(highEarners))

	// Query 2: Calculate statistics
	stats := calculateStats(allPeople)
	fmt.Printf("✅ Dataset statistics:\n")
	fmt.Printf("    Total records: %d\n", stats.Count)
	fmt.Printf("    Average age: %.1f\n", stats.AvgAge)
	fmt.Printf("    Average salary: $%.2f\n", stats.AvgSalary)
	fmt.Printf("    Active employees: %d (%.1f%%)\n", stats.ActiveCount, float64(stats.ActiveCount)/float64(stats.Count)*100)

	return nil
}

// Generic function to write any struct slice to parquet
func writeParquetFile[T any](filename string, data []T) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := parquet.NewWriter(file)
	defer writer.Close()

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// Write parquet file with compression
func writeCompressedParquetFile[T any](filename string, data []T) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Configure writer with Snappy compression
	writer := parquet.NewWriter(file, parquet.Compression(&snappy.Codec{}))
	defer writer.Close()

	for _, record := range data {
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

// Generic function to read parquet file into struct slice
func readParquetFile[T any](filename string) ([]T, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := parquet.NewReader(file)
	defer reader.Close()

	var records []T
	for {
		var record T
		if err := reader.Read(&record); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
		records = append(records, record)
	}

	return records, nil
}

// Helper functions for querying
func filterPeople(people []Person, predicate func(Person) bool) []Person {
	var filtered []Person
	for _, person := range people {
		if predicate(person) {
			filtered = append(filtered, person)
		}
	}
	return filtered
}

type Stats struct {
	Count       int
	AvgAge      float64
	AvgSalary   float64
	ActiveCount int
}

func calculateStats(people []Person) Stats {
	if len(people) == 0 {
		return Stats{}
	}

	var totalAge int32
	var totalSalary float64
	var activeCount int

	for _, person := range people {
		totalAge += person.Age
		totalSalary += person.Salary
		if person.IsActive {
			activeCount++
		}
	}

	return Stats{
		Count:       len(people),
		AvgAge:      float64(totalAge) / float64(len(people)),
		AvgSalary:   totalSalary / float64(len(people)),
		ActiveCount: activeCount,
	}
}
