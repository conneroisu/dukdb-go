package dataload

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
	
	"github.com/connerohnesorge/dukdb-go/benchmarks"
)

// Benchmark CSV loading
func BenchmarkCSVLoad(b *testing.B) {
	// Create test CSV files of different sizes
	sizes := []struct {
		name string
		rows int
	}{
		{"Small_1K", 1000},
		{"Medium_10K", 10000},
		{"Large_100K", 100000},
	}
	
	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			// Create CSV file
			csvFile := createTestCSV(b, size.rows)
			defer os.Remove(csvFile)
			
			runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
				b.ResetTimer()
				
				for i := 0; i < b.N; i++ {
					// Drop table if exists
					db.Exec("DROP TABLE IF EXISTS csv_load_test")
					
					// Load CSV using COPY command
					start := time.Now()
					_, err := db.Exec(fmt.Sprintf(`
						COPY csv_load_test FROM '%s' (AUTO_DETECT TRUE)
					`, csvFile))
					
					if err != nil {
						// Try alternative syntax
						_, err = db.Exec(fmt.Sprintf(`
							CREATE TABLE csv_load_test AS 
							SELECT * FROM read_csv_auto('%s')
						`, csvFile))
						
						if err != nil {
							b.Fatal(err)
						}
					}
					
					duration := time.Since(start)
					
					// Verify row count
					var count int
					err = db.QueryRow("SELECT COUNT(*) FROM csv_load_test").Scan(&count)
					if err != nil {
						b.Fatal(err)
					}
					
					if count != size.rows {
						b.Fatalf("Expected %d rows, got %d", size.rows, count)
					}
					
					b.ReportMetric(float64(duration.Nanoseconds())/float64(size.rows), "ns/row")
				}
			})
		})
	}
}

// Benchmark CSV export
func BenchmarkCSVExport(b *testing.B) {
	rowCounts := []int{1000, 10000, 50000}
	
	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("Rows_%d", rowCount), func(b *testing.B) {
			runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
				// Setup table with data
				setupExportTable(b, db, rowCount)
				
				b.ResetTimer()
				
				for i := 0; i < b.N; i++ {
					// Create temp file for export
					tmpFile, err := ioutil.TempFile("", "export_*.csv")
					if err != nil {
						b.Fatal(err)
					}
					tmpFile.Close()
					defer os.Remove(tmpFile.Name())
					
					// Export to CSV
					start := time.Now()
					_, err = db.Exec(fmt.Sprintf(`
						COPY export_test TO '%s' (FORMAT CSV, HEADER)
					`, tmpFile.Name()))
					
					if err != nil {
						// Try alternative syntax
						_, err = db.Exec(fmt.Sprintf(`
							COPY (SELECT * FROM export_test) TO '%s' (FORMAT CSV, HEADER)
						`, tmpFile.Name()))
						
						if err != nil {
							b.Fatal(err)
						}
					}
					
					duration := time.Since(start)
					
					// Check file size
					info, err := os.Stat(tmpFile.Name())
					if err != nil {
						b.Fatal(err)
					}
					
					b.ReportMetric(float64(duration.Nanoseconds())/float64(rowCount), "ns/row")
					b.ReportMetric(float64(info.Size())/float64(rowCount), "bytes/row")
				}
			})
		})
	}
}

// Benchmark Parquet loading
func BenchmarkParquetLoad(b *testing.B) {
	// Check if there's a parquet file available
	parquetFile := findParquetFile(b)
	if parquetFile == "" {
		b.Skip("No Parquet file found for testing")
		return
	}
	
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			// Drop table if exists
			db.Exec("DROP TABLE IF EXISTS parquet_load_test")
			
			// Load Parquet file
			start := time.Now()
			_, err := db.Exec(fmt.Sprintf(`
				CREATE TABLE parquet_load_test AS 
				SELECT * FROM read_parquet('%s')
			`, parquetFile))
			
			if err != nil {
				b.Fatal(err)
			}
			
			duration := time.Since(start)
			
			// Get row count
			var count int
			err = db.QueryRow("SELECT COUNT(*) FROM parquet_load_test").Scan(&count)
			if err != nil {
				b.Fatal(err)
			}
			
			b.ReportMetric(float64(duration.Nanoseconds())/float64(count), "ns/row")
		}
	})
}

// Benchmark JSON loading
func BenchmarkJSONLoad(b *testing.B) {
	// Create test JSON files
	sizes := []struct {
		name string
		rows int
	}{
		{"Small_1K", 1000},
		{"Medium_10K", 10000},
	}
	
	for _, size := range sizes {
		b.Run(size.name, func(b *testing.B) {
			// Create JSON file
			jsonFile := createTestJSON(b, size.rows)
			defer os.Remove(jsonFile)
			
			runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
				b.ResetTimer()
				
				for i := 0; i < b.N; i++ {
					// Drop table if exists
					db.Exec("DROP TABLE IF EXISTS json_load_test")
					
					// Load JSON
					start := time.Now()
					_, err := db.Exec(fmt.Sprintf(`
						CREATE TABLE json_load_test AS 
						SELECT * FROM read_json_auto('%s')
					`, jsonFile))
					
					if err != nil {
						// JSON loading might not be supported
						if implName == "PureGo" {
							b.Skip("JSON loading not yet implemented in pure-Go version")
							return
						}
						b.Fatal(err)
					}
					
					duration := time.Since(start)
					
					// Verify row count
					var count int
					err = db.QueryRow("SELECT COUNT(*) FROM json_load_test").Scan(&count)
					if err != nil {
						b.Fatal(err)
					}
					
					b.ReportMetric(float64(duration.Nanoseconds())/float64(count), "ns/row")
				}
			})
		})
	}
}

// Benchmark bulk INSERT vs COPY performance
func BenchmarkBulkInsertVsCopy(b *testing.B) {
	rowCount := 10000
	
	b.Run("BulkInsert", func(b *testing.B) {
		runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				// Create table
				db.Exec("DROP TABLE IF EXISTS bulk_insert_test")
				_, err := db.Exec(`
					CREATE TABLE bulk_insert_test (
						id INTEGER,
						name VARCHAR(100),
						value DOUBLE,
						created_at TIMESTAMP
					)
				`)
				if err != nil {
					b.Fatal(err)
				}
				
				// Bulk insert
				start := time.Now()
				tx, err := db.Begin()
				if err != nil {
					b.Fatal(err)
				}
				
				stmt, err := tx.Prepare("INSERT INTO bulk_insert_test VALUES (?, ?, ?, ?)")
				if err != nil {
					b.Fatal(err)
				}
				
				baseTime := time.Now()
				for j := 0; j < rowCount; j++ {
					_, err := stmt.Exec(j, fmt.Sprintf("Item_%d", j), float64(j)*1.5, baseTime)
					if err != nil {
						b.Fatal(err)
					}
				}
				
				stmt.Close()
				err = tx.Commit()
				if err != nil {
					b.Fatal(err)
				}
				
				duration := time.Since(start)
				b.ReportMetric(float64(duration.Nanoseconds())/float64(rowCount), "ns/row")
			}
		})
	})
	
	b.Run("CSVCopy", func(b *testing.B) {
		// Create CSV file
		csvFile := createTestCSV(b, rowCount)
		defer os.Remove(csvFile)
		
		runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
			b.ResetTimer()
			
			for i := 0; i < b.N; i++ {
				// Drop and recreate table
				db.Exec("DROP TABLE IF EXISTS csv_copy_test")
				
				start := time.Now()
				_, err := db.Exec(fmt.Sprintf(`
					CREATE TABLE csv_copy_test AS 
					SELECT * FROM read_csv_auto('%s')
				`, csvFile))
				
				if err != nil {
					b.Fatal(err)
				}
				
				duration := time.Since(start)
				b.ReportMetric(float64(duration.Nanoseconds())/float64(rowCount), "ns/row")
			}
		})
	})
}

// Helper functions

func runBothImplementations(b *testing.B, benchFunc func(*testing.B, *sql.DB, string)) {
	implementations := []struct {
		name     string
		openFunc func(string) (*sql.DB, error)
	}{
		{"PureGo", benchmarks.OpenPureGoDatabase},
		{"CGO", benchmarks.OpenCGODatabase},
	}
	
	for _, impl := range implementations {
		b.Run(impl.name, func(b *testing.B) {
			db, err := impl.openFunc(":memory:")
			if err != nil {
				if impl.name == "CGO" {
					b.Skipf("CGO implementation not available: %v", err)
					return
				}
				b.Fatal(err)
			}
			defer db.Close()
			
			benchFunc(b, db, impl.name)
		})
	}
}

func createTestCSV(b *testing.B, rowCount int) string {
	tmpFile, err := ioutil.TempFile("", "test_*.csv")
	if err != nil {
		b.Fatal(err)
	}
	defer tmpFile.Close()
	
	writer := csv.NewWriter(tmpFile)
	
	// Write header
	header := []string{"id", "name", "value", "created_at", "category"}
	if err := writer.Write(header); err != nil {
		b.Fatal(err)
	}
	
	// Write data
	categories := []string{"A", "B", "C", "D", "E"}
	baseTime := time.Now()
	
	for i := 0; i < rowCount; i++ {
		record := []string{
			fmt.Sprintf("%d", i),
			fmt.Sprintf("Item_%d", i),
			fmt.Sprintf("%.2f", float64(i)*1.5),
			baseTime.Add(time.Duration(i) * time.Second).Format(time.RFC3339),
			categories[i%len(categories)],
		}
		
		if err := writer.Write(record); err != nil {
			b.Fatal(err)
		}
	}
	
	writer.Flush()
	if err := writer.Error(); err != nil {
		b.Fatal(err)
	}
	
	return tmpFile.Name()
}

func createTestJSON(b *testing.B, rowCount int) string {
	tmpFile, err := ioutil.TempFile("", "test_*.json")
	if err != nil {
		b.Fatal(err)
	}
	defer tmpFile.Close()
	
	// Write JSON array
	tmpFile.WriteString("[\n")
	
	categories := []string{"A", "B", "C", "D", "E"}
	baseTime := time.Now()
	
	for i := 0; i < rowCount; i++ {
		if i > 0 {
			tmpFile.WriteString(",\n")
		}
		
		json := fmt.Sprintf(`  {"id": %d, "name": "Item_%d", "value": %.2f, "created_at": "%s", "category": "%s"}`,
			i, i, float64(i)*1.5, baseTime.Add(time.Duration(i)*time.Second).Format(time.RFC3339), categories[i%len(categories)])
		
		tmpFile.WriteString(json)
	}
	
	tmpFile.WriteString("\n]\n")
	
	return tmpFile.Name()
}

func setupExportTable(b *testing.B, db *sql.DB, rowCount int) {
	// Drop table if exists
	db.Exec("DROP TABLE IF EXISTS export_test")
	
	// Create table
	_, err := db.Exec(`
		CREATE TABLE export_test (
			id INTEGER,
			name VARCHAR(100),
			value DOUBLE,
			created_at TIMESTAMP,
			category VARCHAR(50)
		)
	`)
	if err != nil {
		b.Fatal(err)
	}
	
	// Insert data
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	
	stmt, err := tx.Prepare("INSERT INTO export_test VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	
	categories := []string{"A", "B", "C", "D", "E"}
	baseTime := time.Now()
	
	for i := 0; i < rowCount; i++ {
		_, err := stmt.Exec(
			i,
			fmt.Sprintf("Item_%d", i),
			float64(i)*1.5,
			baseTime.Add(time.Duration(i)*time.Second),
			categories[i%len(categories)],
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	
	stmt.Close()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

func findParquetFile(b *testing.B) string {
	// Look for parquet files in common test data locations
	locations := []string{
		"../duckdb/data/parquet-testing/userdata1.parquet",
		"../../duckdb/data/parquet-testing/userdata1.parquet",
		"testdata/sample.parquet",
		"sales_data.parquet", // Found in the root directory
	}
	
	for _, loc := range locations {
		if _, err := os.Stat(loc); err == nil {
			absPath, _ := filepath.Abs(loc)
			return absPath
		}
	}
	
	return ""
}