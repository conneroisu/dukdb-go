package basic

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
	
	"github.com/connerohnesorge/dukdb-go/benchmarks"
)

// Benchmark simple SELECT queries
func BenchmarkSimpleSelect(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupSimpleTable(b, db)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT id, name, value FROM simple_table WHERE id = ?", i%1000)
			if err != nil {
				b.Fatal(err)
			}
			
			// Consume results
			for rows.Next() {
				var id int
				var name string
				var value float64
				if err := rows.Scan(&id, &name, &value); err != nil {
					b.Fatal(err)
				}
			}
			rows.Close()
		}
	})
}

// Benchmark range SELECT queries
func BenchmarkRangeSelect(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupSimpleTable(b, db)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := (i * 100) % 900
			end := start + 100
			
			rows, err := db.Query("SELECT id, name, value FROM simple_table WHERE id BETWEEN ? AND ?", start, end)
			if err != nil {
				b.Fatal(err)
			}
			
			// Consume results
			count := 0
			for rows.Next() {
				var id int
				var name string
				var value float64
				if err := rows.Scan(&id, &name, &value); err != nil {
					b.Fatal(err)
				}
				count++
			}
			rows.Close()
			
			if count == 0 {
				b.Fatal("No rows returned")
			}
		}
	})
}

// Benchmark single INSERT operations
func BenchmarkSingleInsert(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Drop table if exists
		db.Exec("DROP TABLE IF EXISTS insert_test")
		
		// Create table
		_, err := db.Exec(`
			CREATE TABLE insert_test (
				id INTEGER PRIMARY KEY,
				name VARCHAR(100),
				value DOUBLE,
				created_at TIMESTAMP
			)
		`)
		if err != nil {
			b.Fatal(err)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := db.Exec(
				"INSERT INTO insert_test (id, name, value, created_at) VALUES (?, ?, ?, ?)",
				i, fmt.Sprintf("Item_%d", i), float64(i)*1.5, time.Now(),
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark batch INSERT operations
func BenchmarkBatchInsert(b *testing.B) {
	batchSizes := []int{10, 100, 1000}
	
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
				b.ResetTimer()
				
				for i := 0; i < b.N; i++ {
					// Create new table for each iteration
					tableName := fmt.Sprintf("batch_insert_%d", i)
					_, err := db.Exec(fmt.Sprintf(`
						CREATE TABLE %s (
							id INTEGER PRIMARY KEY,
							name VARCHAR(100),
							value DOUBLE
						)
					`, tableName))
					if err != nil {
						b.Fatal(err)
					}
					
					// Build batch insert query
					query := fmt.Sprintf("INSERT INTO %s VALUES ", tableName)
					values := make([]interface{}, 0, batchSize*3)
					
					for j := 0; j < batchSize; j++ {
						if j > 0 {
							query += ", "
						}
						query += "(?, ?, ?)"
						values = append(values, j, fmt.Sprintf("Item_%d", j), float64(j)*1.5)
					}
					
					// Execute batch insert
					_, err = db.Exec(query, values...)
					if err != nil {
						b.Fatal(err)
					}
					
					// Clean up
					db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
				}
			})
		})
	}
}

// Benchmark prepared INSERT statements
func BenchmarkPreparedInsert(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Drop table if exists
		db.Exec("DROP TABLE IF EXISTS prepared_insert_test")
		
		// Create table
		_, err := db.Exec(`
			CREATE TABLE prepared_insert_test (
				id INTEGER PRIMARY KEY,
				name VARCHAR(100),
				value DOUBLE
			)
		`)
		if err != nil {
			b.Fatal(err)
		}
		
		// Prepare statement
		stmt, err := db.Prepare("INSERT INTO prepared_insert_test (id, name, value) VALUES (?, ?, ?)")
		if err != nil {
			b.Fatal(err)
		}
		defer stmt.Close()
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := stmt.Exec(i, fmt.Sprintf("Item_%d", i), float64(i)*1.5)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark UPDATE operations
func BenchmarkUpdate(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup table with data
		setupSimpleTable(b, db)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			id := i % 1000
			newValue := float64(i) * 2.5
			
			result, err := db.Exec("UPDATE simple_table SET value = ? WHERE id = ?", newValue, id)
			if err != nil {
				b.Fatal(err)
			}
			
			// Verify update
			affected, err := result.RowsAffected()
			if err != nil {
				b.Fatal(err)
			}
			if affected != 1 {
				b.Fatalf("Expected 1 row affected, got %d", affected)
			}
		}
	})
}

// Benchmark batch UPDATE operations
func BenchmarkBatchUpdate(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup table with data
		setupSimpleTable(b, db)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			category := fmt.Sprintf("Category_%d", i%10)
			newValue := float64(i) * 1.1
			
			result, err := db.Exec(
				"UPDATE simple_table SET value = ? WHERE name LIKE ?",
				newValue, category+"%",
			)
			if err != nil {
				b.Fatal(err)
			}
			
			// Check that some rows were updated
			affected, err := result.RowsAffected()
			if err != nil {
				b.Fatal(err)
			}
			if affected == 0 {
				b.Fatal("No rows were updated")
			}
		}
	})
}

// Benchmark DELETE operations
func BenchmarkDelete(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			// Recreate table for each iteration
			setupSimpleTableWithRows(b, db, 100)
			
			// Delete specific row
			result, err := db.Exec("DELETE FROM simple_table WHERE id = ?", i%100)
			if err != nil {
				b.Fatal(err)
			}
			
			affected, err := result.RowsAffected()
			if err != nil {
				b.Fatal(err)
			}
			if affected != 1 {
				b.Fatalf("Expected 1 row deleted, got %d", affected)
			}
		}
	})
}

// Benchmark batch DELETE operations
func BenchmarkBatchDelete(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		b.ResetTimer()
		
		for i := 0; i < b.N; i++ {
			// Recreate table for each iteration
			setupSimpleTableWithRows(b, db, 1000)
			
			// Delete range of rows
			start := (i * 100) % 900
			end := start + 100
			
			result, err := db.Exec("DELETE FROM simple_table WHERE id BETWEEN ? AND ?", start, end)
			if err != nil {
				b.Fatal(err)
			}
			
			affected, err := result.RowsAffected()
			if err != nil {
				b.Fatal(err)
			}
			if affected == 0 {
				b.Fatal("No rows were deleted")
			}
		}
	})
}

// Benchmark transaction with mixed operations
func BenchmarkMixedTransaction(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup initial table
		setupSimpleTable(b, db)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tx, err := db.Begin()
			if err != nil {
				b.Fatal(err)
			}
			
			// INSERT
			_, err = tx.Exec(
				"INSERT INTO simple_table (id, name, value) VALUES (?, ?, ?)",
				1000+i, fmt.Sprintf("TX_Item_%d", i), float64(i)*3.14,
			)
			if err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
			
			// UPDATE
			_, err = tx.Exec("UPDATE simple_table SET value = value * 1.1 WHERE id < ?", 100)
			if err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
			
			// SELECT
			var count int
			err = tx.QueryRow("SELECT COUNT(*) FROM simple_table WHERE value > ?", 100.0).Scan(&count)
			if err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
			
			// DELETE
			_, err = tx.Exec("DELETE FROM simple_table WHERE id = ?", i%100)
			if err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
			
			// Commit
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
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

func setupSimpleTable(b *testing.B, db *sql.DB) {
	setupSimpleTableWithRows(b, db, 1000)
}

func setupSimpleTableWithRows(b *testing.B, db *sql.DB, rowCount int) {
	// Drop table if exists
	if _, err := db.Exec("DROP TABLE IF EXISTS simple_table"); err != nil {
		b.Logf("Warning: DROP TABLE failed: %v", err)
	}
	
	// Create table
	_, err := db.Exec(`
		CREATE TABLE simple_table (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			value DOUBLE
		)
	`)
	if err != nil {
		b.Fatal(err)
	}
	
	// Insert test data
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	
	stmt, err := tx.Prepare("INSERT INTO simple_table (id, name, value) VALUES (?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	
	for i := 0; i < rowCount; i++ {
		category := fmt.Sprintf("Category_%d", i%10)
		name := fmt.Sprintf("%s_Item_%d", category, i)
		_, err := stmt.Exec(i, name, float64(i)*1.5)
		if err != nil {
			b.Fatal(err)
		}
	}
	
	stmt.Close()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}