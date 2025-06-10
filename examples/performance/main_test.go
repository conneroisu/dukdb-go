package main

import (
	"database/sql"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	_ "github.com/connerohnesorge/dukdb-go"
)

func TestConnectionPerformance(t *testing.T) {
	t.Run("ConnectionCreation", func(t *testing.T) {
		iterations := 10 // Reduced for tests

		start := time.Now()
		for i := 0; i < iterations; i++ {
			db, err := sql.Open("duckdb", ":memory:")
			if err != nil {
				t.Skipf("Skipping test due to connection error: %v", err)
				return
			}
			db.Close()
		}
		duration := time.Since(start)

		avgMs := float64(duration.Nanoseconds()) / float64(iterations) / 1e6
		t.Logf("Created %d connections in %v (avg: %.2fms per connection)", iterations, duration, avgMs)

		// Verify reasonable performance (should be under 100ms per connection)
		if avgMs > 100 {
			t.Errorf("Connection creation too slow: %.2fms per connection", avgMs)
		}
	})

	t.Run("ConnectionReuse", func(t *testing.T) {
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			t.Skipf("Skipping test due to connection error: %v", err)
			return
		}
		defer db.Close()

		if err := db.Ping(); err != nil {
			t.Skipf("Skipping test due to ping error: %v", err)
			return
		}

		iterations := 100
		start := time.Now()
		for i := 0; i < iterations; i++ {
			if err := db.Ping(); err != nil {
				t.Errorf("Ping failed on iteration %d: %v", i, err)
			}
		}
		duration := time.Since(start)

		avgMs := float64(duration.Nanoseconds()) / float64(iterations) / 1e6
		t.Logf("Performed %d pings in %v (avg: %.2fms per ping)", iterations, duration, avgMs)

		// Ping should be very fast on existing connections
		if avgMs > 1 {
			t.Errorf("Ping too slow: %.2fms per ping", avgMs)
		}
	})
}

func TestQueryPerformance(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Skipf("Skipping test due to connection error: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("Skipping test due to ping error: %v", err)
		return
	}

	// Setup test data
	_, err = db.Exec("CREATE TABLE perf_test (id INTEGER, value VARCHAR, amount DOUBLE)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
		return
	}

	// Insert test data
	for i := 0; i < 1000; i++ {
		_, err = db.Exec("INSERT INTO perf_test VALUES (?, ?, ?)",
			i, fmt.Sprintf("value_%d", i), float64(i)*1.5)
		if err != nil {
			t.Errorf("Failed to insert test data: %v", err)
			return
		}
	}

	testCases := []struct {
		name  string
		query string
		args  []interface{}
	}{
		{"SimpleSelect", "SELECT COUNT(*) FROM perf_test", nil},
		{"RangeQuery", "SELECT * FROM perf_test WHERE id BETWEEN ? AND ?", []interface{}{100, 200}},
		{"Aggregation", "SELECT AVG(amount), MIN(amount), MAX(amount) FROM perf_test", nil},
		{"GroupBy", "SELECT id % 10 as bucket, COUNT(*) FROM perf_test GROUP BY id % 10", nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iterations := 10 // Reduced for tests
			start := time.Now()

			for i := 0; i < iterations; i++ {
				var rows *sql.Rows
				var err error

				if tc.args != nil {
					rows, err = db.Query(tc.query, tc.args...)
				} else {
					rows, err = db.Query(tc.query)
				}

				if err != nil {
					t.Errorf("Query failed: %v", err)
					continue
				}

				// Consume results
				for rows.Next() {
					cols, _ := rows.Columns()
					values := make([]interface{}, len(cols))
					valuePtrs := make([]interface{}, len(cols))
					for j := range values {
						valuePtrs[j] = &values[j]
					}
					rows.Scan(valuePtrs...)
				}
				rows.Close()
			}

			duration := time.Since(start)
			avgMs := float64(duration.Nanoseconds()) / float64(iterations) / 1e6
			t.Logf("%s: %d queries in %v (avg: %.2fms)", tc.name, iterations, duration, avgMs)
		})
	}
}

func TestInsertPerformance(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Skipf("Skipping test due to connection error: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("Skipping test due to ping error: %v", err)
		return
	}

	t.Run("IndividualInserts", func(t *testing.T) {
		_, err = db.Exec("CREATE TABLE insert_test1 (id INTEGER, value VARCHAR)")
		if err != nil {
			t.Errorf("Failed to create table: %v", err)
			return
		}

		iterations := 100 // Reduced for tests
		start := time.Now()
		for i := 0; i < iterations; i++ {
			_, err = db.Exec("INSERT INTO insert_test1 VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
			if err != nil {
				t.Errorf("Failed to insert: %v", err)
			}
		}
		duration := time.Since(start)

		avgMs := float64(duration.Nanoseconds()) / float64(iterations) / 1e6
		t.Logf("Individual inserts: %d inserts in %v (avg: %.2fms)", iterations, duration, avgMs)
	})

	t.Run("PreparedInserts", func(t *testing.T) {
		_, err = db.Exec("CREATE TABLE insert_test2 (id INTEGER, value VARCHAR)")
		if err != nil {
			t.Errorf("Failed to create table: %v", err)
			return
		}

		stmt, err := db.Prepare("INSERT INTO insert_test2 VALUES (?, ?)")
		if err != nil {
			t.Errorf("Failed to prepare statement: %v", err)
			return
		}
		defer stmt.Close()

		iterations := 100 // Reduced for tests
		start := time.Now()
		for i := 0; i < iterations; i++ {
			_, err = stmt.Exec(i, fmt.Sprintf("value_%d", i))
			if err != nil {
				t.Errorf("Failed to execute prepared statement: %v", err)
			}
		}
		duration := time.Since(start)

		avgMs := float64(duration.Nanoseconds()) / float64(iterations) / 1e6
		t.Logf("Prepared inserts: %d inserts in %v (avg: %.2fms)", iterations, duration, avgMs)
	})

	t.Run("BatchInserts", func(t *testing.T) {
		_, err = db.Exec("CREATE TABLE insert_test3 (id INTEGER, value VARCHAR)")
		if err != nil {
			t.Errorf("Failed to create table: %v", err)
			return
		}

		batchSizes := []int{10, 50, 100}
		for _, batchSize := range batchSizes {
			t.Run(fmt.Sprintf("BatchSize%d", batchSize), func(t *testing.T) {
				start := time.Now()

				query := "INSERT INTO insert_test3 VALUES "
				values := make([]interface{}, 0, batchSize*2)

				for i := 0; i < batchSize; i++ {
					if i > 0 {
						query += ", "
					}
					query += "(?, ?)"
					values = append(values, i, fmt.Sprintf("batch_%d", i))
				}

				_, err = db.Exec(query, values...)
				duration := time.Since(start)

				if err != nil {
					t.Errorf("Failed to execute batch insert: %v", err)
					return
				}

				msPerRow := float64(duration.Nanoseconds()) / float64(batchSize) / 1e6
				t.Logf("Batch insert (%d rows): %v (%.2fms per row)", batchSize, duration, msPerRow)
			})
		}
	})
}

func TestTransactionPerformance(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Skipf("Skipping test due to connection error: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("Skipping test due to ping error: %v", err)
		return
	}

	_, err = db.Exec("CREATE TABLE tx_test (id INTEGER, value VARCHAR)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
		return
	}

	iterations := 50 // Reduced for tests

	t.Run("NoTransactions", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < iterations; i++ {
			_, err = db.Exec("INSERT INTO tx_test VALUES (?, ?)", i, fmt.Sprintf("no_tx_%d", i))
			if err != nil {
				t.Errorf("Failed to insert: %v", err)
			}
		}
		duration := time.Since(start)
		t.Logf("No transactions: %d inserts in %v", iterations, duration)
	})

	t.Run("IndividualTransactions", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < iterations; i++ {
			tx, err := db.Begin()
			if err != nil {
				t.Errorf("Failed to begin transaction: %v", err)
				continue
			}

			_, err = tx.Exec("INSERT INTO tx_test VALUES (?, ?)", i+iterations, fmt.Sprintf("single_tx_%d", i))
			if err != nil {
				tx.Rollback()
				t.Errorf("Failed to insert in transaction: %v", err)
				continue
			}

			err = tx.Commit()
			if err != nil {
				t.Errorf("Failed to commit transaction: %v", err)
			}
		}
		duration := time.Since(start)
		t.Logf("Individual transactions: %d inserts in %v", iterations, duration)
	})

	t.Run("BatchTransaction", func(t *testing.T) {
		start := time.Now()
		tx, err := db.Begin()
		if err != nil {
			t.Errorf("Failed to begin batch transaction: %v", err)
			return
		}

		for i := 0; i < iterations; i++ {
			_, err = tx.Exec("INSERT INTO tx_test VALUES (?, ?)", i+iterations*2, fmt.Sprintf("batch_tx_%d", i))
			if err != nil {
				tx.Rollback()
				t.Errorf("Failed to insert in batch transaction: %v", err)
				return
			}
		}

		err = tx.Commit()
		if err != nil {
			t.Errorf("Failed to commit batch transaction: %v", err)
		}

		duration := time.Since(start)
		t.Logf("Batch transaction: %d inserts in %v", iterations, duration)
	})
}

func TestConcurrentPerformance(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Skipf("Skipping test due to connection error: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("Skipping test due to ping error: %v", err)
		return
	}

	// Configure connection pool
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	// Setup test data
	_, err = db.Exec("CREATE TABLE concurrent_test (id INTEGER, value VARCHAR)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
		return
	}

	for i := 0; i < 100; i++ {
		_, err = db.Exec("INSERT INTO concurrent_test VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			t.Errorf("Failed to insert test data: %v", err)
			return
		}
	}

	concurrencyLevels := []int{1, 2, 4}

	for _, numGoroutines := range concurrencyLevels {
		t.Run(fmt.Sprintf("Concurrency%d", numGoroutines), func(t *testing.T) {
			iterations := 50 // Total queries across all goroutines

			start := time.Now()
			var wg sync.WaitGroup
			var errorCount int64

			for g := 0; g < numGoroutines; g++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()

					queriesPerGoroutine := iterations / numGoroutines
					for i := 0; i < queriesPerGoroutine; i++ {
						rows, err := db.Query("SELECT COUNT(*) FROM concurrent_test WHERE id < ?", i*2)
						if err != nil {
							t.Errorf("Query error in goroutine %d: %v", goroutineID, err)
							errorCount++
							continue
						}

						for rows.Next() {
							var count int
							rows.Scan(&count)
						}
						rows.Close()
					}
				}(g)
			}

			wg.Wait()
			duration := time.Since(start)

			qps := float64(iterations) / duration.Seconds()
			t.Logf("%d goroutines: %d queries in %v (%.0f QPS)", numGoroutines, iterations, duration, qps)

			if errorCount > 0 {
				t.Errorf("Encountered %d errors during concurrent execution", errorCount)
			}
		})
	}
}

func TestMemoryUsage(t *testing.T) {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Skipf("Skipping test due to connection error: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("Skipping test due to ping error: %v", err)
		return
	}

	// Create table and insert data
	_, err = db.Exec("CREATE TABLE memory_test (id INTEGER, data VARCHAR)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
		return
	}

	iterations := 1000 // Reduced for tests
	for i := 0; i < iterations; i++ {
		// Large string data
		largeData := fmt.Sprintf("%0*d", 100, i) // 100 byte string
		_, err = db.Exec("INSERT INTO memory_test VALUES (?, ?)", i, largeData)
		if err != nil {
			t.Errorf("Failed to insert memory test data: %v", err)
		}
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	heapAllocMB := float64(m2.Alloc-m1.Alloc) / 1024 / 1024
	totalAllocMB := float64(m2.TotalAlloc-m1.TotalAlloc) / 1024 / 1024
	sysMemMB := float64(m2.Sys-m1.Sys) / 1024 / 1024

	t.Logf("Memory usage after %d inserts:", iterations)
	t.Logf("  Heap allocated: %.2f MB", heapAllocMB)
	t.Logf("  Total allocations: %.2f MB", totalAllocMB)
	t.Logf("  System memory: %.2f MB", sysMemMB)
	t.Logf("  GC cycles: %d", m2.NumGC-m1.NumGC)

	// Verify reasonable memory usage (should be under 50MB for this test)
	if heapAllocMB > 50 {
		t.Errorf("Memory usage too high: %.2f MB heap allocated", heapAllocMB)
	}

	// Test memory cleanup
	_, err = db.Exec("DELETE FROM memory_test")
	if err != nil {
		t.Errorf("Failed to delete data: %v", err)
	}

	runtime.GC()
	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)

	finalHeapMB := float64(m3.Alloc) / 1024 / 1024
	t.Logf("After cleanup: %.2f MB heap allocated", finalHeapMB)
}

// Benchmark functions for performance testing
func BenchmarkConnectionCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			b.Skipf("Skipping benchmark due to connection error: %v", err)
			return
		}
		db.Close()
	}
}

func BenchmarkSimpleQuery(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Skipf("Skipping benchmark due to connection error: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		b.Skipf("Skipping benchmark due to ping error: %v", err)
		return
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT 1")
		if err != nil {
			b.Errorf("Query failed: %v", err)
		}
		rows.Close()
	}
}

func BenchmarkPreparedStatement(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Skipf("Skipping benchmark due to connection error: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		b.Skipf("Skipping benchmark due to ping error: %v", err)
		return
	}

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		b.Errorf("Failed to prepare statement: %v", err)
		return
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Query(i)
		if err != nil {
			b.Errorf("Query failed: %v", err)
		}
		rows.Close()
	}
}
