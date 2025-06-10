package main

import (
	"database/sql"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	_ "github.com/connerohnesorge/dukdb-go"
)

func main() {
	fmt.Println("=== DuckDB Performance Testing ===\n")

	// Performance tests
	fmt.Println("1. Connection Performance")
	testConnectionPerformance()

	fmt.Println("\n2. Query Performance")
	testQueryPerformance()

	fmt.Println("\n3. Insert Performance")
	testInsertPerformance()

	fmt.Println("\n4. Transaction Performance")
	testTransactionPerformance()

	fmt.Println("\n5. Concurrent Access Performance")
	testConcurrentPerformance()

	fmt.Println("\n6. Memory Usage Analysis")
	testMemoryUsage()
}

func testConnectionPerformance() {
	iterations := 100

	start := time.Now()
	for i := 0; i < iterations; i++ {
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			log.Fatal(err)
		}
		db.Close()
	}
	duration := time.Since(start)

	fmt.Printf("  Created %d connections in %v\n", iterations, duration)
	fmt.Printf("  Average: %.2fms per connection\n",
		float64(duration.Nanoseconds())/float64(iterations)/1e6)
}

func testQueryPerformance() {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Setup test data
	db.Exec("CREATE TABLE perf_test (id INTEGER, value VARCHAR, amount DOUBLE)")

	// Insert test data
	fmt.Print("  Setting up test data...")
	start := time.Now()
	for i := 0; i < 10000; i++ {
		db.Exec("INSERT INTO perf_test VALUES (?, ?, ?)",
			i, fmt.Sprintf("value_%d", i), float64(i)*1.5)
	}
	setupTime := time.Since(start)
	fmt.Printf(" %v\n", setupTime)

	// Test different query types
	queries := []struct {
		name  string
		query string
		args  []interface{}
	}{
		{"Simple Select", "SELECT COUNT(*) FROM perf_test", nil},
		{"Range Query", "SELECT * FROM perf_test WHERE id BETWEEN ? AND ?", []interface{}{1000, 2000}},
		{"Aggregation", "SELECT AVG(amount), MIN(amount), MAX(amount) FROM perf_test", nil},
		{"GROUP BY", "SELECT id % 10 as bucket, COUNT(*), AVG(amount) FROM perf_test GROUP BY id % 10", nil},
		{"String Search", "SELECT * FROM perf_test WHERE value LIKE ?", []interface{}{"%value_1%"}},
	}

	for _, q := range queries {
		iterations := 100
		start := time.Now()

		for i := 0; i < iterations; i++ {
			var rows *sql.Rows
			var err error

			if q.args != nil {
				rows, err = db.Query(q.query, q.args...)
			} else {
				rows, err = db.Query(q.query)
			}

			if err != nil {
				log.Fatal(err)
			}

			// Consume results
			for rows.Next() {
				// Scan into interface{} to avoid type-specific overhead
				cols, _ := rows.Columns()
				values := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				rows.Scan(valuePtrs...)
			}
			rows.Close()
		}

		duration := time.Since(start)
		fmt.Printf("  %s: %d queries in %v (%.2fms avg)\n",
			q.name, iterations, duration,
			float64(duration.Nanoseconds())/float64(iterations)/1e6)
	}
}

func testInsertPerformance() {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Test individual inserts
	db.Exec("CREATE TABLE insert_test1 (id INTEGER, value VARCHAR)")

	iterations := 1000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		db.Exec("INSERT INTO insert_test1 VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
	}
	singleDuration := time.Since(start)

	fmt.Printf("  Individual inserts: %d inserts in %v (%.2fms avg)\n",
		iterations, singleDuration,
		float64(singleDuration.Nanoseconds())/float64(iterations)/1e6)

	// Test prepared statement inserts
	db.Exec("CREATE TABLE insert_test2 (id INTEGER, value VARCHAR)")
	stmt, err := db.Prepare("INSERT INTO insert_test2 VALUES (?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	start = time.Now()
	for i := 0; i < iterations; i++ {
		stmt.Exec(i, fmt.Sprintf("value_%d", i))
	}
	preparedDuration := time.Since(start)

	fmt.Printf("  Prepared inserts: %d inserts in %v (%.2fms avg)\n",
		iterations, preparedDuration,
		float64(preparedDuration.Nanoseconds())/float64(iterations)/1e6)

	// Test batch inserts
	db.Exec("CREATE TABLE insert_test3 (id INTEGER, value VARCHAR)")

	batchSizes := []int{10, 100, 1000}
	for _, batchSize := range batchSizes {
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

		db.Exec(query, values...)
		batchDuration := time.Since(start)

		fmt.Printf("  Batch insert (%d rows): %v (%.2fms per row)\n",
			batchSize, batchDuration,
			float64(batchDuration.Nanoseconds())/float64(batchSize)/1e6)
	}
}

func testTransactionPerformance() {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE tx_test (id INTEGER, value VARCHAR)")

	// Test without transactions
	iterations := 1000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		db.Exec("INSERT INTO tx_test VALUES (?, ?)", i, fmt.Sprintf("no_tx_%d", i))
	}
	noTxDuration := time.Since(start)

	// Test with individual transactions
	start = time.Now()
	for i := 0; i < iterations; i++ {
		tx, _ := db.Begin()
		tx.Exec("INSERT INTO tx_test VALUES (?, ?)", i+iterations, fmt.Sprintf("single_tx_%d", i))
		tx.Commit()
	}
	singleTxDuration := time.Since(start)

	// Test with batch transaction
	start = time.Now()
	tx, _ := db.Begin()
	for i := 0; i < iterations; i++ {
		tx.Exec("INSERT INTO tx_test VALUES (?, ?)", i+iterations*2, fmt.Sprintf("batch_tx_%d", i))
	}
	tx.Commit()
	batchTxDuration := time.Since(start)

	fmt.Printf("  No transactions: %d inserts in %v\n", iterations, noTxDuration)
	fmt.Printf("  Individual transactions: %d inserts in %v (%.1fx slower)\n",
		iterations, singleTxDuration,
		float64(singleTxDuration)/float64(noTxDuration))
	fmt.Printf("  Batch transaction: %d inserts in %v (%.1fx faster than individual)\n",
		iterations, batchTxDuration,
		float64(singleTxDuration)/float64(batchTxDuration))
}

func testConcurrentPerformance() {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Configure connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	// Setup test data
	db.Exec("CREATE TABLE concurrent_test (id INTEGER, value VARCHAR)")
	for i := 0; i < 1000; i++ {
		db.Exec("INSERT INTO concurrent_test VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
	}

	// Test different concurrency levels
	concurrencyLevels := []int{1, 2, 4, 8, 16}

	for _, numGoroutines := range concurrencyLevels {
		iterations := 1000

		start := time.Now()
		var wg sync.WaitGroup

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()

				for i := 0; i < iterations/numGoroutines; i++ {
					rows, err := db.Query("SELECT COUNT(*) FROM concurrent_test WHERE id < ?", i*10)
					if err != nil {
						log.Printf("Query error: %v", err)
						return
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
		fmt.Printf("  %d goroutines: %d queries in %v (%.0f QPS)\n",
			numGoroutines, iterations, duration, qps)
	}
}

func testMemoryUsage() {
	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table and insert large amounts of data
	db.Exec("CREATE TABLE memory_test (id INTEGER, data VARCHAR)")

	iterations := 10000
	for i := 0; i < iterations; i++ {
		// Large string data
		largeData := fmt.Sprintf("%0*d", 100, i) // 100 byte string
		db.Exec("INSERT INTO memory_test VALUES (?, ?)", i, largeData)
	}

	runtime.GC()
	runtime.ReadMemStats(&m2)

	fmt.Printf("  Memory usage:\n")
	fmt.Printf("    Heap allocated: %.2f MB\n",
		float64(m2.Alloc-m1.Alloc)/1024/1024)
	fmt.Printf("    Total allocations: %.2f MB\n",
		float64(m2.TotalAlloc-m1.TotalAlloc)/1024/1024)
	fmt.Printf("    System memory: %.2f MB\n",
		float64(m2.Sys-m1.Sys)/1024/1024)
	fmt.Printf("    GC cycles: %d\n", m2.NumGC-m1.NumGC)

	// Test memory cleanup
	db.Exec("DELETE FROM memory_test")
	runtime.GC()

	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)

	fmt.Printf("  After cleanup:\n")
	fmt.Printf("    Heap allocated: %.2f MB\n",
		float64(m3.Alloc)/1024/1024)
}
