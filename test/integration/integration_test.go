//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/connerohnesorge/dukdb-go"
)

func TestRealDuckDBIntegration(t *testing.T) {
	// Test both in-memory and file-based databases
	t.Run("InMemory", func(t *testing.T) {
		testDuckDBFeatures(t, ":memory:")
	})

	t.Run("FileDatabase", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")
		testDuckDBFeatures(t, dbPath)

		// Verify file was created
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			t.Error("Database file was not created")
		}
	})
}

func testDuckDBFeatures(t *testing.T, dsn string) {
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	// Run various feature tests
	t.Run("BasicTypes", func(t *testing.T) { testBasicTypes(t, db) })
	t.Run("Transactions", func(t *testing.T) { testTransactions(t, db) })
	t.Run("PreparedStatements", func(t *testing.T) { testPreparedStatements(t, db) })
	t.Run("DateTimeTypes", func(t *testing.T) { testDateTimeTypes(t, db) })
	t.Run("BlobData", func(t *testing.T) { testBlobData(t, db) })
	t.Run("Analytics", func(t *testing.T) { testAnalyticsFunctions(t, db) })
	t.Run("ConcurrentAccess", func(t *testing.T) { testConcurrentAccess(t, db) })
}

func testBasicTypes(t *testing.T, db *sql.DB) {
	// Create table with various types
	_, err := db.Exec(`
		CREATE TABLE type_test (
			bool_col BOOLEAN,
			tinyint_col TINYINT,
			smallint_col SMALLINT,
			int_col INTEGER,
			bigint_col BIGINT,
			utinyint_col UTINYINT,
			usmallint_col USMALLINT,
			uint_col UINTEGER,
			ubigint_col UBIGINT,
			float_col FLOAT,
			double_col DOUBLE,
			varchar_col VARCHAR,
			decimal_col DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`
		INSERT INTO type_test VALUES (
			true, -128, -32768, -2147483648, -9223372036854775808,
			255, 65535, 4294967295, 18446744073709551615,
			3.14, 2.71828, 'Hello DuckDB!', 12345.67
		)
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Query and verify
	var (
		boolVal     bool
		tinyintVal  int8
		smallintVal int16
		intVal      int32
		bigintVal   int64
		utinyintVal uint8
		usmallVal   uint16
		uintVal     uint32
		ubigintVal  uint64
		floatVal    float32
		doubleVal   float64
		varcharVal  string
		decimalVal  float64
	)

	err = db.QueryRow("SELECT * FROM type_test").Scan(
		&boolVal, &tinyintVal, &smallintVal, &intVal, &bigintVal,
		&utinyintVal, &usmallVal, &uintVal, &ubigintVal,
		&floatVal, &doubleVal, &varcharVal, &decimalVal,
	)
	if err != nil {
		t.Fatalf("Failed to scan values: %v", err)
	}

	// Verify values
	if !boolVal {
		t.Error("Expected bool to be true")
	}
	if tinyintVal != -128 {
		t.Errorf("Expected tinyint -128, got %d", tinyintVal)
	}
	if varcharVal != "Hello DuckDB!" {
		t.Errorf("Expected varchar 'Hello DuckDB!', got %s", varcharVal)
	}
	if decimalVal != 12345.67 {
		t.Errorf("Expected decimal 12345.67, got %f", decimalVal)
	}

	// Clean up
	db.Exec("DROP TABLE type_test")
}

func testTransactions(t *testing.T, db *sql.DB) {
	// Create test table
	_, err := db.Exec("CREATE TABLE tx_test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE tx_test")

	// Insert initial data
	db.Exec("INSERT INTO tx_test VALUES (1, 100)")

	// Test rollback
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	tx.Exec("UPDATE tx_test SET value = 200 WHERE id = 1")
	tx.Rollback()

	// Verify rollback worked
	var value int
	db.QueryRow("SELECT value FROM tx_test WHERE id = 1").Scan(&value)
	if value != 100 {
		t.Errorf("Rollback failed: expected 100, got %d", value)
	}

	// Test commit
	tx, err = db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	tx.Exec("UPDATE tx_test SET value = 300 WHERE id = 1")
	tx.Commit()

	// Verify commit worked
	db.QueryRow("SELECT value FROM tx_test WHERE id = 1").Scan(&value)
	if value != 300 {
		t.Errorf("Commit failed: expected 300, got %d", value)
	}
}

func testPreparedStatements(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec("CREATE TABLE prep_test (id INTEGER, name VARCHAR, score DOUBLE)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE prep_test")

	// Prepare insert statement
	stmt, err := db.Prepare("INSERT INTO prep_test VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute multiple times
	data := []struct {
		id    int
		name  string
		score float64
	}{
		{1, "Alice", 95.5},
		{2, "Bob", 87.3},
		{3, "Charlie", 92.1},
	}

	for _, d := range data {
		_, err = stmt.Exec(d.id, d.name, d.score)
		if err != nil {
			t.Fatalf("Failed to execute prepared statement: %v", err)
		}
	}

	// Verify data
	var count int
	db.QueryRow("SELECT COUNT(*) FROM prep_test").Scan(&count)
	if count != len(data) {
		t.Errorf("Expected %d rows, got %d", len(data), count)
	}

	// Test prepared query
	queryStmt, err := db.Prepare("SELECT name, score FROM prep_test WHERE score > ?")
	if err != nil {
		t.Fatalf("Failed to prepare query: %v", err)
	}
	defer queryStmt.Close()

	rows, err := queryStmt.Query(90.0)
	if err != nil {
		t.Fatalf("Failed to execute prepared query: %v", err)
	}
	defer rows.Close()

	var resultCount int
	for rows.Next() {
		var name string
		var score float64
		rows.Scan(&name, &score)
		resultCount++
		t.Logf("High scorer: %s with %.1f", name, score)
	}

	if resultCount != 2 {
		t.Errorf("Expected 2 high scorers, got %d", resultCount)
	}
}

func testDateTimeTypes(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec(`
		CREATE TABLE datetime_test (
			date_col DATE,
			time_col TIME,
			timestamp_col TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE datetime_test")

	// Test values
	testDate := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	testTime := time.Date(1970, 1, 1, 14, 30, 45, 0, time.UTC)
	testTimestamp := time.Date(2024, 1, 15, 14, 30, 45, 123000000, time.UTC)

	// Insert
	_, err = db.Exec("INSERT INTO datetime_test VALUES (?, ?, ?)",
		testDate, testTime, testTimestamp)
	if err != nil {
		t.Fatalf("Failed to insert datetime values: %v", err)
	}

	// Query back
	var date, timeVal, timestamp time.Time
	err = db.QueryRow("SELECT * FROM datetime_test").Scan(&date, &timeVal, &timestamp)
	if err != nil {
		t.Fatalf("Failed to scan datetime values: %v", err)
	}

	// Verify date
	if date.Year() != testDate.Year() || date.Month() != testDate.Month() || date.Day() != testDate.Day() {
		t.Errorf("Date mismatch: expected %v, got %v", testDate, date)
	}

	// Verify time
	if timeVal.Hour() != testTime.Hour() || timeVal.Minute() != testTime.Minute() {
		t.Errorf("Time mismatch: expected %v, got %v", testTime, timeVal)
	}

	// Verify timestamp (within microsecond precision)
	diff := timestamp.Sub(testTimestamp).Abs()
	if diff > time.Microsecond {
		t.Errorf("Timestamp mismatch: expected %v, got %v (diff: %v)",
			testTimestamp, timestamp, diff)
	}
}

func testBlobData(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec("CREATE TABLE blob_test (id INTEGER, data BLOB)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE blob_test")

	// Test data including binary
	testData := [][]byte{
		[]byte("Simple text"),
		[]byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD},
		[]byte("UTF-8 text: Hello 世界 🦆"),
		make([]byte, 1024), // 1KB of zeros
	}

	// Fill last test data with pattern
	for i := range testData[3] {
		testData[3][i] = byte(i % 256)
	}

	// Insert blobs
	for i, data := range testData {
		_, err = db.Exec("INSERT INTO blob_test VALUES (?, ?)", i, data)
		if err != nil {
			t.Fatalf("Failed to insert blob %d: %v", i, err)
		}
	}

	// Query back and verify
	rows, err := db.Query("SELECT id, data FROM blob_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query blobs: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var id int
		var data []byte
		err = rows.Scan(&id, &data)
		if err != nil {
			t.Fatalf("Failed to scan blob: %v", err)
		}

		if len(data) != len(testData[i]) {
			t.Errorf("Blob %d length mismatch: expected %d, got %d",
				i, len(testData[i]), len(data))
		}

		for j := range data {
			if data[j] != testData[i][j] {
				t.Errorf("Blob %d byte %d mismatch: expected %v, got %v",
					i, j, testData[i][j], data[j])
				break
			}
		}
		i++
	}
}

func testAnalyticsFunctions(t *testing.T, db *sql.DB) {
	// Create and populate sales table
	_, err := db.Exec(`
		CREATE TABLE sales (
			date DATE,
			product VARCHAR,
			amount DECIMAL(10,2)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE sales")

	// Insert sample data
	_, err = db.Exec(`
		INSERT INTO sales VALUES
			('2024-01-01', 'Widget', 100.00),
			('2024-01-01', 'Gadget', 150.00),
			('2024-01-02', 'Widget', 120.00),
			('2024-01-02', 'Gadget', 180.00),
			('2024-01-03', 'Widget', 110.00),
			('2024-01-03', 'Gadget', 160.00)
	`)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test aggregations
	var totalSales float64
	err = db.QueryRow("SELECT SUM(amount) FROM sales").Scan(&totalSales)
	if err != nil {
		t.Fatalf("Failed to calculate sum: %v", err)
	}
	if totalSales != 820.00 {
		t.Errorf("Expected total sales 820.00, got %.2f", totalSales)
	}

	// Test GROUP BY
	rows, err := db.Query(`
		SELECT product, COUNT(*) as cnt, AVG(amount) as avg_amount
		FROM sales
		GROUP BY product
		ORDER BY product
	`)
	if err != nil {
		t.Fatalf("Failed to query grouped data: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		product string
		count   int
		avg     float64
	}{
		{"Gadget", 3, 163.33},
		{"Widget", 3, 110.00},
	}

	i := 0
	for rows.Next() {
		var product string
		var count int
		var avg float64
		err = rows.Scan(&product, &count, &avg)
		if err != nil {
			t.Fatalf("Failed to scan grouped result: %v", err)
		}

		if i >= len(expected) {
			t.Fatal("Too many results")
		}

		if product != expected[i].product || count != expected[i].count {
			t.Errorf("Row %d mismatch: expected %v, got (%s, %d, %.2f)",
				i, expected[i], product, count, avg)
		}
		i++
	}
}

func testConcurrentAccess(t *testing.T, db *sql.DB) {
	// Create table
	_, err := db.Exec("CREATE TABLE concurrent_test (id INTEGER, value INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE concurrent_test")

	// Set up connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	// Run concurrent operations
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// Each goroutine performs multiple operations
			for j := 0; j < 10; j++ {
				_, err := db.Exec("INSERT INTO concurrent_test VALUES (?, ?)",
					id*10+j, id*100+j)
				if err != nil {
					t.Errorf("Goroutine %d: insert failed: %v", id, err)
					return
				}

				var count int
				err = db.QueryRow("SELECT COUNT(*) FROM concurrent_test WHERE id >= ?",
					id*10).Scan(&count)
				if err != nil {
					t.Errorf("Goroutine %d: query failed: %v", id, err)
					return
				}
			}
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify final count
	var totalCount int
	err = db.QueryRow("SELECT COUNT(*) FROM concurrent_test").Scan(&totalCount)
	if err != nil {
		t.Fatalf("Failed to get final count: %v", err)
	}
	if totalCount != 100 {
		t.Errorf("Expected 100 rows, got %d", totalCount)
	}
}

// Benchmark tests
func BenchmarkInsert(b *testing.B) {
	db, _ := sql.Open("duckdb", ":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE bench_test (id INTEGER, value VARCHAR)")
	defer db.Exec("DROP TABLE bench_test")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO bench_test VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
	}
}

func BenchmarkPreparedInsert(b *testing.B) {
	db, _ := sql.Open("duckdb", ":memory:")
	defer db.Close()

	db.Exec("CREATE TABLE bench_test (id INTEGER, value VARCHAR)")
	defer db.Exec("DROP TABLE bench_test")

	stmt, _ := db.Prepare("INSERT INTO bench_test VALUES (?, ?)")
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Exec(i, fmt.Sprintf("value_%d", i))
	}
}

func BenchmarkSelect(b *testing.B) {
	db, _ := sql.Open("duckdb", ":memory:")
	defer db.Close()

	// Prepare data
	db.Exec("CREATE TABLE bench_test (id INTEGER, value VARCHAR)")
	for i := 0; i < 1000; i++ {
		db.Exec("INSERT INTO bench_test VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, _ := db.Query("SELECT * FROM bench_test WHERE id < 100")
		for rows.Next() {
			var id int
			var value string
			rows.Scan(&id, &value)
		}
		rows.Close()
	}
}
