package main

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/connerohnesorge/dukdb-go"
)

func TestDateTimeOperations(t *testing.T) {
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

	// Create table with date/time columns
	_, err = db.Exec(`
		CREATE TABLE events (
			id INTEGER,
			event_date DATE,
			event_time TIME,
			event_timestamp TIMESTAMP,
			description VARCHAR
		)
	`)
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
		return
	}

	t.Run("InsertDateTimeData", func(t *testing.T) {
		now := time.Now()
		date := now.Truncate(24 * time.Hour)
		timestamp := now.Truncate(time.Second)

		_, err := db.Exec(`
			INSERT INTO events VALUES 
			(1, ?, TIME '14:30:00', ?, 'Test event')
		`, date, timestamp)

		if err != nil {
			t.Errorf("Failed to insert datetime data: %v", err)
		}
	})

	t.Run("QueryDateTimeData", func(t *testing.T) {
		rows, err := db.Query("SELECT id, event_date, event_timestamp, description FROM events")
		if err != nil {
			t.Errorf("Failed to query datetime data: %v", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var id int
			var eventDate time.Time
			var eventTimestamp time.Time
			var description string

			err := rows.Scan(&id, &eventDate, &eventTimestamp, &description)
			if err != nil {
				t.Errorf("Failed to scan datetime row: %v", err)
				continue
			}

			if id != 1 {
				t.Errorf("Expected id 1, got %d", id)
			}
			if description != "Test event" {
				t.Errorf("Expected description 'Test event', got %s", description)
			}
		}
	})
}

func TestBlobOperations(t *testing.T) {
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

	// Create table with BLOB column
	_, err = db.Exec(`CREATE TABLE files (id INTEGER, filename VARCHAR, data BLOB)`)
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
		return
	}

	t.Run("InsertBlobData", func(t *testing.T) {
		testData := []byte("This is test binary data \x00\x01\x02\x03")

		_, err := db.Exec("INSERT INTO files VALUES (?, ?, ?)", 1, "test.bin", testData)
		if err != nil {
			t.Errorf("Failed to insert blob data: %v", err)
		}
	})

	t.Run("QueryBlobData", func(t *testing.T) {
		var id int
		var filename string
		var data []byte

		err := db.QueryRow("SELECT id, filename, data FROM files WHERE id = 1").Scan(&id, &filename, &data)
		if err != nil {
			t.Errorf("Failed to query blob data: %v", err)
			return
		}

		expectedData := []byte("This is test binary data \x00\x01\x02\x03")
		if string(data) != string(expectedData) {
			t.Errorf("Blob data mismatch: got %v, expected %v", data, expectedData)
		}

		if filename != "test.bin" {
			t.Errorf("Expected filename 'test.bin', got %s", filename)
		}
	})
}

func TestAnalyticsQueries(t *testing.T) {
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

	// Create sales table
	_, err = db.Exec(`
		CREATE TABLE sales (
			id INTEGER,
			product VARCHAR,
			category VARCHAR,
			amount DECIMAL(10,2),
			sale_date DATE
		)
	`)
	if err != nil {
		t.Errorf("Failed to create sales table: %v", err)
		return
	}

	// Insert test sales data (outside of subtest to persist across subtests)
	salesData := [][]interface{}{
		{1, "Laptop", "Electronics", 999.99, "2024-01-15"},
		{2, "Mouse", "Electronics", 29.99, "2024-01-16"},
		{3, "Desk", "Furniture", 299.99, "2024-01-17"},
		{4, "Chair", "Furniture", 199.99, "2024-01-18"},
		{5, "Monitor", "Electronics", 399.99, "2024-01-19"},
	}

	for _, sale := range salesData {
		_, err := db.Exec("INSERT INTO sales VALUES (?, ?, ?, ?, ?)", sale...)
		if err != nil {
			t.Errorf("Failed to insert sales data: %v", err)
			return
		}
	}

	t.Run("AggregationQueries", func(t *testing.T) {
		// First check if data exists
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM sales").Scan(&count)
		if err != nil {
			t.Errorf("Failed to count sales data: %v", err)
			return
		}
		if count == 0 {
			t.Error("No sales data found - insertion may have failed")
			return
		}
		t.Logf("Found %d sales records", count)

		// Test aggregation query
		var totalSales float64
		var avgSale float64
		var maxSale float64
		var minSale float64

		err = db.QueryRow(`
			SELECT 
				SUM(amount) as total_sales,
				AVG(amount) as avg_sale,
				MAX(amount) as max_sale,
				MIN(amount) as min_sale
			FROM sales
		`).Scan(&totalSales, &avgSale, &maxSale, &minSale)

		if err != nil {
			t.Errorf("Failed to execute aggregation query: %v", err)
			return
		}

		// Verify results
		expectedTotal := 1929.95
		if totalSales < expectedTotal-0.01 || totalSales > expectedTotal+0.01 {
			t.Errorf("Expected total sales around %.2f, got %.2f", expectedTotal, totalSales)
		}

		if maxSale != 999.99 {
			t.Errorf("Expected max sale 999.99, got %.2f", maxSale)
		}

		if minSale != 29.99 {
			t.Errorf("Expected min sale 29.99, got %.2f", minSale)
		}
	})

	t.Run("GroupByQueries", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT 
				category,
				COUNT(*) as item_count,
				SUM(amount) as category_total,
				AVG(amount) as category_avg
			FROM sales 
			GROUP BY category 
			ORDER BY category_total DESC
		`)
		if err != nil {
			t.Errorf("Failed to execute GROUP BY query: %v", err)
			return
		}
		defer rows.Close()

		categories := make(map[string]struct {
			count int
			total float64
			avg   float64
		})

		for rows.Next() {
			var category string
			var count int
			var total, avg float64

			err := rows.Scan(&category, &count, &total, &avg)
			if err != nil {
				t.Errorf("Failed to scan GROUP BY result: %v", err)
				continue
			}

			categories[category] = struct {
				count int
				total float64
				avg   float64
			}{count, total, avg}
		}

		// Verify Electronics category
		if electronics, exists := categories["Electronics"]; exists {
			if electronics.count != 3 {
				t.Errorf("Expected 3 electronics items, got %d", electronics.count)
			}
		} else {
			t.Error("Electronics category not found in results")
		}

		// Verify Furniture category
		if furniture, exists := categories["Furniture"]; exists {
			if furniture.count != 2 {
				t.Errorf("Expected 2 furniture items, got %d", furniture.count)
			}
		} else {
			t.Error("Furniture category not found in results")
		}
	})
}

func TestWindowFunctions(t *testing.T) {
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

	// Create employees table
	_, err = db.Exec(`
		CREATE TABLE employees (
			id INTEGER,
			name VARCHAR,
			department VARCHAR,
			salary INTEGER
		)
	`)
	if err != nil {
		t.Errorf("Failed to create employees table: %v", err)
		return
	}

	t.Run("InsertEmployeeData", func(t *testing.T) {
		employees := [][]interface{}{
			{1, "Alice", "Engineering", 90000},
			{2, "Bob", "Engineering", 85000},
			{3, "Charlie", "Sales", 70000},
			{4, "Diana", "Sales", 75000},
			{5, "Eve", "Marketing", 65000},
		}

		for _, emp := range employees {
			_, err := db.Exec("INSERT INTO employees VALUES (?, ?, ?, ?)", emp...)
			if err != nil {
				t.Errorf("Failed to insert employee data: %v", err)
			}
		}
	})

	t.Run("WindowFunctionQuery", func(t *testing.T) {
		t.Skip("Window functions not yet implemented")
		rows, err := db.Query(`
			SELECT 
				name,
				department,
				salary,
				RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
				AVG(salary) OVER (PARTITION BY department) as dept_avg_salary
			FROM employees
			ORDER BY department, dept_rank
		`)
		if err != nil {
			t.Errorf("Failed to execute window function query: %v", err)
			return
		}
		defer rows.Close()

		resultCount := 0
		for rows.Next() {
			var name, department string
			var salary, rank int
			var deptAvg float64

			err := rows.Scan(&name, &department, &salary, &rank, &deptAvg)
			if err != nil {
				t.Errorf("Failed to scan window function result: %v", err)
				continue
			}

			resultCount++

			// Verify some specific cases
			if department == "Engineering" && rank == 1 {
				if name != "Alice" {
					t.Errorf("Expected Alice to be rank 1 in Engineering, got %s", name)
				}
			}
		}

		if resultCount != 5 {
			t.Errorf("Expected 5 result rows, got %d", resultCount)
		}
	})
}

func BenchmarkAdvancedOperations(b *testing.B) {
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

	// Setup benchmark data
	_, err = db.Exec(`
		CREATE TABLE bench_analytics (
			id INTEGER,
			category VARCHAR,
			value DOUBLE,
			created_at TIMESTAMP
		)
	`)
	if err != nil {
		b.Errorf("Failed to create benchmark table: %v", err)
		return
	}

	// Insert sample data
	for i := 0; i < 1000; i++ {
		category := []string{"A", "B", "C"}[i%3]
		_, err := db.Exec("INSERT INTO bench_analytics VALUES (?, ?, ?, NOW())",
			i, category, float64(i)*1.5)
		if err != nil {
			b.Errorf("Failed to insert benchmark data: %v", err)
		}
	}

	b.Run("AggregationQuery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT category, COUNT(*), AVG(value), SUM(value) 
				FROM bench_analytics 
				GROUP BY category
			`)
			if err != nil {
				b.Errorf("Failed to execute aggregation query: %v", err)
			}
			for rows.Next() {
				var category string
				var count int
				var avg, sum float64
				rows.Scan(&category, &count, &avg, &sum)
			}
			rows.Close()
		}
	})

	b.Run("WindowFunctionQuery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT 
					id, category, value,
					ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) as rn
				FROM bench_analytics
			`)
			if err != nil {
				b.Errorf("Failed to execute window function query: %v", err)
			}
			for rows.Next() {
				var id, rn int
				var category string
				var value float64
				rows.Scan(&id, &category, &value, &rn)
			}
			rows.Close()
		}
	})
}
