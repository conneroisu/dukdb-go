package test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/connerohnesorge/dukdb-go"
)

func BenchmarkConnectionCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			b.Fatal(err)
		}
		db.Close()
	}
}

func BenchmarkSimpleQueryDB(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT 1")
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkPreparedQuery(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := stmt.Query(i)
		if err != nil {
			b.Fatal(err)
		}
		rows.Close()
	}
}

func BenchmarkInsertSingle(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench_insert (id INTEGER, value VARCHAR)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec("INSERT INTO bench_insert VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertPrepared(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench_insert_prep (id INTEGER, value VARCHAR)")
	stmt, err := db.Prepare("INSERT INTO bench_insert_prep VALUES (?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stmt.Exec(i, fmt.Sprintf("value_%d", i))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsertBatch(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	batchSizes := []int{10, 100, 1000, 10000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize%d", batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				tableName := fmt.Sprintf("bench_batch_%d_%d", batchSize, i)
				db.Exec(fmt.Sprintf("CREATE TABLE %s (id INTEGER, value VARCHAR)", tableName))

				// Build batch insert query
				query := fmt.Sprintf("INSERT INTO %s VALUES ", tableName)
				values := make([]interface{}, 0, batchSize*2)

				for j := 0; j < batchSize; j++ {
					if j > 0 {
						query += ", "
					}
					query += "(?, ?)"
					values = append(values, j, fmt.Sprintf("value_%d", j))
				}

				_, err := db.Exec(query, values...)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSelectRange(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Setup test data
	db.Exec("CREATE TABLE bench_select (id INTEGER, value VARCHAR, amount DOUBLE)")

	// Insert test data
	for i := 0; i < 10000; i++ {
		db.Exec("INSERT INTO bench_select VALUES (?, ?, ?)",
			i, fmt.Sprintf("value_%d", i), float64(i)*1.5)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT id, value, amount FROM bench_select WHERE id BETWEEN ? AND ?",
			i%1000, (i%1000)+100)
		if err != nil {
			b.Fatal(err)
		}

		// Consume results
		for rows.Next() {
			var id int
			var value string
			var amount float64
			rows.Scan(&id, &value, &amount)
		}
		rows.Close()
	}
}

func BenchmarkAggregateQuery(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Setup test data
	db.Exec("CREATE TABLE bench_agg (category VARCHAR, amount DOUBLE)")

	categories := []string{"A", "B", "C", "D", "E"}
	for i := 0; i < 10000; i++ {
		category := categories[i%len(categories)]
		amount := float64(i % 1000)
		db.Exec("INSERT INTO bench_agg VALUES (?, ?)", category, amount)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`
			SELECT category, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount)
			FROM bench_agg 
			GROUP BY category
			ORDER BY category
		`)
		if err != nil {
			b.Fatal(err)
		}

		// Consume results
		for rows.Next() {
			var category string
			var count int
			var sum, avg, min, max float64
			rows.Scan(&category, &count, &sum, &avg, &min, &max)
		}
		rows.Close()
	}
}

func BenchmarkComplexJoin(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Setup test data
	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR)")
	db.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount DOUBLE)")

	// Insert users
	for i := 0; i < 1000; i++ {
		db.Exec("INSERT INTO users VALUES (?, ?)", i, fmt.Sprintf("User_%d", i))
	}

	// Insert orders
	for i := 0; i < 5000; i++ {
		userID := i % 1000
		amount := float64(i%100) * 10.5
		db.Exec("INSERT INTO orders VALUES (?, ?, ?)", i, userID, amount)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`
			SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
			FROM users u
			LEFT JOIN orders o ON u.id = o.user_id
			GROUP BY u.id, u.name
			HAVING COUNT(o.id) > 3
			ORDER BY total_amount DESC
			LIMIT 10
		`)
		if err != nil {
			b.Fatal(err)
		}

		// Consume results
		for rows.Next() {
			var name string
			var orderCount int
			var totalAmount float64
			rows.Scan(&name, &orderCount, &totalAmount)
		}
		rows.Close()
	}
}

func BenchmarkTransactionCommit(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench_tx (id INTEGER, value VARCHAR)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}

		tx.Exec("INSERT INTO bench_tx VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))

		err = tx.Commit()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTransactionRollback(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench_tx_rollback (id INTEGER, value VARCHAR)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}

		tx.Exec("INSERT INTO bench_tx_rollback VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))

		err = tx.Rollback()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDateTimeOperations(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench_datetime (id INTEGER, created_at TIMESTAMP, event_date DATE)")

	now := time.Now()
	for i := 0; i < 1000; i++ {
		timestamp := now.Add(time.Duration(i) * time.Hour)
		date := timestamp.Truncate(24 * time.Hour)
		db.Exec("INSERT INTO bench_datetime VALUES (?, ?, ?)", i, timestamp, date)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`
			SELECT 
				id,
				EXTRACT(hour FROM created_at) as hour,
				EXTRACT(day FROM event_date) as day,
				created_at + INTERVAL 1 DAY as tomorrow
			FROM bench_datetime
			WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
			ORDER BY created_at
		`)
		if err != nil {
			b.Fatal(err)
		}

		for rows.Next() {
			var id, hour, day int
			var tomorrow time.Time
			rows.Scan(&id, &hour, &day, &tomorrow)
		}
		rows.Close()
	}
}

func BenchmarkComplexTypes(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec(`
		CREATE TABLE bench_complex (
			id INTEGER,
			tags VARCHAR[],
			metadata MAP(VARCHAR, VARCHAR),
			config STRUCT(enabled BOOLEAN, priority INTEGER)
		)
	`)

	for i := 0; i < 1000; i++ {
		db.Exec(`
			INSERT INTO bench_complex VALUES (
				?,
				['tag1', 'tag2', 'tag3'],
				MAP(['key1', 'key2'], ['value1', 'value2']),
				{'enabled': true, 'priority': ?}
			)
		`, i, i%10)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(`
			SELECT 
				id,
				array_length(tags) as tag_count,
				metadata['key1'] as key_value,
				config.priority as priority
			FROM bench_complex
			WHERE list_contains(tags, 'tag1')
			AND config.enabled = true
		`)
		if err != nil {
			b.Fatal(err)
		}

		for rows.Next() {
			var id, tagCount, priority int
			var keyValue string
			rows.Scan(&id, &tagCount, &keyValue, &priority)
		}
		rows.Close()
	}
}

// Benchmark concurrent access
func BenchmarkConcurrentQueries(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Set up connection pool
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	db.Exec("CREATE TABLE bench_concurrent (id INTEGER, value VARCHAR)")
	for i := 0; i < 1000; i++ {
		db.Exec("INSERT INTO bench_concurrent VALUES (?, ?)", i, fmt.Sprintf("value_%d", i))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rows, err := db.Query("SELECT COUNT(*) FROM bench_concurrent WHERE id < ?", 500)
			if err != nil {
				b.Fatal(err)
			}

			for rows.Next() {
				var count int
				rows.Scan(&count)
			}
			rows.Close()
		}
	})
}

func BenchmarkMemoryUsage(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table with larger data
	db.Exec("CREATE TABLE bench_memory (id INTEGER, data VARCHAR)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Insert large strings
		largeData := fmt.Sprintf("%0*d", 1000, i) // 1KB string
		db.Exec("INSERT INTO bench_memory VALUES (?, ?)", i, largeData)

		// Query it back
		var retrievedData string
		db.QueryRow("SELECT data FROM bench_memory WHERE id = ?", i).Scan(&retrievedData)

		// Clean up
		db.Exec("DELETE FROM bench_memory WHERE id = ?", i)
	}
}
