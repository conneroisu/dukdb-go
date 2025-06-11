package analytical

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
	
	"github.com/connerohnesorge/dukdb-go/benchmarks"
)

// Benchmark simple aggregation queries
func BenchmarkSimpleAggregation(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupAnalyticsTable(b, db, 10000)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var count int
			var sum, avg, min, max float64
			
			err := db.QueryRow(`
				SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount)
				FROM analytics_table
				WHERE category = ?
			`, "Category_A").Scan(&count, &sum, &avg, &min, &max)
			
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark GROUP BY with multiple aggregations
func BenchmarkGroupByAggregation(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupAnalyticsTable(b, db, 50000)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT 
					category,
					COUNT(*) as count,
					SUM(amount) as total,
					AVG(amount) as average,
					MIN(amount) as min_amount,
					MAX(amount) as max_amount,
					STDDEV(amount) as std_dev
				FROM analytics_table
				GROUP BY category
				ORDER BY total DESC
			`)
			if err != nil {
				b.Fatal(err)
			}
			
			// Consume results
			for rows.Next() {
				var category string
				var count int
				var total, average, minAmount, maxAmount, stdDev float64
				
				err := rows.Scan(&category, &count, &total, &average, &minAmount, &maxAmount, &stdDev)
				if err != nil {
					b.Fatal(err)
				}
			}
			rows.Close()
		}
	})
}

// Benchmark HAVING clause
func BenchmarkHavingClause(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupAnalyticsTable(b, db, 20000)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT 
					category,
					region,
					COUNT(*) as count,
					SUM(amount) as total
				FROM analytics_table
				GROUP BY category, region
				HAVING COUNT(*) > 100 AND SUM(amount) > 50000
				ORDER BY total DESC
			`)
			if err != nil {
				b.Fatal(err)
			}
			
			// Consume results
			count := 0
			for rows.Next() {
				var category, region string
				var rowCount int
				var total float64
				
				err := rows.Scan(&category, &region, &rowCount, &total)
				if err != nil {
					b.Fatal(err)
				}
				count++
			}
			rows.Close()
		}
	})
}

// Benchmark simple JOIN operations
func BenchmarkSimpleJoin(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupJoinTables(b, db)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT 
					o.id,
					o.amount,
					c.name,
					c.email
				FROM orders o
				JOIN customers c ON o.customer_id = c.id
				WHERE o.amount > ?
				LIMIT 100
			`, 100.0)
			if err != nil {
				b.Fatal(err)
			}
			
			// Consume results
			for rows.Next() {
				var orderId int
				var amount float64
				var name, email string
				
				err := rows.Scan(&orderId, &amount, &name, &email)
				if err != nil {
					b.Fatal(err)
				}
			}
			rows.Close()
		}
	})
}

// Benchmark complex JOIN with aggregations
func BenchmarkComplexJoinAggregation(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupJoinTables(b, db)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT 
					c.id,
					c.name,
					c.region,
					COUNT(o.id) as order_count,
					SUM(o.amount) as total_spent,
					AVG(o.amount) as avg_order,
					MAX(o.order_date) as last_order
				FROM customers c
				LEFT JOIN orders o ON c.id = o.customer_id
				GROUP BY c.id, c.name, c.region
				HAVING COUNT(o.id) > 0
				ORDER BY total_spent DESC
				LIMIT 50
			`)
			if err != nil {
				b.Fatal(err)
			}
			
			// Consume results
			for rows.Next() {
				var customerId int
				var name, region string
				var orderCount int
				var totalSpent, avgOrder float64
				var lastOrder time.Time
				
				err := rows.Scan(&customerId, &name, &region, &orderCount, &totalSpent, &avgOrder, &lastOrder)
				if err != nil {
					b.Fatal(err)
				}
			}
			rows.Close()
		}
	})
}

// Benchmark multi-table JOIN
func BenchmarkMultiTableJoin(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupJoinTables(b, db)
		setupProductTables(b, db)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT 
					c.name as customer_name,
					p.name as product_name,
					oi.quantity,
					oi.unit_price,
					oi.quantity * oi.unit_price as line_total
				FROM customers c
				JOIN orders o ON c.id = o.customer_id
				JOIN order_items oi ON o.id = oi.order_id
				JOIN products p ON oi.product_id = p.id
				WHERE c.region = ? AND p.category = ?
				ORDER BY line_total DESC
				LIMIT 100
			`, "North", "Electronics")
			if err != nil {
				b.Fatal(err)
			}
			
			// Consume results
			for rows.Next() {
				var customerName, productName string
				var quantity int
				var unitPrice, lineTotal float64
				
				err := rows.Scan(&customerName, &productName, &quantity, &unitPrice, &lineTotal)
				if err != nil {
					b.Fatal(err)
				}
			}
			rows.Close()
		}
	})
}

// Benchmark window functions (if supported)
func BenchmarkWindowFunctions(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupAnalyticsTable(b, db, 10000)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT 
					id,
					category,
					amount,
					ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) as row_num,
					RANK() OVER (PARTITION BY category ORDER BY amount DESC) as rank,
					SUM(amount) OVER (PARTITION BY category) as category_total,
					AVG(amount) OVER (PARTITION BY category) as category_avg,
					amount - LAG(amount) OVER (PARTITION BY category ORDER BY created_at) as diff_from_prev
				FROM analytics_table
				WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
				ORDER BY category, amount DESC
				LIMIT 1000
			`)
			if err != nil {
				// Window functions might not be supported in early implementation
				if implName == "PureGo" {
					b.Skip("Window functions not yet implemented in pure-Go version")
					return
				}
				b.Fatal(err)
			}
			
			// Consume results
			for rows.Next() {
				var id int
				var category string
				var amount, categoryTotal, categoryAvg float64
				var rowNum, rank int
				var diffFromPrev sql.NullFloat64
				
				err := rows.Scan(&id, &category, &amount, &rowNum, &rank, &categoryTotal, &categoryAvg, &diffFromPrev)
				if err != nil {
					b.Fatal(err)
				}
			}
			rows.Close()
		}
	})
}

// Benchmark CTEs (Common Table Expressions)
func BenchmarkCTE(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupJoinTables(b, db)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				WITH customer_stats AS (
					SELECT 
						c.id,
						c.name,
						COUNT(o.id) as order_count,
						SUM(o.amount) as total_spent
					FROM customers c
					LEFT JOIN orders o ON c.id = o.customer_id
					GROUP BY c.id, c.name
				),
				top_customers AS (
					SELECT * FROM customer_stats
					WHERE total_spent > 1000
					ORDER BY total_spent DESC
					LIMIT 100
				)
				SELECT 
					tc.*,
					o.id as order_id,
					o.amount,
					o.order_date
				FROM top_customers tc
				JOIN orders o ON tc.id = o.customer_id
				ORDER BY tc.total_spent DESC, o.order_date DESC
			`)
			if err != nil {
				// CTEs might not be supported in early implementation
				if implName == "PureGo" {
					b.Skip("CTEs not yet implemented in pure-Go version")
					return
				}
				b.Fatal(err)
			}
			
			// Consume results
			for rows.Next() {
				var customerId, orderId int
				var name string
				var orderCount int
				var totalSpent, amount float64
				var orderDate time.Time
				
				err := rows.Scan(&customerId, &name, &orderCount, &totalSpent, &orderId, &amount, &orderDate)
				if err != nil {
					b.Fatal(err)
				}
			}
			rows.Close()
		}
	})
}

// Benchmark UNION operations
func BenchmarkUnion(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupAnalyticsTable(b, db, 5000)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT 'High Value' as type, id, amount 
				FROM analytics_table 
				WHERE amount > 1000
				UNION ALL
				SELECT 'Medium Value' as type, id, amount 
				FROM analytics_table 
				WHERE amount BETWEEN 100 AND 1000
				UNION ALL
				SELECT 'Low Value' as type, id, amount 
				FROM analytics_table 
				WHERE amount < 100
				ORDER BY amount DESC
				LIMIT 500
			`)
			if err != nil {
				b.Fatal(err)
			}
			
			// Consume results
			for rows.Next() {
				var valueType string
				var id int
				var amount float64
				
				err := rows.Scan(&valueType, &id, &amount)
				if err != nil {
					b.Fatal(err)
				}
			}
			rows.Close()
		}
	})
}

// Benchmark subqueries
func BenchmarkSubqueries(b *testing.B) {
	runBothImplementations(b, func(b *testing.B, db *sql.DB, implName string) {
		// Setup
		setupJoinTables(b, db)
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(`
				SELECT 
					c.id,
					c.name,
					(SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) as order_count,
					(SELECT SUM(amount) FROM orders o WHERE o.customer_id = c.id) as total_spent,
					(SELECT MAX(order_date) FROM orders o WHERE o.customer_id = c.id) as last_order
				FROM customers c
				WHERE EXISTS (
					SELECT 1 FROM orders o 
					WHERE o.customer_id = c.id 
					AND o.amount > 100
				)
				ORDER BY total_spent DESC
				LIMIT 50
			`)
			if err != nil {
				b.Fatal(err)
			}
			
			// Consume results
			for rows.Next() {
				var customerId int
				var name string
				var orderCount sql.NullInt64
				var totalSpent sql.NullFloat64
				var lastOrder sql.NullTime
				
				err := rows.Scan(&customerId, &name, &orderCount, &totalSpent, &lastOrder)
				if err != nil {
					b.Fatal(err)
				}
			}
			rows.Close()
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

func setupAnalyticsTable(b *testing.B, db *sql.DB, rowCount int) {
	// Drop table if exists
	db.Exec("DROP TABLE IF EXISTS analytics_table")
	
	// Create table
	_, err := db.Exec(`
		CREATE TABLE analytics_table (
			id INTEGER PRIMARY KEY,
			category VARCHAR(50),
			region VARCHAR(50),
			amount DOUBLE,
			quantity INTEGER,
			created_at TIMESTAMP
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
	
	stmt, err := tx.Prepare(`
		INSERT INTO analytics_table (id, category, region, amount, quantity, created_at) 
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		b.Fatal(err)
	}
	
	categories := []string{"Category_A", "Category_B", "Category_C", "Category_D", "Category_E"}
	regions := []string{"North", "South", "East", "West", "Central"}
	baseTime := time.Now().Add(-30 * 24 * time.Hour) // 30 days ago
	
	for i := 0; i < rowCount; i++ {
		category := categories[i%len(categories)]
		region := regions[i%len(regions)]
		amount := float64(i%1000) * 1.5 + float64(i%100)
		quantity := (i % 50) + 1
		createdAt := baseTime.Add(time.Duration(i) * time.Minute)
		
		_, err := stmt.Exec(i, category, region, amount, quantity, createdAt)
		if err != nil {
			b.Fatal(err)
		}
	}
	
	stmt.Close()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

func setupJoinTables(b *testing.B, db *sql.DB) {
	// Create customers table
	db.Exec("DROP TABLE IF EXISTS customers")
	_, err := db.Exec(`
		CREATE TABLE customers (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			email VARCHAR(100),
			region VARCHAR(50)
		)
	`)
	if err != nil {
		b.Fatal(err)
	}
	
	// Create orders table
	db.Exec("DROP TABLE IF EXISTS orders")
	_, err = db.Exec(`
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			amount DOUBLE,
			order_date TIMESTAMP
		)
	`)
	if err != nil {
		b.Fatal(err)
	}
	
	// Insert customers
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	
	regions := []string{"North", "South", "East", "West", "Central"}
	for i := 0; i < 1000; i++ {
		_, err := tx.Exec(
			"INSERT INTO customers (id, name, email, region) VALUES (?, ?, ?, ?)",
			i, fmt.Sprintf("Customer_%d", i), fmt.Sprintf("customer%d@example.com", i), regions[i%len(regions)],
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	
	// Insert orders
	baseTime := time.Now().Add(-365 * 24 * time.Hour) // 1 year ago
	for i := 0; i < 10000; i++ {
		customerId := i % 1000
		amount := float64(i%500) * 10.5 + float64(i%100)
		orderDate := baseTime.Add(time.Duration(i) * time.Hour)
		
		_, err := tx.Exec(
			"INSERT INTO orders (id, customer_id, amount, order_date) VALUES (?, ?, ?, ?)",
			i, customerId, amount, orderDate,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

func setupProductTables(b *testing.B, db *sql.DB) {
	// Create products table
	db.Exec("DROP TABLE IF EXISTS products")
	_, err := db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name VARCHAR(100),
			category VARCHAR(50),
			price DOUBLE
		)
	`)
	if err != nil {
		b.Fatal(err)
	}
	
	// Create order_items table
	db.Exec("DROP TABLE IF EXISTS order_items")
	_, err = db.Exec(`
		CREATE TABLE order_items (
			id INTEGER PRIMARY KEY,
			order_id INTEGER,
			product_id INTEGER,
			quantity INTEGER,
			unit_price DOUBLE
		)
	`)
	if err != nil {
		b.Fatal(err)
	}
	
	// Insert products
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	
	categories := []string{"Electronics", "Clothing", "Food", "Books", "Toys"}
	for i := 0; i < 500; i++ {
		category := categories[i%len(categories)]
		price := float64(i%100) * 5.99 + 9.99
		
		_, err := tx.Exec(
			"INSERT INTO products (id, name, category, price) VALUES (?, ?, ?, ?)",
			i, fmt.Sprintf("Product_%d", i), category, price,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	
	// Insert order items
	for i := 0; i < 50000; i++ {
		orderId := i % 10000
		productId := i % 500
		quantity := (i % 10) + 1
		unitPrice := float64(i%100) * 5.99 + 9.99
		
		_, err := tx.Exec(
			"INSERT INTO order_items (id, order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?, ?)",
			i, orderId, productId, quantity, unitPrice,
		)
		if err != nil {
			b.Fatal(err)
		}
	}
	
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}