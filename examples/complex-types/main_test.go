package main

import (
	"database/sql"
	"testing"

	_ "github.com/connerohnesorge/dukdb-go"
	"github.com/connerohnesorge/dukdb-go/internal/types"
)

func TestUUIDOperations(t *testing.T) {
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

	// Create table with UUID column
	_, err = db.Exec(`CREATE TABLE users (id UUID, name VARCHAR, email VARCHAR)`)
	if err != nil {
		t.Errorf("Failed to create table with UUID: %v", err)
		return
	}

	t.Run("InsertUUID", func(t *testing.T) {
		// Create UUIDs
		uuid1 := types.NewUUID()
		uuid2 := types.NewUUID()

		_, err := db.Exec("INSERT INTO users VALUES (?, ?, ?)", uuid1, "Alice", "alice@example.com")
		if err != nil {
			t.Errorf("Failed to insert UUID: %v", err)
		}

		_, err = db.Exec("INSERT INTO users VALUES (?, ?, ?)", uuid2, "Bob", "bob@example.com")
		if err != nil {
			t.Errorf("Failed to insert second UUID: %v", err)
		}
	})

	t.Run("QueryUUID", func(t *testing.T) {
		rows, err := db.Query("SELECT id, name, email FROM users ORDER BY name")
		if err != nil {
			t.Errorf("Failed to query UUIDs: %v", err)
			return
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id types.UUID
			var name, email string

			err := rows.Scan(&id, &name, &email)
			if err != nil {
				t.Errorf("Failed to scan UUID row: %v", err)
				continue
			}

			// Verify UUID is valid
			if !id.IsValid() {
				t.Errorf("Invalid UUID: %s", id.String())
			}

			count++
		}

		if count != 2 {
			t.Errorf("Expected 2 UUID rows, got %d", count)
		}
	})

	t.Run("UUIDComparison", func(t *testing.T) {
		// Test UUID-specific operations
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM users WHERE id IS NOT NULL").Scan(&count)
		if err != nil {
			t.Errorf("Failed to count non-null UUIDs: %v", err)
		}

		if count != 2 {
			t.Errorf("Expected 2 non-null UUIDs, got %d", count)
		}
	})
}

func TestListOperations(t *testing.T) {
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

	// Create table with LIST column
	_, err = db.Exec(`CREATE TABLE products (id INTEGER, name VARCHAR, tags VARCHAR[])`)
	if err != nil {
		t.Errorf("Failed to create table with LIST: %v", err)
		return
	}

	t.Run("InsertList", func(t *testing.T) {
		// Insert data with array literals
		_, err := db.Exec(`
			INSERT INTO products VALUES 
			(1, 'Laptop', ['electronics', 'computer', 'portable']),
			(2, 'Desk', ['furniture', 'office', 'wood']),
			(3, 'Phone', ['electronics', 'mobile', 'communication'])
		`)
		if err != nil {
			t.Errorf("Failed to insert LIST data: %v", err)
		}
	})

	t.Run("QueryList", func(t *testing.T) {
		// Test array functions
		rows, err := db.Query(`
			SELECT 
				name,
				tags,
				array_length(tags) as tag_count,
				tags[1] as first_tag
			FROM products 
			WHERE list_contains(tags, 'electronics')
			ORDER BY name
		`)
		if err != nil {
			t.Errorf("Failed to query LIST data: %v", err)
			return
		}
		defer rows.Close()

		electronicsProducts := 0
		for rows.Next() {
			var name string
			var tags types.List
			var tagCount int
			var firstTag string

			err := rows.Scan(&name, &tags, &tagCount, &firstTag)
			if err != nil {
				t.Errorf("Failed to scan LIST row: %v", err)
				continue
			}

			if tagCount <= 0 {
				t.Errorf("Expected positive tag count, got %d", tagCount)
			}

			if firstTag == "" {
				t.Errorf("Expected non-empty first tag")
			}

			electronicsProducts++
		}

		if electronicsProducts != 2 {
			t.Errorf("Expected 2 electronics products, got %d", electronicsProducts)
		}
	})

	t.Run("ListAggregation", func(t *testing.T) {
		// Test list aggregation functions
		var totalTags int
		err := db.QueryRow(`
			SELECT SUM(array_length(tags)) as total_tags 
			FROM products
		`).Scan(&totalTags)
		
		if err != nil {
			t.Errorf("Failed to aggregate LIST data: %v", err)
		}

		if totalTags != 9 { // 3 + 3 + 3 tags total
			t.Errorf("Expected 9 total tags, got %d", totalTags)
		}
	})
}

func TestStructOperations(t *testing.T) {
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

	// Create table with STRUCT column
	_, err = db.Exec(`
		CREATE TABLE employees (
			id INTEGER,
			name VARCHAR,
			address STRUCT(street VARCHAR, city VARCHAR, zipcode VARCHAR),
			salary_info STRUCT(base INTEGER, bonus INTEGER, currency VARCHAR)
		)
	`)
	if err != nil {
		t.Errorf("Failed to create table with STRUCT: %v", err)
		return
	}

	t.Run("InsertStruct", func(t *testing.T) {
		_, err := db.Exec(`
			INSERT INTO employees VALUES 
			(
				1, 
				'Alice Johnson',
				{'street': '123 Main St', 'city': 'New York', 'zipcode': '10001'},
				{'base': 75000, 'bonus': 5000, 'currency': 'USD'}
			),
			(
				2,
				'Bob Smith', 
				{'street': '456 Oak Ave', 'city': 'San Francisco', 'zipcode': '94102'},
				{'base': 85000, 'bonus': 7500, 'currency': 'USD'}
			)
		`)
		if err != nil {
			t.Errorf("Failed to insert STRUCT data: %v", err)
		}
	})

	t.Run("QueryStruct", func(t *testing.T) {
		// Test struct field access
		rows, err := db.Query(`
			SELECT 
				name,
				address,
				address.city,
				salary_info.base,
				salary_info.base + salary_info.bonus as total_comp
			FROM employees
			ORDER BY salary_info.base
		`)
		if err != nil {
			t.Errorf("Failed to query STRUCT data: %v", err)
			return
		}
		defer rows.Close()

		employeeCount := 0
		for rows.Next() {
			var name string
			var address types.Struct
			var city string
			var baseSalary int
			var totalComp int

			err := rows.Scan(&name, &address, &city, &baseSalary, &totalComp)
			if err != nil {
				t.Errorf("Failed to scan STRUCT row: %v", err)
				continue
			}

			if city == "" {
				t.Errorf("Expected non-empty city")
			}

			if baseSalary <= 0 {
				t.Errorf("Expected positive base salary, got %d", baseSalary)
			}

			if totalComp <= baseSalary {
				t.Errorf("Expected total compensation > base salary")
			}

			employeeCount++
		}

		if employeeCount != 2 {
			t.Errorf("Expected 2 employees, got %d", employeeCount)
		}
	})

	t.Run("StructAggregation", func(t *testing.T) {
		// Test aggregation on struct fields
		var avgSalary float64
		err := db.QueryRow(`
			SELECT AVG(salary_info.base) 
			FROM employees
		`).Scan(&avgSalary)
		
		if err != nil {
			t.Errorf("Failed to aggregate STRUCT field: %v", err)
		}

		expectedAvg := 80000.0 // (75000 + 85000) / 2
		if avgSalary != expectedAvg {
			t.Errorf("Expected average salary %.2f, got %.2f", expectedAvg, avgSalary)
		}
	})
}

func TestMapOperations(t *testing.T) {
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

	// Create table with MAP column
	_, err = db.Exec(`
		CREATE TABLE user_settings (
			user_id INTEGER,
			username VARCHAR,
			preferences MAP(VARCHAR, VARCHAR),
			feature_flags MAP(VARCHAR, BOOLEAN)
		)
	`)
	if err != nil {
		t.Errorf("Failed to create table with MAP: %v", err)
		return
	}

	t.Run("InsertMap", func(t *testing.T) {
		_, err := db.Exec(`
			INSERT INTO user_settings VALUES 
			(
				1, 
				'alice',
				MAP(['theme', 'language', 'timezone'], ['dark', 'en', 'UTC']),
				MAP(['beta_features', 'notifications'], [true, false])
			),
			(
				2,
				'bob',
				MAP(['theme', 'language'], ['light', 'es']),
				MAP(['beta_features', 'notifications', 'analytics'], [false, true, true])
			)
		`)
		if err != nil {
			t.Errorf("Failed to insert MAP data: %v", err)
		}
	})

	t.Run("QueryMap", func(t *testing.T) {
		// Test map access
		rows, err := db.Query(`
			SELECT 
				username,
				preferences,
				preferences['theme'] as user_theme,
				feature_flags['beta_features'] as has_beta
			FROM user_settings
			ORDER BY user_id
		`)
		if err != nil {
			t.Errorf("Failed to query MAP data: %v", err)
			return
		}
		defer rows.Close()

		users := make(map[string]struct {
			theme    string
			hasBeta  bool
			prefs    types.Map
		})

		for rows.Next() {
			var username string
			var preferences types.Map
			var theme string
			var hasBeta bool

			err := rows.Scan(&username, &preferences, &theme, &hasBeta)
			if err != nil {
				t.Errorf("Failed to scan MAP row: %v", err)
				continue
			}

			users[username] = struct {
				theme    string
				hasBeta  bool
				prefs    types.Map
			}{theme, hasBeta, preferences}
		}

		// Verify specific users
		if alice, exists := users["alice"]; exists {
			if alice.theme != "dark" {
				t.Errorf("Expected Alice's theme to be 'dark', got '%s'", alice.theme)
			}
			if !alice.hasBeta {
				t.Errorf("Expected Alice to have beta features enabled")
			}
		} else {
			t.Error("Alice not found in results")
		}

		if bob, exists := users["bob"]; exists {
			if bob.theme != "light" {
				t.Errorf("Expected Bob's theme to be 'light', got '%s'", bob.theme)
			}
			if bob.hasBeta {
				t.Errorf("Expected Bob to have beta features disabled")
			}
		} else {
			t.Error("Bob not found in results")
		}
	})

	t.Run("MapFunctions", func(t *testing.T) {
		// Test map functions
		rows, err := db.Query(`
			SELECT 
				username,
				cardinality(preferences) as pref_count,
				map_keys(preferences) as pref_keys
			FROM user_settings
			ORDER BY user_id
		`)
		if err != nil {
			t.Errorf("Failed to query MAP functions: %v", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var username string
			var prefCount int
			var prefKeys types.List

			err := rows.Scan(&username, &prefCount, &prefKeys)
			if err != nil {
				t.Errorf("Failed to scan MAP function result: %v", err)
				continue
			}

			if prefCount <= 0 {
				t.Errorf("Expected positive preference count for %s, got %d", username, prefCount)
			}
		}
	})
}

func TestNestedComplexTypes(t *testing.T) {
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

	// Create table with nested complex types
	_, err = db.Exec(`
		CREATE TABLE orders (
			order_id UUID,
			customer_info STRUCT(
				name VARCHAR, 
				email VARCHAR, 
				addresses STRUCT(
					billing STRUCT(street VARCHAR, city VARCHAR), 
					shipping STRUCT(street VARCHAR, city VARCHAR)
				)
			),
			items STRUCT(
				product_ids INTEGER[],
				quantities INTEGER[],
				metadata MAP(VARCHAR, VARCHAR)
			)
		)
	`)
	if err != nil {
		t.Errorf("Failed to create table with nested complex types: %v", err)
		return
	}

	t.Run("InsertNestedData", func(t *testing.T) {
		orderID := types.NewUUID()
		
		_, err := db.Exec(`
			INSERT INTO orders VALUES (
				?,
				{
					'name': 'John Doe',
					'email': 'john@example.com',
					'addresses': {
						'billing': {'street': '123 Main St', 'city': 'New York'},
						'shipping': {'street': '456 Oak Ave', 'city': 'Boston'}
					}
				},
				{
					'product_ids': [101, 102, 103],
					'quantities': [1, 2, 1],
					'metadata': MAP(['priority', 'gift_wrap'], ['high', 'true'])
				}
			)
		`, orderID)
		
		if err != nil {
			t.Errorf("Failed to insert nested complex data: %v", err)
		}
	})

	t.Run("QueryNestedData", func(t *testing.T) {
		// Test deep nested field access
		rows, err := db.Query(`
			SELECT 
				order_id,
				customer_info.name,
				customer_info.addresses.billing.city as billing_city,
				customer_info.addresses.shipping.city as shipping_city,
				array_length(items.product_ids) as item_count,
				items.metadata['priority'] as priority
			FROM orders
		`)
		if err != nil {
			t.Errorf("Failed to query nested complex data: %v", err)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var orderID types.UUID
			var customerName string
			var billingCity, shippingCity string
			var itemCount int
			var priority string

			err := rows.Scan(&orderID, &customerName, &billingCity, &shippingCity, &itemCount, &priority)
			if err != nil {
				t.Errorf("Failed to scan nested complex row: %v", err)
				continue
			}

			if !orderID.IsValid() {
				t.Errorf("Invalid order UUID")
			}

			if customerName != "John Doe" {
				t.Errorf("Expected customer name 'John Doe', got '%s'", customerName)
			}

			if billingCity != "New York" {
				t.Errorf("Expected billing city 'New York', got '%s'", billingCity)
			}

			if shippingCity != "Boston" {
				t.Errorf("Expected shipping city 'Boston', got '%s'", shippingCity)
			}

			if itemCount != 3 {
				t.Errorf("Expected 3 items, got %d", itemCount)
			}

			if priority != "high" {
				t.Errorf("Expected priority 'high', got '%s'", priority)
			}
		}
	})
}

func BenchmarkComplexTypes(b *testing.B) {
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
		CREATE TABLE complex_bench (
			id UUID,
			data STRUCT(values INTEGER[], metadata MAP(VARCHAR, VARCHAR))
		)
	`)
	if err != nil {
		b.Errorf("Failed to create benchmark table: %v", err)
		return
	}

	b.Run("UUIDOperations", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			uuid := types.NewUUID()
			_, err := db.Exec("INSERT INTO complex_bench VALUES (?, {'values': [1,2,3], 'metadata': MAP(['key'], ['value'])})", uuid)
			if err != nil {
				b.Errorf("Failed to insert UUID: %v", err)
			}
		}
	})

	b.Run("ComplexTypeQuery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT id, data.values, data.metadata FROM complex_bench LIMIT 10")
			if err != nil {
				b.Errorf("Failed to query complex types: %v", err)
			}
			for rows.Next() {
				var id types.UUID
				var values types.List
				var metadata types.Map
				rows.Scan(&id, &values, &metadata)
			}
			rows.Close()
		}
	})
}