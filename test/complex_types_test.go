package test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	_ "github.com/connerohnesorge/dukdb-go"
)

func TestUUIDType(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with UUID
	_, err = db.Exec(`
		CREATE TABLE uuid_test (
			id UUID PRIMARY KEY,
			name VARCHAR
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test UUID values
	testUUIDs := []string{
		"550e8400-e29b-41d4-a716-446655440000",
		"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		"00000000-0000-0000-0000-000000000000",
	}

	// Insert UUIDs
	for i, uuid := range testUUIDs {
		_, err = db.Exec("INSERT INTO uuid_test VALUES (?, ?)", uuid, fmt.Sprintf("Item %d", i))
		if err != nil {
			t.Fatalf("Failed to insert UUID: %v", err)
		}
	}

	// Query back
	rows, err := db.Query("SELECT id, name FROM uuid_test ORDER BY name")
	if err != nil {
		t.Fatalf("Failed to query UUIDs: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var id string
		var name string
		err := rows.Scan(&id, &name)
		if err != nil {
			t.Fatalf("Failed to scan UUID: %v", err)
		}
		t.Logf("UUID: %s, Name: %s", id, name)
		i++
	}

	if i != len(testUUIDs) {
		t.Errorf("Expected %d rows, got %d", len(testUUIDs), i)
	}
}

func TestListType(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with LIST
	_, err = db.Exec(`
		CREATE TABLE list_test (
			id INTEGER,
			tags VARCHAR[],
			numbers INTEGER[]
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert lists using DuckDB syntax
	_, err = db.Exec(`
		INSERT INTO list_test VALUES 
			(1, ['tag1', 'tag2', 'tag3'], [1, 2, 3]),
			(2, ['hello', 'world'], [10, 20]),
			(3, [], [])
	`)
	if err != nil {
		t.Fatalf("Failed to insert lists: %v", err)
	}

	// Query back
	rows, err := db.Query("SELECT id, tags, numbers FROM list_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query lists: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var tags, numbers string // Get as JSON strings for now
		err := rows.Scan(&id, &tags, &numbers)
		if err != nil {
			t.Fatalf("Failed to scan list: %v", err)
		}
		t.Logf("ID: %d, Tags: %s, Numbers: %s", id, tags, numbers)

		// Verify we can parse as JSON
		var tagList []string
		if tags != "[]" && tags != "" {
			if err := json.Unmarshal([]byte(tags), &tagList); err != nil {
				t.Errorf("Failed to parse tags as JSON: %v", err)
			}
		}

		var numList []int
		if numbers != "[]" && numbers != "" {
			if err := json.Unmarshal([]byte(numbers), &numList); err != nil {
				t.Errorf("Failed to parse numbers as JSON: %v", err)
			}
		}
	}
}

func TestStructType(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with STRUCT
	_, err = db.Exec(`
		CREATE TABLE struct_test (
			id INTEGER,
			person STRUCT(name VARCHAR, age INTEGER, email VARCHAR)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert structs
	_, err = db.Exec(`
		INSERT INTO struct_test VALUES 
			(1, {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'}),
			(2, {'name': 'Bob', 'age': 25, 'email': 'bob@example.com'}),
			(3, NULL)
	`)
	if err != nil {
		t.Fatalf("Failed to insert structs: %v", err)
	}

	// Query back
	rows, err := db.Query("SELECT id, person FROM struct_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query structs: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var person sql.NullString
		err := rows.Scan(&id, &person)
		if err != nil {
			t.Fatalf("Failed to scan struct: %v", err)
		}

		if person.Valid {
			t.Logf("ID: %d, Person: %s", id, person.String)

			// Verify we can parse as JSON
			var p map[string]interface{}
			if err := json.Unmarshal([]byte(person.String), &p); err != nil {
				t.Errorf("Failed to parse person as JSON: %v", err)
			}
		} else {
			t.Logf("ID: %d, Person: NULL", id)
		}
	}
}

func TestMapType(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with MAP
	_, err = db.Exec(`
		CREATE TABLE map_test (
			id INTEGER,
			properties MAP(VARCHAR, VARCHAR)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert maps
	_, err = db.Exec(`
		INSERT INTO map_test VALUES 
			(1, MAP(['key1', 'key2'], ['value1', 'value2'])),
			(2, MAP(['color', 'size'], ['red', 'large'])),
			(3, MAP([], []))
	`)
	if err != nil {
		t.Fatalf("Failed to insert maps: %v", err)
	}

	// Query back
	rows, err := db.Query("SELECT id, properties FROM map_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query maps: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var properties string // Get as JSON string for now
		err := rows.Scan(&id, &properties)
		if err != nil {
			t.Fatalf("Failed to scan map: %v", err)
		}
		t.Logf("ID: %d, Properties: %s", id, properties)

		// Verify we can parse as JSON
		if properties != "{}" && properties != "" {
			var m map[string]interface{}
			if err := json.Unmarshal([]byte(properties), &m); err != nil {
				t.Errorf("Failed to parse properties as JSON: %v", err)
			}
		}
	}
}

func TestNestedComplexTypes(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with nested types
	_, err = db.Exec(`
		CREATE TABLE nested_test (
			id INTEGER,
			data STRUCT(
				items VARCHAR[],
				metadata MAP(VARCHAR, VARCHAR)
			)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table with nested types: %v", err)
	}

	// Insert nested data
	_, err = db.Exec(`
		INSERT INTO nested_test VALUES 
			(1, {
				'items': ['item1', 'item2'], 
				'metadata': MAP(['type', 'status'], ['test', 'active'])
			})
	`)
	if err != nil {
		t.Fatalf("Failed to insert nested data: %v", err)
	}

	// Query back
	var id int
	var data string
	err = db.QueryRow("SELECT id, data FROM nested_test WHERE id = 1").Scan(&id, &data)
	if err != nil {
		t.Fatalf("Failed to query nested data: %v", err)
	}

	t.Logf("Nested data: %s", data)

	// Verify structure
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(data), &parsed); err != nil {
		t.Errorf("Failed to parse nested data as JSON: %v", err)
	}
}

func TestComplexTypeQueries(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER,
			name VARCHAR,
			tags VARCHAR[],
			attributes MAP(VARCHAR, VARCHAR)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(`
		INSERT INTO products VALUES 
			(1, 'Laptop', ['electronics', 'computers'], MAP(['brand', 'cpu'], ['Dell', 'Intel'])),
			(2, 'Phone', ['electronics', 'mobile'], MAP(['brand', 'os'], ['Apple', 'iOS'])),
			(3, 'Desk', ['furniture', 'office'], MAP(['material', 'color'], ['wood', 'brown']))
	`)
	if err != nil {
		t.Fatalf("Failed to insert products: %v", err)
	}

	// Test querying with list contains
	t.Run("ListContains", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT name 
			FROM products 
			WHERE list_contains(tags, 'electronics')
			ORDER BY name
		`)
		if err != nil {
			t.Fatalf("Failed to query with list_contains: %v", err)
		}
		defer rows.Close()

		count := 0
		for rows.Next() {
			var name string
			rows.Scan(&name)
			t.Logf("Electronics product: %s", name)
			count++
		}

		if count != 2 {
			t.Errorf("Expected 2 electronics products, got %d", count)
		}
	})

	// Test extracting from maps
	t.Run("MapExtract", func(t *testing.T) {
		rows, err := db.Query(`
			SELECT name, attributes['brand'] as brand
			FROM products 
			WHERE attributes['brand'] IS NOT NULL
			ORDER BY name
		`)
		if err != nil {
			t.Fatalf("Failed to query map values: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var name, brand string
			rows.Scan(&name, &brand)
			t.Logf("Product: %s, Brand: %s", name, brand)
		}
	})
}
