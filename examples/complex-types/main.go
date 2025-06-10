package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/connerohnesorge/dukdb-go"
)

func main() {
	// Open database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	fmt.Println("=== DuckDB Complex Types Example ===\n")

	// Example 1: UUID Type
	fmt.Println("1. UUID Type Example")
	demonstrateUUID(db)

	// Example 2: LIST Type
	fmt.Println("\n2. LIST Type Example")
	demonstrateLists(db)

	// Example 3: STRUCT Type
	fmt.Println("\n3. STRUCT Type Example")
	demonstrateStructs(db)

	// Example 4: MAP Type
	fmt.Println("\n4. MAP Type Example")
	demonstrateMaps(db)

	// Example 5: Nested Complex Types
	fmt.Println("\n5. Nested Complex Types Example")
	demonstrateNestedTypes(db)
}

func demonstrateUUID(db *sql.DB) {
	// Create table with UUID
	_, err := db.Exec(`
		CREATE TABLE users (
			id UUID PRIMARY KEY,
			username VARCHAR,
			created_at TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatal("Failed to create users table:", err)
	}

	// Insert users with UUIDs
	_, err = db.Exec(`
		INSERT INTO users VALUES 
			('550e8400-e29b-41d4-a716-446655440000', 'alice', CURRENT_TIMESTAMP),
			('6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'bob', CURRENT_TIMESTAMP),
			(gen_random_uuid(), 'charlie', CURRENT_TIMESTAMP)
	`)
	if err != nil {
		log.Fatal("Failed to insert users:", err)
	}

	// Query users
	rows, err := db.Query("SELECT id, username FROM users ORDER BY username")
	if err != nil {
		log.Fatal("Failed to query users:", err)
	}
	defer rows.Close()

	fmt.Println("Users:")
	for rows.Next() {
		var id, username string
		rows.Scan(&id, &username)
		fmt.Printf("  %s: %s\n", username, id)
	}
}

func demonstrateLists(db *sql.DB) {
	// Create table with lists
	_, err := db.Exec(`
		CREATE TABLE articles (
			id INTEGER,
			title VARCHAR,
			tags VARCHAR[],
			ratings INTEGER[]
		)
	`)
	if err != nil {
		log.Fatal("Failed to create articles table:", err)
	}

	// Insert articles with lists
	_, err = db.Exec(`
		INSERT INTO articles VALUES 
			(1, 'Getting Started with DuckDB', ['database', 'tutorial', 'beginner'], [5, 4, 5, 5]),
			(2, 'Advanced SQL Techniques', ['sql', 'advanced', 'performance'], [4, 5, 3]),
			(3, 'Data Analytics Guide', ['analytics', 'data-science'], [5, 5, 4, 5, 5])
	`)
	if err != nil {
		log.Fatal("Failed to insert articles:", err)
	}

	// Query with list operations
	fmt.Println("Articles with their tags:")
	rows, err := db.Query(`
		SELECT 
			title,
			tags,
			array_length(tags) as tag_count,
			list_avg(ratings) as avg_rating
		FROM articles
		ORDER BY id
	`)
	if err != nil {
		log.Fatal("Failed to query articles:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var title, tags string
		var tagCount int
		var avgRating float64
		rows.Scan(&title, &tags, &tagCount, &avgRating)

		// Parse tags JSON
		var tagList []string
		json.Unmarshal([]byte(tags), &tagList)

		fmt.Printf("  %s\n", title)
		fmt.Printf("    Tags (%d): %v\n", tagCount, tagList)
		fmt.Printf("    Average Rating: %.2f\n", avgRating)
	}

	// Find articles with specific tag
	fmt.Println("\nArticles tagged with 'tutorial':")
	rows2, err := db.Query(`
		SELECT title 
		FROM articles 
		WHERE list_contains(tags, 'tutorial')
	`)
	if err != nil {
		log.Fatal("Failed to query by tag:", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var title string
		rows2.Scan(&title)
		fmt.Printf("  - %s\n", title)
	}
}

func demonstrateStructs(db *sql.DB) {
	// Create table with structs
	_, err := db.Exec(`
		CREATE TABLE employees (
			id INTEGER,
			name VARCHAR,
			contact STRUCT(
				email VARCHAR,
				phone VARCHAR,
				address STRUCT(
					street VARCHAR,
					city VARCHAR,
					country VARCHAR
				)
			)
		)
	`)
	if err != nil {
		log.Fatal("Failed to create employees table:", err)
	}

	// Insert employees with nested structs
	_, err = db.Exec(`
		INSERT INTO employees VALUES 
			(1, 'John Doe', {
				'email': 'john@example.com',
				'phone': '+1-555-0123',
				'address': {
					'street': '123 Main St',
					'city': 'New York',
					'country': 'USA'
				}
			}),
			(2, 'Jane Smith', {
				'email': 'jane@example.com',
				'phone': '+1-555-0456',
				'address': {
					'street': '456 Oak Ave',
					'city': 'San Francisco',
					'country': 'USA'
				}
			})
	`)
	if err != nil {
		log.Fatal("Failed to insert employees:", err)
	}

	// Query struct fields
	fmt.Println("Employee Contacts:")
	rows, err := db.Query(`
		SELECT 
			name,
			contact.email,
			contact.phone,
			contact.address.city
		FROM employees
		ORDER BY name
	`)
	if err != nil {
		log.Fatal("Failed to query employees:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, email, phone, city string
		rows.Scan(&name, &email, &phone, &city)
		fmt.Printf("  %s\n", name)
		fmt.Printf("    Email: %s\n", email)
		fmt.Printf("    Phone: %s\n", phone)
		fmt.Printf("    City: %s\n", city)
	}
}

func demonstrateMaps(db *sql.DB) {
	// Create table with maps
	_, err := db.Exec(`
		CREATE TABLE products (
			id INTEGER,
			name VARCHAR,
			specifications MAP(VARCHAR, VARCHAR),
			prices MAP(VARCHAR, DECIMAL)
		)
	`)
	if err != nil {
		log.Fatal("Failed to create products table:", err)
	}

	// Insert products with maps
	_, err = db.Exec(`
		INSERT INTO products VALUES 
			(1, 'Laptop Pro', 
				MAP(['cpu', 'ram', 'storage'], ['Intel i7', '16GB', '512GB SSD']),
				MAP(['USD', 'EUR', 'GBP'], [999.99, 899.99, 799.99])
			),
			(2, 'Smartphone X',
				MAP(['screen', 'camera', 'battery'], ['6.1 inch', '12MP', '3000mAh']),
				MAP(['USD', 'EUR'], [699.99, 629.99])
			)
	`)
	if err != nil {
		log.Fatal("Failed to insert products:", err)
	}

	// Query map data
	fmt.Println("Product Specifications:")
	rows, err := db.Query(`
		SELECT 
			name,
			specifications,
			prices['USD'] as usd_price
		FROM products
		ORDER BY name
	`)
	if err != nil {
		log.Fatal("Failed to query products:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, specs string
		var usdPrice sql.NullFloat64
		rows.Scan(&name, &specs, &usdPrice)

		fmt.Printf("  %s\n", name)

		// Parse specifications
		var specsMap map[string]interface{}
		json.Unmarshal([]byte(specs), &specsMap)
		fmt.Printf("    Specifications: %v\n", specsMap)

		if usdPrice.Valid {
			fmt.Printf("    USD Price: $%.2f\n", usdPrice.Float64)
		}
	}

	// Extract specific map values
	fmt.Println("\nProduct RAM specifications:")
	rows2, err := db.Query(`
		SELECT 
			name,
			specifications['ram'] as ram
		FROM products
		WHERE specifications['ram'] IS NOT NULL
	`)
	if err != nil {
		log.Fatal("Failed to query RAM specs:", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var name, ram string
		rows2.Scan(&name, &ram)
		fmt.Printf("  %s: %s\n", name, ram)
	}
}

func demonstrateNestedTypes(db *sql.DB) {
	// Create table with complex nested types
	_, err := db.Exec(`
		CREATE TABLE projects (
			id INTEGER,
			name VARCHAR,
			team STRUCT(
				lead VARCHAR,
				members VARCHAR[],
				skills MAP(VARCHAR, VARCHAR[])
			),
			milestones STRUCT(
				completed INTEGER[],
				upcoming MAP(VARCHAR, DATE)
			)[]
		)
	`)
	if err != nil {
		log.Fatal("Failed to create projects table:", err)
	}

	// Insert project with nested data
	_, err = db.Exec(`
		INSERT INTO projects VALUES (
			1, 
			'DuckDB Integration',
			{
				'lead': 'Alice Johnson',
				'members': ['Bob Smith', 'Charlie Brown'],
				'skills': MAP(
					['backend', 'frontend', 'database'],
					[
						['Go', 'Python'],
						['React', 'Vue'],
						['DuckDB', 'PostgreSQL']
					]
				)
			},
			[
				{
					'completed': [1, 2, 3],
					'upcoming': MAP(['Phase 2', 'Phase 3'], [DATE '2024-02-01', DATE '2024-03-01'])
				}
			]
		)
	`)
	if err != nil {
		log.Fatal("Failed to insert project:", err)
	}

	// Query nested data
	var projectData string
	err = db.QueryRow(`
		SELECT 
			row_to_json({
				'name': name,
				'team_lead': team.lead,
				'member_count': array_length(team.members),
				'has_backend_skills': map_contains(team.skills, 'backend')
			}) as project_info
		FROM projects 
		WHERE id = 1
	`).Scan(&projectData)
	if err != nil {
		log.Fatal("Failed to query project:", err)
	}

	fmt.Printf("Project Info (JSON):\n%s\n", projectData)

	// Parse and display
	var info map[string]interface{}
	json.Unmarshal([]byte(projectData), &info)
	fmt.Println("\nParsed Project Info:")
	for k, v := range info {
		fmt.Printf("  %s: %v\n", k, v)
	}
}
