package main

import (
	"database/sql"
	"fmt"
	"time"
	_ "github.com/connerohnesorge/dukdb-go"
)

func main() {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`
		CREATE TABLE test_table (
			category VARCHAR(50),
			amount DOUBLE
		)
	`)
	if err != nil {
		panic(err)
	}

	// Insert data
	_, err = db.Exec("INSERT INTO test_table VALUES ('A', 100.0), ('A', 200.0), ('B', 150.0)")
	if err != nil {
		panic(err)
	}

	// Test simple GROUP BY without aliases
	fmt.Println("Testing simple GROUP BY...")
	rows, err := db.Query("SELECT category, COUNT(*) FROM test_table GROUP BY category")
	if err != nil {
		fmt.Printf("Query error: %v\n", err)
		return
	}
	defer rows.Close()

	fmt.Println("Query executed successfully!")
	
	for rows.Next() {
		var category string
		var count int32
		err := rows.Scan(&category, &count)
		if err != nil {
			fmt.Printf("Scan error: %v\n", err)
			continue
		}
		fmt.Printf("Category: %s, Count: %d\n", category, count)
	}
}