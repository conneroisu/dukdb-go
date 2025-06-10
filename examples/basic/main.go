package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/connerohnesorge/dukdb-go"
)

func main() {
	// Open an in-memory database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`
		CREATE TABLE employees (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			department VARCHAR,
			salary DOUBLE
		)
	`)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}

	// Insert some data
	_, err = db.Exec(`
		INSERT INTO employees VALUES 
			(1, 'Alice Johnson', 'Engineering', 75000),
			(2, 'Bob Smith', 'Sales', 65000),
			(3, 'Charlie Brown', 'Engineering', 80000),
			(4, 'Diana Prince', 'HR', 70000),
			(5, 'Eve Wilson', 'Sales', 72000)
	`)
	if err != nil {
		log.Fatal("Failed to insert data:", err)
	}

	// Query the data
	fmt.Println("All Employees:")
	rows, err := db.Query("SELECT name, department, salary FROM employees ORDER BY name")
	if err != nil {
		log.Fatal("Failed to query data:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, department string
		var salary float64
		err := rows.Scan(&name, &department, &salary)
		if err != nil {
			log.Fatal("Failed to scan row:", err)
		}
		fmt.Printf("  %s (%s): $%.2f\n", name, department, salary)
	}

	// Aggregate query
	fmt.Println("\nDepartment Summary:")
	rows, err = db.Query(`
		SELECT 
			department, 
			COUNT(*) as employee_count,
			AVG(salary) as avg_salary
		FROM employees 
		GROUP BY department
		ORDER BY department
	`)
	if err != nil {
		log.Fatal("Failed to query aggregate data:", err)
	}
	defer rows.Close()

	for rows.Next() {
		var department string
		var count int
		var avgSalary float64
		err := rows.Scan(&department, &count, &avgSalary)
		if err != nil {
			log.Fatal("Failed to scan aggregate row:", err)
		}
		fmt.Printf("  %s: %d employees, avg salary $%.2f\n", department, count, avgSalary)
	}

	// Using a transaction
	fmt.Println("\nGiving everyone a raise...")
	tx, err := db.Begin()
	if err != nil {
		log.Fatal("Failed to begin transaction:", err)
	}

	_, err = tx.Exec("UPDATE employees SET salary = salary * 1.1")
	if err != nil {
		tx.Rollback()
		log.Fatal("Failed to update salaries:", err)
	}

	err = tx.Commit()
	if err != nil {
		log.Fatal("Failed to commit transaction:", err)
	}

	// Check the new salaries
	var totalSalary float64
	err = db.QueryRow("SELECT SUM(salary) FROM employees").Scan(&totalSalary)
	if err != nil {
		log.Fatal("Failed to get total salary:", err)
	}
	fmt.Printf("Total salary budget after raise: $%.2f\n", totalSalary)
}