// Example 4: Join operations between parquet files
// This example demonstrates joining data from multiple parquet files

package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/connerohnesorge/dukdb-go"
	"github.com/parquet-go/parquet-go"
)

// Department represents a department
type Department struct {
	ID   int32  `parquet:"id"`
	Name string `parquet:"name"`
}

// Employee represents an employee
type Employee struct {
	ID     int32  `parquet:"id"`
	Name   string `parquet:"name"`
	DeptID int32  `parquet:"dept_id"`
	Salary int32  `parquet:"salary"`
}

func main() {
	// Clean up any existing files
	os.Remove("departments.parquet")
	os.Remove("employees.parquet")

	// Create sample data
	if err := createSampleData(); err != nil {
		log.Fatal("Failed to create sample data:", err)
	}
	defer os.Remove("departments.parquet")
	defer os.Remove("employees.parquet")

	// Open database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Setup tables
	if err := setupTables(db); err != nil {
		log.Fatal("Failed to setup tables:", err)
	}

	// Since our parser doesn't support JOINs yet, we'll simulate joins manually
	fmt.Println("=== Employee Directory ===")
	
	// Simulate INNER JOIN employees e ON e.dept_id = d.id
	fmt.Println("Employees with their departments:")
	if err := showEmployeeDirectory(db); err != nil {
		log.Fatal("Failed to show directory:", err)
	}

	// Department summary
	fmt.Println("\n\nDepartment Summary:")
	if err := showDepartmentSummary(db); err != nil {
		log.Fatal("Failed to show summary:", err)
	}

	fmt.Println("\nExample completed successfully!")
}

func createSampleData() error {
	// Create departments
	departments := []Department{
		{1, "Engineering"},
		{2, "Sales"},
		{3, "Marketing"},
		{4, "HR"},
	}

	file, err := os.Create("departments.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := parquet.NewGenericWriter[Department](file)
	if _, err := writer.Write(departments); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	// Create employees
	employees := []Employee{
		{101, "Alice Johnson", 1, 95000},
		{102, "Bob Smith", 1, 85000},
		{103, "Carol White", 2, 75000},
		{104, "David Brown", 2, 80000},
		{105, "Eve Davis", 3, 70000},
		{106, "Frank Miller", 1, 90000},
		{107, "Grace Wilson", 4, 65000},
		{108, "Henry Moore", 3, 72000},
		{109, "Iris Taylor", 1, 100000},
		{110, "Jack Anderson", 2, 78000},
	}

	file2, err := os.Create("employees.parquet")
	if err != nil {
		return err
	}
	defer file2.Close()

	writer2 := parquet.NewGenericWriter[Employee](file2)
	if _, err := writer2.Write(employees); err != nil {
		return err
	}
	return writer2.Close()
}

func setupTables(db *sql.DB) error {
	// Create departments table
	_, err := db.Exec(`
		CREATE TABLE departments (
			id INTEGER,
			name VARCHAR
		)
	`)
	if err != nil {
		return err
	}

	// Create employees table
	_, err = db.Exec(`
		CREATE TABLE employees (
			id INTEGER,
			name VARCHAR,
			dept_id INTEGER,
			salary INTEGER
		)
	`)
	if err != nil {
		return err
	}

	// Load departments
	if err := loadDepartments(db); err != nil {
		return fmt.Errorf("loading departments: %w", err)
	}

	// Load employees
	if err := loadEmployees(db); err != nil {
		return fmt.Errorf("loading employees: %w", err)
	}

	return nil
}

func loadDepartments(db *sql.DB) error {
	file, err := os.Open("departments.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[Department](file)
	defer reader.Close()

	depts := make([]Department, 10)
	for {
		n, err := reader.Read(depts)
		if err != nil && err.Error() == "EOF" {
			if n > 0 {
				goto ProcessRecords
			}
			break
		}
		if n == 0 {
			break
		}

	ProcessRecords:
		for i := 0; i < n; i++ {
			d := depts[i]
			sql := fmt.Sprintf("INSERT INTO departments VALUES (%d, '%s')",
				d.ID, d.Name)
			if _, err := db.Exec(sql); err != nil {
				return err
			}
		}
		
		if err != nil && err.Error() == "EOF" {
			break
		}
	}

	return nil
}

func loadEmployees(db *sql.DB) error {
	file, err := os.Open("employees.parquet")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[Employee](file)
	defer reader.Close()

	emps := make([]Employee, 20)
	for {
		n, err := reader.Read(emps)
		if err != nil && err.Error() == "EOF" {
			if n > 0 {
				goto ProcessRecords
			}
			break
		}
		if n == 0 {
			break
		}

	ProcessRecords:
		for i := 0; i < n; i++ {
			e := emps[i]
			sql := fmt.Sprintf("INSERT INTO employees VALUES (%d, '%s', %d, %d)",
				e.ID, e.Name, e.DeptID, e.Salary)
			if _, err := db.Exec(sql); err != nil {
				return err
			}
		}
		
		if err != nil && err.Error() == "EOF" {
			break
		}
	}

	return nil
}

func showEmployeeDirectory(db *sql.DB) error {
	// First get all departments
	deptRows, err := db.Query("SELECT id, name FROM departments")
	if err != nil {
		return err
	}
	
	departments := make(map[int32]string)
	for deptRows.Next() {
		var id int32
		var name string
		if err := deptRows.Scan(&id, &name); err != nil {
			deptRows.Close()
			return err
		}
		departments[id] = name
	}
	deptRows.Close()

	// Now get all employees
	empRows, err := db.Query("SELECT id, name, dept_id, salary FROM employees")
	if err != nil {
		return err
	}
	defer empRows.Close()

	fmt.Printf("%-12s %-20s %-15s %10s\n", "Employee ID", "Name", "Department", "Salary")
	fmt.Println(string(make([]byte, 60)))

	for empRows.Next() {
		var id, deptID, salary int32
		var name string
		if err := empRows.Scan(&id, &name, &deptID, &salary); err != nil {
			return err
		}
		
		deptName := departments[deptID]
		if deptName == "" {
			deptName = "Unknown"
		}
		
		fmt.Printf("%-12d %-20s %-15s $%9d\n", id, name, deptName, salary)
	}

	return nil
}

func showDepartmentSummary(db *sql.DB) error {
	// First get all departments
	deptRows, err := db.Query("SELECT id, name FROM departments")
	if err != nil {
		return err
	}
	
	type deptInfo struct {
		name  string
		count int
		total int64
	}
	
	departments := make(map[int32]*deptInfo)
	for deptRows.Next() {
		var id int32
		var name string
		if err := deptRows.Scan(&id, &name); err != nil {
			deptRows.Close()
			return err
		}
		departments[id] = &deptInfo{name: name}
	}
	deptRows.Close()

	// Get employee data
	empRows, err := db.Query("SELECT dept_id, salary FROM employees")
	if err != nil {
		return err
	}
	defer empRows.Close()

	for empRows.Next() {
		var deptID int32
		var salary int32
		if err := empRows.Scan(&deptID, &salary); err != nil {
			return err
		}
		
		if dept, ok := departments[deptID]; ok {
			dept.count++
			dept.total += int64(salary)
		}
	}

	// Display summary
	fmt.Printf("%-15s %10s %15s %15s\n", "Department", "Employees", "Total Salary", "Average Salary")
	fmt.Println(string(make([]byte, 60)))

	for _, dept := range departments {
		avg := int64(0)
		if dept.count > 0 {
			avg = dept.total / int64(dept.count)
		}
		fmt.Printf("%-15s %10d $%14d $%14d\n", dept.name, dept.count, dept.total, avg)
	}

	return nil
}