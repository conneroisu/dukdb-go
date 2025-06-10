// +build cgo

package compatibility

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/connerohnesorge/dukdb-go" // Our pure-Go driver
	// _ "github.com/marcboeker/go-duckdb"     // CGO driver (requires CGO) - Temporarily disabled
)

// TestCompareBasicQueries compares basic query results between implementations
func TestCompareBasicQueries(t *testing.T) {
	// Skip if CGO driver is not available
	t.Skip("CGO comparison tests require the marcboeker/go-duckdb driver")

	queries := []string{
		"SELECT 1",
		"SELECT 1 + 1",
		"SELECT 'hello world'",
		"SELECT NULL",
		"SELECT true, false",
		"SELECT CAST(42 AS BIGINT)",
		"SELECT CAST(3.14 AS DOUBLE)",
	}

	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			// Test with our driver
			ourResult := executeQuery(t, "duckdb", query)
			
			// Test with CGO driver
			cgoResult := executeQuery(t, "duckdb-cgo", query)
			
			// Compare results
			if ourResult != cgoResult {
				t.Errorf("Results differ:\nOur driver: %s\nCGO driver: %s",
					ourResult, cgoResult)
			}
		})
	}
}

// TestCompareTableOperations compares table operations
func TestCompareTableOperations(t *testing.T) {
	t.Skip("CGO comparison tests require the marcboeker/go-duckdb driver")

	operations := []struct {
		name    string
		queries []string
		check   string
	}{
		{
			name: "CREATE_INSERT_SELECT",
			queries: []string{
				"CREATE TABLE test (id INTEGER, name VARCHAR)",
				"INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')",
			},
			check: "SELECT COUNT(*) FROM test",
		},
		{
			name: "UPDATE",
			queries: []string{
				"CREATE TABLE test (id INTEGER, value INTEGER)",
				"INSERT INTO test VALUES (1, 10), (2, 20)",
				"UPDATE test SET value = value * 2",
			},
			check: "SELECT SUM(value) FROM test",
		},
		{
			name: "DELETE",
			queries: []string{
				"CREATE TABLE test (id INTEGER)",
				"INSERT INTO test VALUES (1), (2), (3)",
				"DELETE FROM test WHERE id = 2",
			},
			check: "SELECT COUNT(*) FROM test",
		},
	}

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			// Test with our driver
			ourResult := executeOperations(t, "duckdb", op.queries, op.check)
			
			// Test with CGO driver
			cgoResult := executeOperations(t, "duckdb-cgo", op.queries, op.check)
			
			// Compare results
			if ourResult != cgoResult {
				t.Errorf("Results differ:\nOur driver: %s\nCGO driver: %s",
					ourResult, cgoResult)
			}
		})
	}
}

// executeQuery executes a single query and returns the result as a string
func executeQuery(t *testing.T, driver, query string) string {
	db, err := sql.Open(driver, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open %s: %v", driver, err)
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed on %s: %v", driver, err)
	}
	defer rows.Close()

	// Get column count
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Failed to get columns on %s: %v", driver, err)
	}

	// Read results
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var result string
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			t.Fatalf("Scan failed on %s: %v", driver, err)
		}
		result += fmt.Sprintf("%v\n", values)
	}

	return result
}

// executeOperations executes a series of operations and returns the check query result
func executeOperations(t *testing.T, driver string, operations []string, checkQuery string) string {
	db, err := sql.Open(driver, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open %s: %v", driver, err)
	}
	defer db.Close()

	// Execute operations
	for _, op := range operations {
		_, err := db.Exec(op)
		if err != nil {
			t.Fatalf("Operation failed on %s: %v\nQuery: %s", driver, err, op)
		}
	}

	// Execute check query
	return executeQuery(t, driver, checkQuery)
}