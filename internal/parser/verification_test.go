package parser

import (
	"testing"

	"github.com/connerohnesorge/dukdb-go/internal/ast"
)

// TestSpecificationCompliance tests parser compliance with the v1.md specification
func TestSpecificationCompliance(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectError bool
	}{
		// Basic SELECT statements (from spec)
		{
			name: "Simple SELECT",
			sql:  "SELECT id, name FROM users WHERE age > 18",
		},
		{
			name: "SELECT with DISTINCT",
			sql:  "SELECT DISTINCT name FROM users",
		},
		{
			name: "SELECT with aliases",
			sql:  "SELECT u.name AS user_name, u.email FROM users u",
		},

		// JOINs (from spec)
		{
			name: "INNER JOIN",
			sql:  "SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id",
		},
		{
			name: "LEFT JOIN",
			sql:  "SELECT u.name, p.title FROM users u LEFT JOIN posts p ON u.id = p.user_id",
		},
		{
			name: "RIGHT JOIN",
			sql:  "SELECT u.name, p.title FROM users u RIGHT JOIN posts p ON u.id = p.user_id",
		},
		{
			name: "FULL JOIN",
			sql:  "SELECT u.name, p.title FROM users u FULL JOIN posts p ON u.id = p.user_id",
		},
		{
			name: "CROSS JOIN",
			sql:  "SELECT u.name, p.title FROM users u CROSS JOIN posts p",
		},

		// Aggregations (from spec)
		{
			name: "GROUP BY with aggregates",
			sql:  "SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department",
		},
		{
			name: "HAVING clause",
			sql:  "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
		},

		// ORDER BY and LIMIT (from spec)
		{
			name: "ORDER BY",
			sql:  "SELECT name, age FROM users ORDER BY age DESC, name ASC",
		},
		{
			name: "LIMIT and OFFSET",
			sql:  "SELECT name FROM users ORDER BY name LIMIT 10 OFFSET 20",
		},

		// DML statements (from spec)
		{
			name: "INSERT with columns",
			sql:  "INSERT INTO users (name, email, age) VALUES ('John', 'john@example.com', 30)",
		},
		{
			name: "INSERT without columns",
			sql:  "INSERT INTO users VALUES ('John', 'john@example.com', 30)",
		},
		{
			name: "UPDATE statement",
			sql:  "UPDATE users SET email = 'newemail@example.com' WHERE id = 1",
		},
		{
			name: "DELETE statement",
			sql:  "DELETE FROM users WHERE age < 18",
		},

		// DDL statements (from spec)
		{
			name: "CREATE TABLE",
			sql:  "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER NOT NULL)",
		},
		{
			name: "CREATE TABLE IF NOT EXISTS",
			sql:  "CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR)",
		},
		{
			name: "DROP TABLE",
			sql:  "DROP TABLE users",
		},
		{
			name: "DROP TABLE IF EXISTS",
			sql:  "DROP TABLE IF EXISTS users",
		},

		// DuckDB-specific types (from spec)
		{
			name: "LIST type",
			sql:  "SELECT [1, 2, 3] AS numbers",
		},
		{
			name: "LIST type in CREATE TABLE",
			sql:  "CREATE TABLE test_table (id INTEGER, tags LIST<VARCHAR>)",
		},
		{
			name: "STRUCT type in CREATE TABLE",
			sql:  "CREATE TABLE test_table (id INTEGER, metadata STRUCT<name, value>)",
		},
		{
			name: "MAP type in CREATE TABLE",
			sql:  "CREATE TABLE test_table (id INTEGER, properties MAP<VARCHAR, INTEGER>)",
		},

		// Functions (from spec)
		{
			name: "Function calls",
			sql:  "SELECT LENGTH(name), UPPER(email) FROM users",
		},
		{
			name: "COUNT function",
			sql:  "SELECT COUNT(*) FROM users",
		},
		{
			name: "COUNT DISTINCT",
			sql:  "SELECT COUNT(DISTINCT department) FROM employees",
		},

		// Expressions (from spec)
		{
			name: "Arithmetic expressions",
			sql:  "SELECT age + 1, salary * 1.1 FROM users",
		},
		{
			name: "Comparison expressions",
			sql:  "SELECT name FROM users WHERE age >= 18 AND salary < 50000",
		},
		{
			name: "String operations",
			sql:  "SELECT name FROM users WHERE name LIKE 'John%'",
		},
		{
			name: "NULL operations",
			sql:  "SELECT name FROM users WHERE email IS NOT NULL",
		},

		// CASE expressions (from spec)
		{
			name: "Simple CASE",
			sql:  "SELECT CASE status WHEN 'active' THEN 'Active User' ELSE 'Inactive' END FROM users",
		},
		{
			name: "Searched CASE",
			sql:  "SELECT CASE WHEN age < 18 THEN 'Minor' ELSE 'Adult' END FROM users",
		},

		// Subqueries (from spec)
		{
			name: "Subquery in FROM",
			sql:  "SELECT * FROM (SELECT name, age FROM users WHERE age > 18) AS adults",
		},
		{
			name: "Subquery in WHERE",
			sql:  "SELECT name FROM users WHERE id IN (SELECT user_id FROM posts)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseSQL(test.sql)
			
			if test.expectError {
				if err == nil {
					t.Errorf("Expected error for SQL: %s", test.sql)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for SQL '%s': %v", test.sql, err)
				} else if stmt == nil {
					t.Errorf("Expected statement but got nil for SQL: %s", test.sql)
				}
			}
		})
	}
}

// TestDuckDBFeatureSupport tests DuckDB-specific features from the specification
func TestDuckDBFeatureSupport(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		expectedType   interface{}
		checkStructure func(ast.Statement) bool
	}{
		{
			name:         "LIST expression",
			sql:          "SELECT [1, 2, 3] AS list_col",
			expectedType: &ast.SelectStmt{},
			checkStructure: func(stmt ast.Statement) bool {
				selectStmt := stmt.(*ast.SelectStmt)
				if len(selectStmt.SelectList) != 1 {
					return false
				}
				_, ok := selectStmt.SelectList[0].Expression.(*ast.ListExpr)
				return ok
			},
		},
		{
			name:         "Complex data types in CREATE TABLE",
			sql:          "CREATE TABLE complex_table (id INTEGER, data LIST<INTEGER>, info STRUCT<name, value>)",
			expectedType: &ast.CreateTableStmt{},
			checkStructure: func(stmt ast.Statement) bool {
				createStmt := stmt.(*ast.CreateTableStmt)
				if len(createStmt.Columns) != 3 {
					return false
				}
				// Check for LIST type
				_, isListType := createStmt.Columns[1].Type.(*ast.ListType)
				_, isStructType := createStmt.Columns[2].Type.(*ast.StructType)
				return isListType && isStructType
			},
		},
		{
			name:         "JOIN with aliases",
			sql:          "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
			expectedType: &ast.SelectStmt{},
			checkStructure: func(stmt ast.Statement) bool {
				selectStmt := stmt.(*ast.SelectStmt)
				return len(selectStmt.From) > 0
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseSQL(test.sql)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Check type
			if stmt == nil {
				t.Fatal("Expected statement but got nil")
			}

			// Check structure if provided
			if test.checkStructure != nil && !test.checkStructure(stmt) {
				t.Errorf("Structure check failed for SQL: %s", test.sql)
			}
		})
	}
}

// TestParserErrorHandling tests error cases
func TestParserErrorHandling(t *testing.T) {
	errorCases := []string{
		"SELECT FROM",                    // Missing select list
		"SELECT * FROM",                  // Missing table name
		"INSERT INTO",                    // Incomplete INSERT
		"UPDATE SET name = 'value'",      // Missing table in UPDATE
		"DELETE FROM",                    // Missing table name in DELETE
		"CREATE TABLE",                   // Missing table name
		"SELECT * FROM users WHERE",      // Incomplete WHERE clause
		"SELECT * FROM users GROUP BY",  // Incomplete GROUP BY
		"SELECT * FROM users HAVING",    // Incomplete HAVING
		"SELECT * FROM users ORDER BY",  // Incomplete ORDER BY
	}

	for _, sql := range errorCases {
		t.Run("Error: "+sql, func(t *testing.T) {
			_, err := ParseSQL(sql)
			if err == nil {
				t.Errorf("Expected error for invalid SQL: %s", sql)
			}
		})
	}
}

// TestSpecificationTypesSupport verifies support for types mentioned in v1.md
func TestSpecificationTypesSupport(t *testing.T) {
	typeTests := []struct {
		sql      string
		typeName string
	}{
		{"CREATE TABLE t (c BOOLEAN)", "BOOLEAN"},
		{"CREATE TABLE t (c TINYINT)", "TINYINT"},
		{"CREATE TABLE t (c SMALLINT)", "SMALLINT"},
		{"CREATE TABLE t (c INTEGER)", "INTEGER"},
		{"CREATE TABLE t (c BIGINT)", "BIGINT"},
		{"CREATE TABLE t (c FLOAT)", "FLOAT"},
		{"CREATE TABLE t (c DOUBLE)", "DOUBLE"},
		{"CREATE TABLE t (c DECIMAL)", "DECIMAL"},
		{"CREATE TABLE t (c VARCHAR)", "VARCHAR"},
		{"CREATE TABLE t (c TEXT)", "TEXT"},
		{"CREATE TABLE t (c DATE)", "DATE"},
		{"CREATE TABLE t (c TIME)", "TIME"},
		{"CREATE TABLE t (c TIMESTAMP)", "TIMESTAMP"},
		{"CREATE TABLE t (c INTERVAL)", "INTERVAL"},
		{"CREATE TABLE t (c BLOB)", "BLOB"},
		{"CREATE TABLE t (c UUID)", "UUID"},
		{"CREATE TABLE t (c LIST<INTEGER>)", "LIST"},
		{"CREATE TABLE t (c STRUCT<name, value>)", "STRUCT"},
		{"CREATE TABLE t (c MAP<VARCHAR, INTEGER>)", "MAP"},
	}

	for _, test := range typeTests {
		t.Run("Type: "+test.typeName, func(t *testing.T) {
			stmt, err := ParseSQL(test.sql)
			if err != nil {
				t.Errorf("Failed to parse type %s: %v", test.typeName, err)
			} else if stmt == nil {
				t.Errorf("Expected statement for type %s", test.typeName)
			}
		})
	}
}