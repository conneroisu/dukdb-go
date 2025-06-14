package parser

import (
	"testing"

	"github.com/connerohnesorge/dukdb-go/internal/ast"
)

func TestParseSimpleSelect(t *testing.T) {
	sql := "SELECT id, name FROM users WHERE age > 18"
	
	stmt, err := ParseSQL(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}
	
	// Check select list
	if len(selectStmt.SelectList) != 2 {
		t.Errorf("Expected 2 select items, got %d", len(selectStmt.SelectList))
	}
	
	// Check FROM clause
	if len(selectStmt.From) != 1 {
		t.Errorf("Expected 1 from item, got %d", len(selectStmt.From))
	}
	
	// Check WHERE clause
	if selectStmt.Where == nil {
		t.Error("Expected WHERE clause, got nil")
	}
}

func TestParseSelectWithJoin(t *testing.T) {
	sql := "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id"
	
	stmt, err := ParseSQL(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}
	
	// Check that we have select items
	if len(selectStmt.SelectList) != 2 {
		t.Errorf("Expected 2 select items, got %d", len(selectStmt.SelectList))
	}
	
	// Check that we have a FROM clause
	if len(selectStmt.From) == 0 {
		t.Error("Expected FROM clause items")
	}
}

func TestParseInsert(t *testing.T) {
	sql := "INSERT INTO users (name, age) VALUES ('John', 25)"
	
	stmt, err := ParseSQL(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	insertStmt, ok := stmt.(*ast.InsertStmt)
	if !ok {
		t.Fatalf("Expected InsertStmt, got %T", stmt)
	}
	
	if insertStmt.Table.Name != "users" {
		t.Errorf("Expected table name 'users', got '%s'", insertStmt.Table.Name)
	}
	
	if len(insertStmt.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(insertStmt.Columns))
	}
	
	if len(insertStmt.Values) != 1 {
		t.Errorf("Expected 1 value row, got %d", len(insertStmt.Values))
	}
}

func TestParseUpdate(t *testing.T) {
	sql := "UPDATE users SET name = 'Jane' WHERE id = 1"
	
	stmt, err := ParseSQL(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	updateStmt, ok := stmt.(*ast.UpdateStmt)
	if !ok {
		t.Fatalf("Expected UpdateStmt, got %T", stmt)
	}
	
	if updateStmt.Table.Name != "users" {
		t.Errorf("Expected table name 'users', got '%s'", updateStmt.Table.Name)
	}
	
	if len(updateStmt.Set) != 1 {
		t.Errorf("Expected 1 assignment, got %d", len(updateStmt.Set))
	}
	
	if updateStmt.Where == nil {
		t.Error("Expected WHERE clause, got nil")
	}
}

func TestParseDelete(t *testing.T) {
	sql := "DELETE FROM users WHERE age < 18"
	
	stmt, err := ParseSQL(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	deleteStmt, ok := stmt.(*ast.DeleteStmt)
	if !ok {
		t.Fatalf("Expected DeleteStmt, got %T", stmt)
	}
	
	if deleteStmt.Table.Name != "users" {
		t.Errorf("Expected table name 'users', got '%s'", deleteStmt.Table.Name)
	}
	
	if deleteStmt.Where == nil {
		t.Error("Expected WHERE clause, got nil")
	}
}

func TestParseCreateTable(t *testing.T) {
	sql := "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER NOT NULL)"
	
	stmt, err := ParseSQL(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	createStmt, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		t.Fatalf("Expected CreateTableStmt, got %T", stmt)
	}
	
	if createStmt.Name.Name != "users" {
		t.Errorf("Expected table name 'users', got '%s'", createStmt.Name.Name)
	}
	
	if len(createStmt.Columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(createStmt.Columns))
	}
}

func TestParseDuckDBListType(t *testing.T) {
	sql := "SELECT [1, 2, 3] AS numbers"
	
	stmt, err := ParseSQL(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}
	
	if len(selectStmt.SelectList) != 1 {
		t.Errorf("Expected 1 select item, got %d", len(selectStmt.SelectList))
	}
	
	// Check that we parsed a list expression
	listExpr, ok := selectStmt.SelectList[0].Expression.(*ast.ListExpr)
	if !ok {
		t.Errorf("Expected ListExpr, got %T", selectStmt.SelectList[0].Expression)
	} else {
		if len(listExpr.Elements) != 3 {
			t.Errorf("Expected 3 list elements, got %d", len(listExpr.Elements))
		}
	}
}

func TestParseComplexQuery(t *testing.T) {
	sql := `SELECT u.name, COUNT(p.id) as post_count 
			FROM users u 
			LEFT JOIN posts p ON u.id = p.user_id 
			WHERE u.age > 18 
			GROUP BY u.name 
			HAVING COUNT(p.id) > 5 
			ORDER BY post_count DESC 
			LIMIT 10`
	
	stmt, err := ParseSQL(sql)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", stmt)
	}
	
	// Check basic structure
	if len(selectStmt.SelectList) != 2 {
		t.Errorf("Expected 2 select items, got %d", len(selectStmt.SelectList))
	}
	
	if selectStmt.Where == nil {
		t.Error("Expected WHERE clause")
	}
	
	if len(selectStmt.GroupBy) == 0 {
		t.Error("Expected GROUP BY clause")
	}
	
	if selectStmt.Having == nil {
		t.Error("Expected HAVING clause")
	}
	
	if len(selectStmt.OrderBy) == 0 {
		t.Error("Expected ORDER BY clause")
	}
	
	if selectStmt.Limit == nil {
		t.Error("Expected LIMIT clause")
	}
}

func TestLexerTokenization(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"SELECT * FROM users", []string{"SELECT", "*", "FROM", "users"}},
		{"id = 123", []string{"id", "=", "123"}},
		{"name LIKE 'John%'", []string{"name", "LIKE", "'John%'"}},
		{"age > 18 AND status = 'active'", []string{"age", ">", "18", "AND", "status", "=", "'active'"}},
	}
	
	for _, test := range tests {
		parser := New(test.input)
		var tokens []string
		
		for {
			token := parser.lexer.Current()
			if token.Type == 0 { // EOF
				break
			}
			tokens = append(tokens, token.Value)
			parser.lexer.Advance()
		}
		
		if len(tokens) != len(test.expected) {
			t.Errorf("For input '%s': expected %d tokens, got %d", 
				test.input, len(test.expected), len(tokens))
			continue
		}
		
		for i, expected := range test.expected {
			if i < len(tokens) && tokens[i] != expected {
				t.Errorf("For input '%s': token %d expected '%s', got '%s'", 
					test.input, i, expected, tokens[i])
			}
		}
	}
}

func TestErrorHandling(t *testing.T) {
	invalidSQL := []string{
		"SELECT FROM",           // Missing select list
		"SELEECT * FROM users",  // Typo in keyword
		"SELECT * FROM",         // Missing table name
		"INSERT INTO",           // Incomplete INSERT
		"UPDATE SET name = 'x'", // Missing table
	}
	
	for _, sql := range invalidSQL {
		_, err := ParseSQL(sql)
		if err == nil {
			t.Errorf("Expected error for invalid SQL: '%s'", sql)
		}
	}
}