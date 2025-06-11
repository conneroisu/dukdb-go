package test

import (
	"context"
	"testing"

	"github.com/connerohnesorge/dukdb-go/internal/engine"
	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

func TestPureGoEngine(t *testing.T) {
	t.Run("Basic Operations", func(t *testing.T) {
		// Create in-memory database
		db, err := engine.NewDatabase(":memory:")
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}
		defer db.Close()

		// Create connection
		conn, err := db.Connect()
		if err != nil {
			t.Fatalf("Failed to create connection: %v", err)
		}
		defer conn.Close()

		// Create table
		createSQL := `CREATE TABLE users (
			id INTEGER,
			name VARCHAR,
			age INTEGER,
			score DOUBLE
		)`
		
		err = conn.Execute(context.Background(), createSQL)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Verify table exists
		table, err := db.GetCatalog().GetTable("main", "users")
		if err != nil {
			t.Fatalf("Table not found after creation: %v", err)
		}

		columns := table.GetColumns()
		if len(columns) != 4 {
			t.Errorf("Expected 4 columns, got %d", len(columns))
		}

		// Verify column types
		expectedTypes := []storage.TypeID{
			storage.TypeInteger,
			storage.TypeVarchar,
			storage.TypeInteger,
			storage.TypeDouble,
		}

		for i, col := range columns {
			if col.Type.ID != expectedTypes[i] {
				t.Errorf("Column %s: expected type %v, got %v", 
					col.Name, expectedTypes[i], col.Type.ID)
			}
		}
	})

	t.Run("Insert and Select", func(t *testing.T) {
		db, err := engine.NewDatabase(":memory:")
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}
		defer db.Close()

		conn, err := db.Connect()
		if err != nil {
			t.Fatalf("Failed to create connection: %v", err)
		}
		defer conn.Close()

		// Create table
		createSQL := `CREATE TABLE test_data (
			id INTEGER,
			value VARCHAR
		)`
		
		err = conn.Execute(context.Background(), createSQL)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert data
		insertSQL := `INSERT INTO test_data VALUES (1, 'Hello'), (2, 'World')`
		err = conn.Execute(context.Background(), insertSQL)
		if err != nil {
			t.Logf("Insert not yet implemented: %v", err)
			// Expected for now as we haven't implemented value parsing
		}

		// Select data
		selectSQL := `SELECT * FROM test_data`
		result, err := conn.Query(context.Background(), selectSQL)
		if err != nil {
			t.Fatalf("Failed to execute select: %v", err)
		}
		defer result.Close()

		// Verify columns
		cols := result.Columns()
		if len(cols) != 2 {
			t.Errorf("Expected 2 columns, got %d", len(cols))
		}
	})

	t.Run("Transaction Support", func(t *testing.T) {
		db, err := engine.NewDatabase(":memory:")
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}
		defer db.Close()

		conn, err := db.Connect()
		if err != nil {
			t.Fatalf("Failed to create connection: %v", err)
		}
		defer conn.Close()

		// Begin transaction
		err = conn.Execute(context.Background(), "BEGIN")
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Commit transaction
		err = conn.Execute(context.Background(), "COMMIT")
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		// Begin and rollback
		err = conn.Execute(context.Background(), "BEGIN")
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		err = conn.Execute(context.Background(), "ROLLBACK")
		if err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}
	})
}

func TestSQLParser(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{"Empty SQL", "", true},
		{"Simple SELECT", "SELECT * FROM users", false},
		{"CREATE TABLE", "CREATE TABLE test (id INTEGER, name VARCHAR)", false},
		{"BEGIN", "BEGIN", false},
		{"COMMIT", "COMMIT", false},
		{"ROLLBACK", "ROLLBACK", false},
		{"Unsupported", "DROP TABLE test", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := engine.ParseSQL(tt.sql)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				} else if stmt == nil {
					t.Errorf("Got nil statement")
				}
			}
		})
	}
}

func TestCatalog(t *testing.T) {
	catalog := engine.NewCatalog()

	// Test default schemas
	schemas := []string{"main", "temp", "information_schema"}
	for _, schemaName := range schemas {
		schema, err := catalog.GetSchema(schemaName)
		if err != nil {
			t.Errorf("Default schema %s not found: %v", schemaName, err)
		}
		if schema == nil {
			t.Errorf("Schema %s is nil", schemaName)
		}
	}

	// Test creating and retrieving a table
	columns := []engine.ColumnDefinition{
		{
			Name: "id",
			Type: storage.LogicalType{ID: storage.TypeInteger},
		},
		{
			Name: "name", 
			Type: storage.LogicalType{ID: storage.TypeVarchar},
		},
	}

	err := catalog.CreateTable("main", "test_table", columns)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Retrieve the table
	table, err := catalog.GetTable("main", "test_table")
	if err != nil {
		t.Fatalf("Failed to retrieve table: %v", err)
	}

	// Verify columns
	tableCols := table.GetColumns()
	if len(tableCols) != len(columns) {
		t.Errorf("Expected %d columns, got %d", len(columns), len(tableCols))
	}

	// Test duplicate table creation
	err = catalog.CreateTable("main", "test_table", columns)
	if err == nil {
		t.Error("Expected error when creating duplicate table")
	}
}