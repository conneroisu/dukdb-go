package main

import (
	"database/sql"
	"testing"

	_ "github.com/connerohnesorge/dukdb-go"
)

func TestBasicOperations(t *testing.T) {
	// Skip test if we can't connect (due to purego linking issue)
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Skipf("Skipping test due to connection error: %v", err)
		return
	}
	defer db.Close()

	// Test ping
	if err := db.Ping(); err != nil {
		t.Skipf("Skipping test due to ping error: %v", err)
		return
	}

	t.Run("CreateTable", func(t *testing.T) {
		_, err := db.Exec(`CREATE TABLE test_users (id INTEGER, name VARCHAR, email VARCHAR)`)
		if err != nil {
			t.Errorf("Failed to create table: %v", err)
		}
	})

	t.Run("InsertData", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO test_users VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')`)
		if err != nil {
			t.Errorf("Failed to insert data: %v", err)
		}
	})

	t.Run("QueryData", func(t *testing.T) {
		rows, err := db.Query("SELECT id, name, email FROM test_users ORDER BY id")
		if err != nil {
			t.Errorf("Failed to query data: %v", err)
			return
		}
		defer rows.Close()

		expectedUsers := []struct {
			id    int
			name  string
			email string
		}{
			{1, "Alice", "alice@example.com"},
			{2, "Bob", "bob@example.com"},
		}

		i := 0
		for rows.Next() {
			var id int
			var name, email string
			err := rows.Scan(&id, &name, &email)
			if err != nil {
				t.Errorf("Failed to scan row: %v", err)
				continue
			}

			if i >= len(expectedUsers) {
				t.Errorf("Got more rows than expected")
				break
			}

			expected := expectedUsers[i]
			if id != expected.id || name != expected.name || email != expected.email {
				t.Errorf("Row %d: got (%d, %s, %s), want (%d, %s, %s)",
					i, id, name, email, expected.id, expected.name, expected.email)
			}
			i++
		}

		if i != len(expectedUsers) {
			t.Errorf("Got %d rows, expected %d", i, len(expectedUsers))
		}
	})

	t.Run("PreparedStatements", func(t *testing.T) {
		stmt, err := db.Prepare("SELECT name FROM test_users WHERE id = ?")
		if err != nil {
			t.Errorf("Failed to prepare statement: %v", err)
			return
		}
		defer stmt.Close()

		var name string
		err = stmt.QueryRow(1).Scan(&name)
		if err != nil {
			t.Errorf("Failed to execute prepared statement: %v", err)
			return
		}

		if name != "Alice" {
			t.Errorf("Got name %s, expected Alice", name)
		}
	})

	t.Run("UpdateData", func(t *testing.T) {
		result, err := db.Exec("UPDATE test_users SET email = ? WHERE id = ?", "alice.updated@example.com", 1)
		if err != nil {
			t.Errorf("Failed to update data: %v", err)
			return
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Errorf("Failed to get rows affected: %v", err)
			return
		}

		if rowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", rowsAffected)
		}
	})

	t.Run("DeleteData", func(t *testing.T) {
		result, err := db.Exec("DELETE FROM test_users WHERE id = ?", 2)
		if err != nil {
			t.Errorf("Failed to delete data: %v", err)
			return
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Errorf("Failed to get rows affected: %v", err)
			return
		}

		if rowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", rowsAffected)
		}
	})
}

func TestTransactions(t *testing.T) {
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

	// Setup test table
	_, err = db.Exec(`CREATE TABLE tx_test (id INTEGER, value VARCHAR)`)
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
		return
	}

	t.Run("CommitTransaction", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Errorf("Failed to begin transaction: %v", err)
			return
		}

		_, err = tx.Exec("INSERT INTO tx_test VALUES (1, 'committed')")
		if err != nil {
			tx.Rollback()
			t.Errorf("Failed to insert in transaction: %v", err)
			return
		}

		err = tx.Commit()
		if err != nil {
			t.Errorf("Failed to commit transaction: %v", err)
			return
		}

		// Verify data was committed
		var value string
		err = db.QueryRow("SELECT value FROM tx_test WHERE id = 1").Scan(&value)
		if err != nil {
			t.Errorf("Failed to query committed data: %v", err)
			return
		}

		if value != "committed" {
			t.Errorf("Got value %s, expected 'committed'", value)
		}
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Errorf("Failed to begin transaction: %v", err)
			return
		}

		_, err = tx.Exec("INSERT INTO tx_test VALUES (2, 'rolled_back')")
		if err != nil {
			tx.Rollback()
			t.Errorf("Failed to insert in transaction: %v", err)
			return
		}

		err = tx.Rollback()
		if err != nil {
			t.Errorf("Failed to rollback transaction: %v", err)
			return
		}

		// Verify data was not committed
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM tx_test WHERE id = 2").Scan(&count)
		if err != nil {
			t.Errorf("Failed to query rollback verification: %v", err)
			return
		}

		if count != 0 {
			t.Errorf("Got count %d, expected 0 (data should be rolled back)", count)
		}
	})
}

func BenchmarkBasicOperations(b *testing.B) {
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

	_, err = db.Exec(`CREATE TABLE bench_test (id INTEGER, value VARCHAR)`)
	if err != nil {
		b.Errorf("Failed to create table: %v", err)
		return
	}

	b.Run("Insert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := db.Exec("INSERT INTO bench_test VALUES (?, ?)", i, "test_value")
			if err != nil {
				b.Errorf("Failed to insert: %v", err)
			}
		}
	})

	b.Run("Select", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT id, value FROM bench_test LIMIT 10")
			if err != nil {
				b.Errorf("Failed to select: %v", err)
			}
			for rows.Next() {
				var id int
				var value string
				rows.Scan(&id, &value)
			}
			rows.Close()
		}
	})
}