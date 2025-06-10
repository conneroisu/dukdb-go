package test

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/connerohnesorge/dukdb-go"
)

func TestDateTimeTypes(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with date/time types
	_, err = db.Exec(`
		CREATE TABLE datetime_test (
			id INTEGER,
			date_col DATE,
			time_col TIME,
			timestamp_col TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test date
	testDate := time.Date(2023, 12, 25, 0, 0, 0, 0, time.UTC)
	// Test time (using Unix epoch date)
	testTime := time.Date(1970, 1, 1, 14, 30, 45, 0, time.UTC)
	// Test timestamp
	testTimestamp := time.Date(2023, 12, 25, 14, 30, 45, 123456000, time.UTC)

	// Insert data
	_, err = db.Exec("INSERT INTO datetime_test VALUES (?, ?, ?, ?)",
		1, testDate, testTime, testTimestamp)
	if err != nil {
		t.Fatalf("Failed to insert date/time data: %v", err)
	}

	// Query back
	var id int
	var date, timeVal, timestamp time.Time
	err = db.QueryRow("SELECT * FROM datetime_test WHERE id = 1").Scan(
		&id, &date, &timeVal, &timestamp)
	if err != nil {
		t.Fatalf("Failed to scan date/time data: %v", err)
	}

	// Check date (should only have date components)
	if date.Year() != testDate.Year() || date.Month() != testDate.Month() || date.Day() != testDate.Day() {
		t.Errorf("Date mismatch: expected %v, got %v", testDate, date)
	}

	// Check time (should only have time components)
	if timeVal.Hour() != testTime.Hour() || timeVal.Minute() != testTime.Minute() || timeVal.Second() != testTime.Second() {
		t.Errorf("Time mismatch: expected %v, got %v", testTime, timeVal)
	}

	// Check timestamp (allow for microsecond precision)
	diff := timestamp.Sub(testTimestamp).Abs()
	if diff > time.Microsecond {
		t.Errorf("Timestamp mismatch: expected %v, got %v (diff: %v)", testTimestamp, timestamp, diff)
	}
}

func TestBlobType(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with blob
	_, err = db.Exec(`
		CREATE TABLE blob_test (
			id INTEGER,
			data BLOB
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test data
	testData := []byte("Hello, DuckDB! 🦆")

	// Insert blob
	_, err = db.Exec("INSERT INTO blob_test VALUES (?, ?)", 1, testData)
	if err != nil {
		t.Fatalf("Failed to insert blob data: %v", err)
	}

	// Query back
	var id int
	var data []byte
	err = db.QueryRow("SELECT * FROM blob_test WHERE id = 1").Scan(&id, &data)
	if err != nil {
		t.Fatalf("Failed to scan blob data: %v", err)
	}

	// Compare
	if string(data) != string(testData) {
		t.Errorf("Blob data mismatch: expected %v, got %v", testData, data)
	}
}

func TestNullValues(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`
		CREATE TABLE null_test (
			id INTEGER,
			str_col VARCHAR,
			int_col INTEGER,
			float_col DOUBLE,
			bool_col BOOLEAN,
			blob_col BLOB,
			date_col DATE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert nulls
	_, err = db.Exec("INSERT INTO null_test VALUES (?, ?, ?, ?, ?, ?, ?)",
		1, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Failed to insert null values: %v", err)
	}

	// Query back using sql.Null types
	var id int
	var strVal sql.NullString
	var intVal sql.NullInt64
	var floatVal sql.NullFloat64
	var boolVal sql.NullBool
	var blobVal []byte
	var dateVal sql.NullTime

	err = db.QueryRow("SELECT * FROM null_test WHERE id = 1").Scan(
		&id, &strVal, &intVal, &floatVal, &boolVal, &blobVal, &dateVal)
	if err != nil {
		t.Fatalf("Failed to scan null values: %v", err)
	}

	// Check all are null
	if strVal.Valid || intVal.Valid || floatVal.Valid || boolVal.Valid || dateVal.Valid {
		t.Error("Expected all values to be null")
	}
	if blobVal != nil {
		t.Error("Expected blob to be nil")
	}
}

func TestPreparedStatements(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(`
		CREATE TABLE prepared_test (
			id INTEGER,
			name VARCHAR,
			value DOUBLE
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Prepare statement
	stmt, err := db.Prepare("INSERT INTO prepared_test VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute multiple times
	testData := []struct {
		id    int
		name  string
		value float64
	}{
		{1, "first", 1.23},
		{2, "second", 4.56},
		{3, "third", 7.89},
	}

	for _, td := range testData {
		_, err = stmt.Exec(td.id, td.name, td.value)
		if err != nil {
			t.Fatalf("Failed to execute prepared statement: %v", err)
		}
	}

	// Verify data
	rows, err := db.Query("SELECT * FROM prepared_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var id int
		var name string
		var value float64
		err = rows.Scan(&id, &name, &value)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		if i >= len(testData) {
			t.Fatal("Too many rows")
		}

		expected := testData[i]
		if id != expected.id || name != expected.name || value != expected.value {
			t.Errorf("Row %d mismatch: expected %v, got (%d, %s, %f)",
				i, expected, id, name, value)
		}
		i++
	}

	if i != len(testData) {
		t.Errorf("Expected %d rows, got %d", len(testData), i)
	}
}

func TestParameterizedQueries(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create and populate table
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER,
			name VARCHAR,
			age INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test parameterized query
	rows, err := db.Query("SELECT name, age FROM users WHERE age > ? ORDER BY age", 28)
	if err != nil {
		t.Fatalf("Failed to execute parameterized query: %v", err)
	}
	defer rows.Close()

	expected := []struct {
		name string
		age  int
	}{
		{"Alice", 30},
		{"Charlie", 35},
	}

	i := 0
	for rows.Next() {
		var name string
		var age int
		err = rows.Scan(&name, &age)
		if err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}

		if i >= len(expected) {
			t.Fatal("Too many rows")
		}

		if name != expected[i].name || age != expected[i].age {
			t.Errorf("Row %d: expected %v, got (%s, %d)",
				i, expected[i], name, age)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("Expected %d rows, got %d", len(expected), i)
	}
}