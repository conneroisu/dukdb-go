package test

import (
	"database/sql"
	"testing"

	_ "github.com/connerohnesorge/dukdb-go"
	"github.com/connerohnesorge/dukdb-go/internal/types"
)

func TestDecimalPrecisionScale(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		precision uint8
		scale     uint8
		wantErr   bool
	}{
		{"Valid decimal 10,2", "123.45", 10, 2, false},
		{"Valid decimal 5,3", "12.345", 5, 3, false},
		{"Valid integer as decimal", "123", 5, 0, false},
		{"Valid negative decimal", "-123.45", 10, 2, false},
		{"Valid zero", "0", 1, 0, false},
		{"Valid zero with scale", "0.00", 3, 2, false},
		{"Precision too high", "123.45", 39, 2, true}, // DuckDB max precision is 38
		{"Scale greater than precision", "123.45", 3, 5, true},
		{"Zero precision", "123.45", 0, 2, true},
		{"Value exceeds precision", "123456.78", 5, 2, true}, // 6 digits > precision 5
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decimal, err := types.NewDecimalWithPrecision(tt.input, tt.precision, tt.scale)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error for %s with precision=%d, scale=%d", tt.input, tt.precision, tt.scale)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if decimal.Precision() != tt.precision {
				t.Errorf("Expected precision %d, got %d", tt.precision, decimal.Precision())
			}

			if decimal.Scale() != tt.scale {
				t.Errorf("Expected scale %d, got %d", tt.scale, decimal.Scale())
			}
		})
	}
}

func TestDecimalStringConversion(t *testing.T) {
	tests := []struct {
		input     string
		precision uint8
		scale     uint8
		expected  string
	}{
		{"123.45", 10, 2, "123.45"},
		{"123", 5, 2, "123.00"},   // Should pad with zeros
		{"123.4", 5, 2, "123.40"}, // Should pad with one zero
		{"-123.45", 10, 2, "-123.45"},
		{"0", 3, 2, "0.00"},
		{"999.999", 6, 3, "999.999"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			decimal, err := types.NewDecimalWithPrecision(tt.input, tt.precision, tt.scale)
			if err != nil {
				t.Fatalf("Unexpected error creating decimal: %v", err)
			}

			result := decimal.String()
			if result != tt.expected {
				t.Errorf("Expected string %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestDecimalDriverValueScanner(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"String value", "123.45", "123.45"},
		{"Float64 value", 123.45, "123.45"},
		{"Int64 value", int64(123), "123"},
		{"Nil value", nil, "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var decimal types.Decimal
			err := decimal.Scan(tt.value)
			if err != nil {
				t.Fatalf("Scan failed: %v", err)
			}

			// Check driver.Valuer interface
			val, err := decimal.Value()
			if err != nil {
				t.Fatalf("Value() failed: %v", err)
			}

			if val != tt.want {
				t.Errorf("Expected %s, got %v", tt.want, val)
			}
		})
	}
}

func TestDecimalDatabaseIntegration(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with various decimal types
	_, err = db.Exec(`
		CREATE TABLE decimal_test (
			id INTEGER,
			price DECIMAL(10,2),
			rate DECIMAL(5,4),
			amount DECIMAL(18,6)
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test data
	testCases := []struct {
		id     int
		price  string
		rate   string
		amount string
	}{
		{1, "123.45", "0.1234", "1000.123456"},
		{2, "999.99", "0.9999", "999999.999999"},
		{3, "0.01", "0.0001", "0.000001"},
	}

	// Insert test data
	for _, tc := range testCases {
		_, err = db.Exec("INSERT INTO decimal_test VALUES (?, ?, ?, ?)",
			tc.id, tc.price, tc.rate, tc.amount)
		if err != nil {
			t.Fatalf("Failed to insert decimal data: %v", err)
		}
	}

	// Query and verify precision/scale metadata
	rows, err := db.Query("SELECT * FROM decimal_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query decimal data: %v", err)
	}
	defer rows.Close()

	// Check column type information
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("Failed to get column types: %v", err)
	}

	// Verify decimal column precision/scale
	expectedPrecisionScale := []struct {
		name      string
		precision int64
		scale     int64
	}{
		{"price", 10, 2},
		{"rate", 5, 4},
		{"amount", 18, 6},
	}

	for i, expected := range expectedPrecisionScale {
		// Skip the id column (index 0)
		colIndex := i + 1
		if colIndex >= len(columnTypes) {
			t.Fatalf("Not enough columns returned")
		}

		precision, scale, ok := columnTypes[colIndex].DecimalSize()
		if !ok {
			t.Errorf("Column %s should support decimal size", expected.name)
			continue
		}

		if precision != expected.precision {
			t.Errorf("Column %s: expected precision %d, got %d",
				expected.name, expected.precision, precision)
		}

		if scale != expected.scale {
			t.Errorf("Column %s: expected scale %d, got %d",
				expected.name, expected.scale, scale)
		}
	}

	// Verify data can be scanned correctly
	i := 0
	for rows.Next() {
		var id int
		var price, rate, amount string

		err = rows.Scan(&id, &price, &rate, &amount)
		if err != nil {
			t.Fatalf("Failed to scan decimal row: %v", err)
		}

		if i >= len(testCases) {
			t.Fatal("Too many rows returned")
		}

		tc := testCases[i]
		if id != tc.id {
			t.Errorf("ID mismatch: expected %d, got %d", tc.id, id)
		}

		// Note: We're scanning as string here to verify exact decimal representation
		// In practice, you might scan into types.Decimal for precise arithmetic
		if price != tc.price {
			t.Errorf("Price mismatch: expected %s, got %s", tc.price, price)
		}

		if rate != tc.rate {
			t.Errorf("Rate mismatch: expected %s, got %s", tc.rate, rate)
		}

		if amount != tc.amount {
			t.Errorf("Amount mismatch: expected %s, got %s", tc.amount, amount)
		}

		i++
	}

	if i != len(testCases) {
		t.Errorf("Expected %d rows, got %d", len(testCases), i)
	}
}

func TestDecimalArithmetic(t *testing.T) {
	// Test basic arithmetic operations with our decimal type
	decimal1, err := types.NewDecimalWithPrecision("123.45", 10, 2)
	if err != nil {
		t.Fatalf("Failed to create decimal1: %v", err)
	}

	decimal2, err := types.NewDecimalWithPrecision("67.89", 10, 2)
	if err != nil {
		t.Fatalf("Failed to create decimal2: %v", err)
	}

	// Test conversion to float64 for arithmetic
	float1 := decimal1.Float64()
	float2 := decimal2.Float64()

	expectedSum := 123.45 + 67.89
	actualSum := float1 + float2

	if actualSum != expectedSum {
		t.Errorf("Addition failed: expected %f, got %f", expectedSum, actualSum)
	}

	expectedProduct := 123.45 * 67.89
	actualProduct := float1 * float2

	// Allow for small floating point differences
	diff := actualProduct - expectedProduct
	if diff < 0 {
		diff = -diff
	}
	if diff > 0.001 {
		t.Errorf("Multiplication failed: expected %f, got %f", expectedProduct, actualProduct)
	}
}

func TestDecimalEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		test func(t *testing.T)
	}{
		{
			"Large precision decimal",
			func(t *testing.T) {
				// Test maximum precision (38 digits)
				largeNum := "12345678901234567890123456789012345678"
				decimal, err := types.NewDecimalWithPrecision(largeNum, 38, 0)
				if err != nil {
					t.Fatalf("Failed to create large decimal: %v", err)
				}

				if decimal.String() != largeNum {
					t.Errorf("Large decimal string mismatch: expected %s, got %s",
						largeNum, decimal.String())
				}
			},
		},
		{
			"High scale decimal",
			func(t *testing.T) {
				// Test high scale (many decimal places)
				decimal, err := types.NewDecimalWithPrecision("1.123456789012345678", 20, 18)
				if err != nil {
					t.Fatalf("Failed to create high-scale decimal: %v", err)
				}

				if decimal.Scale() != 18 {
					t.Errorf("Scale mismatch: expected 18, got %d", decimal.Scale())
				}
			},
		},
		{
			"Zero with scale",
			func(t *testing.T) {
				decimal, err := types.NewDecimalWithPrecision("0", 5, 3)
				if err != nil {
					t.Fatalf("Failed to create zero decimal: %v", err)
				}

				expected := "0.000"
				if decimal.String() != expected {
					t.Errorf("Zero decimal string: expected %s, got %s", expected, decimal.String())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.test)
	}
}
