package test

import (
	"testing"

	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

func TestVector(t *testing.T) {
	t.Run("Basic Vector Operations", func(t *testing.T) {
		// Create a vector for integers
		logicalType := storage.LogicalType{ID: storage.TypeInteger}
		vector := storage.NewFlatVector(logicalType, 10)

		// Set some values
		for i := 0; i < 5; i++ {
			err := vector.SetValue(i, int32(i*10))
			if err != nil {
				t.Fatalf("Failed to set value at position %d: %v", i, err)
			}
		}

		// Set a null value
		err := vector.SetValue(5, nil)
		if err != nil {
			t.Fatalf("Failed to set null value: %v", err)
		}

		// Verify values
		for i := 0; i < 5; i++ {
			val, err := vector.GetValue(i)
			if err != nil {
				t.Fatalf("Failed to get value at position %d: %v", i, err)
			}
			expected := int32(i * 10)
			if val != expected {
				t.Errorf("Position %d: expected %d, got %v", i, expected, val)
			}
		}

		// Verify null
		val, err := vector.GetValue(5)
		if err != nil {
			t.Fatalf("Failed to get null value: %v", err)
		}
		if val != nil {
			t.Errorf("Expected nil for null value, got %v", val)
		}
	})

	t.Run("String Vector", func(t *testing.T) {
		logicalType := storage.LogicalType{ID: storage.TypeVarchar}
		vector := storage.NewFlatVector(logicalType, 5)

		testStrings := []string{"hello", "world", "test", "vector", "storage"}

		// Set values
		for i, s := range testStrings {
			err := vector.SetValue(i, s)
			if err != nil {
				t.Fatalf("Failed to set string at position %d: %v", i, err)
			}
		}

		// Verify values
		for i, expected := range testStrings {
			val, err := vector.GetValue(i)
			if err != nil {
				t.Fatalf("Failed to get string at position %d: %v", i, err)
			}
			if val != expected {
				t.Errorf("Position %d: expected %s, got %v", i, expected, val)
			}
		}
	})

	t.Run("Decimal Vector", func(t *testing.T) {
		logicalType := storage.LogicalType{
			ID:    storage.TypeDecimal,
			Width: 10,
			Scale: 2,
		}
		vector := storage.NewFlatVector(logicalType, 3)

		decimals := []string{"123.45", "678.90", "0.12"}

		// Set values
		for i, d := range decimals {
			err := vector.SetValue(i, d)
			if err != nil {
				t.Fatalf("Failed to set decimal at position %d: %v", i, err)
			}
		}

		// Verify values
		for i, expected := range decimals {
			val, err := vector.GetValue(i)
			if err != nil {
				t.Fatalf("Failed to get decimal at position %d: %v", i, err)
			}
			if val != expected {
				t.Errorf("Position %d: expected %s, got %v", i, expected, val)
			}
		}
	})
}

func TestDataChunk(t *testing.T) {
	t.Run("Mixed Type DataChunk", func(t *testing.T) {
		// Define schema
		types := []storage.LogicalType{
			{ID: storage.TypeInteger},
			{ID: storage.TypeVarchar},
			{ID: storage.TypeDouble},
			{ID: storage.TypeBoolean},
		}

		chunk := storage.NewDataChunk(types, 100)

		// Add some rows
		testData := [][]interface{}{
			{int32(1), "Alice", 95.5, true},
			{int32(2), "Bob", 87.3, false},
			{int32(3), "Charlie", 92.1, true},
			{nil, "David", 88.9, nil}, // Row with nulls
		}

		// Set values
		for row, rowData := range testData {
			for col, value := range rowData {
				err := chunk.SetValue(col, row, value)
				if err != nil {
					t.Fatalf("Failed to set value at col %d, row %d: %v", col, row, err)
				}
			}
		}

		chunk.SetSize(len(testData))

		// Verify values
		for row, expectedRow := range testData {
			for col, expectedValue := range expectedRow {
				val, err := chunk.GetValue(col, row)
				if err != nil {
					t.Fatalf("Failed to get value at col %d, row %d: %v", col, row, err)
				}
				if val != expectedValue {
					t.Errorf("Col %d, Row %d: expected %v, got %v", col, row, expectedValue, val)
				}
			}
		}
	})

	t.Run("DataChunkBuilder", func(t *testing.T) {
		types := []storage.LogicalType{
			{ID: storage.TypeInteger},
			{ID: storage.TypeVarchar},
			{ID: storage.TypeDecimal, Width: 10, Scale: 2},
		}

		builder := storage.NewDataChunkBuilder(types, 10)

		// Add rows
		rows := [][]interface{}{
			{int32(100), "Product A", "19.99"},
			{int32(101), "Product B", "29.99"},
			{int32(102), "Product C", "39.99"},
		}

		for _, row := range rows {
			err := builder.AppendRow(row)
			if err != nil {
				t.Fatalf("Failed to append row: %v", err)
			}
		}

		chunk := builder.Build()

		// Verify the chunk
		if chunk.ColumnCount() != 3 {
			t.Errorf("Expected 3 columns, got %d", chunk.ColumnCount())
		}

		// Verify first row
		val0, _ := chunk.GetValue(0, 0)
		val1, _ := chunk.GetValue(1, 0)
		val2, _ := chunk.GetValue(2, 0)

		if val0 != int32(100) {
			t.Errorf("Expected 100, got %v", val0)
		}
		if val1 != "Product A" {
			t.Errorf("Expected 'Product A', got %v", val1)
		}
		if val2 != "19.99" {
			t.Errorf("Expected '19.99', got %v", val2)
		}
	})
}

func TestValidityMask(t *testing.T) {
	mask := storage.NewValidityMask(100)

	// Test setting valid/invalid
	mask.SetValid(0)
	mask.SetValid(5)
	mask.SetInvalid(10)
	mask.SetValid(63)
	mask.SetValid(64) // Cross uint64 boundary
	mask.SetInvalid(99)

	// Verify
	if !mask.IsValid(0) {
		t.Error("Position 0 should be valid")
	}
	if !mask.IsValid(5) {
		t.Error("Position 5 should be valid")
	}
	if mask.IsValid(10) {
		t.Error("Position 10 should be invalid")
	}
	if !mask.IsValid(63) {
		t.Error("Position 63 should be valid")
	}
	if !mask.IsValid(64) {
		t.Error("Position 64 should be valid")
	}
	if mask.IsValid(99) {
		t.Error("Position 99 should be invalid")
	}

	// Test unset positions (should be invalid by default)
	if mask.IsValid(50) {
		t.Error("Unset position 50 should be invalid")
	}
}