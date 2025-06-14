package test

import (
	"testing"

	"github.com/connerohnesorge/dukdb-go/internal/types"
)

// TestDecimalUnit tests the decimal type implementation without database
func TestDecimalUnit(t *testing.T) {
	t.Run("NewDecimalWithPrecision", func(t *testing.T) {
		tests := []struct {
			name      string
			value     string
			precision uint8
			scale     uint8
			wantErr   bool
		}{
			{"Valid decimal", "123.45", 5, 2, false},
			{"Valid integer", "12345", 5, 0, false},
			{"Valid negative", "-123.45", 5, 2, false},
			{"Valid zero", "0", 1, 0, false},
			{"Valid zero with scale", "0.00", 3, 2, false},
			{"Precision too high", "123.45", 40, 2, true},
			{"Scale exceeds precision", "123.45", 5, 6, true},
			{"Zero precision", "123.45", 0, 0, true},
			{"Value exceeds precision", "12345.67", 5, 2, true}, // Total digits > precision
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dec, err := types.NewDecimalWithPrecision(tt.value, tt.precision, tt.scale)
				if tt.wantErr {
					if err == nil {
						t.Errorf("Expected error but got none")
					}
				} else {
					if err != nil {
						t.Errorf("Unexpected error: %v", err)
					} else {
						// Verify string representation
						str := dec.String()
						if str == "" {
							t.Errorf("Empty string representation")
						}
					}
				}
			})
		}
	})

	t.Run("Decimal String Conversion", func(t *testing.T) {
		tests := []struct {
			input    string
			expected string
		}{
			{"123.45", "123.45"},
			{"123", "123"},
			{"123.4", "123.4"},
			{"-123.45", "-123.45"},
			{"0", "0"},
			{"999.999", "999.999"},
		}

		for _, tt := range tests {
			t.Run(tt.input, func(t *testing.T) {
				dec, err := types.NewDecimal(tt.input)
				if err != nil {
					t.Fatalf("Failed to create decimal: %v", err)
				}

				result := dec.String()
				if result != tt.expected {
					t.Errorf("Expected %s, got %s", tt.expected, result)
				}
			})
		}
	})

	t.Run("Decimal Value Method", func(t *testing.T) {
		dec, err := types.NewDecimal("123.45")
		if err != nil {
			t.Fatalf("Failed to create decimal: %v", err)
		}

		// Test Value() method
		val, err := dec.Value()
		if err != nil {
			t.Fatalf("Value() returned error: %v", err)
		}

		// Should return string
		str, ok := val.(string)
		if !ok {
			t.Fatalf("Value() should return string, got %T", val)
		}

		if str != "123.45" {
			t.Errorf("Expected '123.45', got '%s'", str)
		}
	})

	t.Run("Decimal Scan Method", func(t *testing.T) {
		tests := []struct {
			name    string
			input   interface{}
			wantErr bool
		}{
			{"String value", "123.45", false},
			{"Float64 value", 123.45, false},
			{"Int64 value", int64(123), false},
			{"Nil value", nil, false},
			{"Invalid type", []byte("123"), true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var dec types.Decimal
				err := dec.Scan(tt.input)
				if tt.wantErr {
					if err == nil {
						t.Errorf("Expected error but got none")
					}
				} else {
					if err != nil {
						t.Errorf("Unexpected error: %v", err)
					}
				}
			})
		}
	})
}