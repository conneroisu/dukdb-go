package types

import (
	"database/sql/driver"
	"fmt"
	"math/big"
	"strings"
)

// Decimal represents a DuckDB DECIMAL type
type Decimal struct {
	value *big.Int
	scale uint8
}

// NewDecimal creates a new Decimal from a string
func NewDecimal(s string) (*Decimal, error) {
	parts := strings.Split(s, ".")
	scale := uint8(0)
	
	if len(parts) > 2 {
		return nil, fmt.Errorf("invalid decimal format: %s", s)
	}
	
	// Remove decimal point and track scale
	if len(parts) == 2 {
		scale = uint8(len(parts[1]))
		s = parts[0] + parts[1]
	} else {
		s = parts[0]
	}
	
	// Parse as big int
	value := new(big.Int)
	_, ok := value.SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("invalid decimal value: %s", s)
	}
	
	return &Decimal{
		value: value,
		scale: scale,
	}, nil
}

// NewDecimalFromBigInt creates a Decimal from a big.Int with scale
func NewDecimalFromBigInt(value *big.Int, scale uint8) *Decimal {
	return &Decimal{
		value: new(big.Int).Set(value),
		scale: scale,
	}
}

// String returns the string representation of the decimal
func (d *Decimal) String() string {
	if d.value == nil {
		return "0"
	}
	
	str := d.value.String()
	if d.scale == 0 {
		return str
	}
	
	// Handle negative numbers
	negative := false
	if str[0] == '-' {
		negative = true
		str = str[1:]
	}
	
	// Pad with zeros if needed
	for len(str) <= int(d.scale) {
		str = "0" + str
	}
	
	// Insert decimal point
	insertPos := len(str) - int(d.scale)
	result := str[:insertPos] + "." + str[insertPos:]
	
	if negative {
		result = "-" + result
	}
	
	return result
}

// Float64 returns the decimal as a float64
func (d *Decimal) Float64() float64 {
	if d.value == nil {
		return 0
	}
	
	// Convert to float
	f := new(big.Float).SetInt(d.value)
	
	// Apply scale
	if d.scale > 0 {
		divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.scale)), nil))
		f.Quo(f, divisor)
	}
	
	result, _ := f.Float64()
	return result
}

// Value implements driver.Valuer
func (d Decimal) Value() (driver.Value, error) {
	return d.String(), nil
}

// Scan implements sql.Scanner
func (d *Decimal) Scan(value interface{}) error {
	if value == nil {
		d.value = nil
		d.scale = 0
		return nil
	}
	
	switch v := value.(type) {
	case string:
		dec, err := NewDecimal(v)
		if err != nil {
			return err
		}
		*d = *dec
		return nil
	case float64:
		// Convert float to decimal with automatic scale detection
		str := fmt.Sprintf("%f", v)
		// Remove trailing zeros
		str = strings.TrimRight(str, "0")
		str = strings.TrimRight(str, ".")
		dec, err := NewDecimal(str)
		if err != nil {
			return err
		}
		*d = *dec
		return nil
	case int64:
		d.value = big.NewInt(v)
		d.scale = 0
		return nil
	default:
		return fmt.Errorf("cannot scan %T into Decimal", value)
	}
}

// BigInt returns the underlying big.Int value
func (d *Decimal) BigInt() *big.Int {
	if d.value == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Set(d.value)
}

// Scale returns the scale of the decimal
func (d *Decimal) Scale() uint8 {
	return d.scale
}