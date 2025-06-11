package types

import (
	"database/sql/driver"
	"fmt"
	"math/big"
	"strings"
)

// Decimal represents a DuckDB DECIMAL type with precision and scale
type Decimal struct {
	value     *big.Int
	precision uint8 // Total number of digits (1-38 for DuckDB)
	scale     uint8 // Number of digits after decimal point
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

	// Use default precision based on detected digits
	digits := len(strings.ReplaceAll(s, ".", ""))
	if strings.HasPrefix(s, "-") {
		digits-- // Don't count the negative sign
	}
	precision := uint8(digits)
	if precision == 0 {
		precision = 1
	}

	return &Decimal{
		value:     value,
		precision: precision,
		scale:     scale,
	}, nil
}

// NewDecimalFromBigInt creates a Decimal from a big.Int with scale and default precision
func NewDecimalFromBigInt(value *big.Int, scale uint8) *Decimal {
	// Calculate precision from the number of digits
	digits := len(value.String())
	if value.Sign() < 0 {
		digits-- // Don't count the negative sign
	}
	precision := uint8(digits)
	if precision == 0 {
		precision = 1
	}

	return &Decimal{
		value:     new(big.Int).Set(value),
		precision: precision,
		scale:     scale,
	}
}

// NewDecimalWithPrecision creates a new Decimal from a string with specific precision and scale
func NewDecimalWithPrecision(s string, precision, scale uint8) (*Decimal, error) {
	// Validate precision and scale
	if precision < 1 || precision > 38 {
		return nil, fmt.Errorf("precision must be between 1 and 38, got %d", precision)
	}
	if scale > precision {
		return nil, fmt.Errorf("scale %d cannot exceed precision %d", scale, precision)
	}

	// First create the decimal normally
	dec, err := NewDecimal(s)
	if err != nil {
		return nil, err
	}

	// Adjust scale if needed
	if dec.scale != scale {
		dec.adjustScale(scale)
	}

	// Validate that the value fits within the specified precision
	if err := dec.validatePrecisionScale(precision, scale); err != nil {
		return nil, err
	}

	// Set the precision and scale
	dec.precision = precision
	dec.scale = scale

	return dec, nil
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
		d.precision = 1
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
		d.precision = uint8(len(fmt.Sprintf("%d", v)))
		if v < 0 {
			d.precision-- // Don't count negative sign
		}
		if d.precision == 0 {
			d.precision = 1
		}
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

// Precision returns the precision of the decimal
func (d *Decimal) Precision() uint8 {
	return d.precision
}

// validatePrecisionScale validates that the decimal value fits within precision/scale constraints
func (d *Decimal) validatePrecisionScale(precision, scale uint8) error {
	if d.value == nil {
		return nil
	}

	// Calculate total digits in the value
	valueStr := d.value.String()
	if valueStr[0] == '-' {
		valueStr = valueStr[1:] // Remove negative sign
	}

	totalDigits := uint8(len(valueStr))
	if totalDigits > precision {
		return fmt.Errorf("value has %d digits, exceeds precision %d", totalDigits, precision)
	}

	return nil
}

// adjustScale adjusts the decimal's internal value to match a new scale
func (d *Decimal) adjustScale(newScale uint8) {
	if d.value == nil {
		d.scale = newScale
		return
	}

	if newScale > d.scale {
		// Increase scale - multiply by 10^(newScale - oldScale)
		multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(newScale-d.scale)), nil)
		d.value.Mul(d.value, multiplier)
	} else if newScale < d.scale {
		// Decrease scale - divide by 10^(oldScale - newScale)
		divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.scale-newScale)), nil)
		d.value.Div(d.value, divisor)
	}

	d.scale = newScale
}
