package types

import (
	"database/sql/driver"
	"fmt"
	"math/big"
)

// Hugeint represents a DuckDB HUGEINT (128-bit integer)
type Hugeint struct {
	value *big.Int
}

// NewHugeint creates a new Hugeint from a string
func NewHugeint(s string) (*Hugeint, error) {
	value := new(big.Int)
	_, ok := value.SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("invalid hugeint value: %s", s)
	}
	
	// Check if it fits in 128 bits
	if value.BitLen() > 128 {
		return nil, fmt.Errorf("value too large for HUGEINT (128-bit): %s", s)
	}
	
	return &Hugeint{value: value}, nil
}

// NewHugeintFromBigInt creates a Hugeint from a big.Int
func NewHugeintFromBigInt(value *big.Int) (*Hugeint, error) {
	if value.BitLen() > 128 {
		return nil, fmt.Errorf("value too large for HUGEINT (128-bit)")
	}
	return &Hugeint{value: new(big.Int).Set(value)}, nil
}

// String returns the string representation
func (h *Hugeint) String() string {
	if h.value == nil {
		return "0"
	}
	return h.value.String()
}

// Int64 returns the value as int64 if it fits
func (h *Hugeint) Int64() (int64, error) {
	if h.value == nil {
		return 0, nil
	}
	if !h.value.IsInt64() {
		return 0, fmt.Errorf("hugeint value too large for int64")
	}
	return h.value.Int64(), nil
}

// Uint64 returns the value as uint64 if it fits
func (h *Hugeint) Uint64() (uint64, error) {
	if h.value == nil {
		return 0, nil
	}
	if !h.value.IsUint64() {
		return 0, fmt.Errorf("hugeint value too large for uint64")
	}
	return h.value.Uint64(), nil
}

// BigInt returns a copy of the underlying big.Int
func (h *Hugeint) BigInt() *big.Int {
	if h.value == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Set(h.value)
}

// Value implements driver.Valuer
func (h Hugeint) Value() (driver.Value, error) {
	return h.String(), nil
}

// Scan implements sql.Scanner
func (h *Hugeint) Scan(value interface{}) error {
	if value == nil {
		h.value = nil
		return nil
	}
	
	switch v := value.(type) {
	case string:
		hugeint, err := NewHugeint(v)
		if err != nil {
			return err
		}
		*h = *hugeint
		return nil
	case int64:
		h.value = big.NewInt(v)
		return nil
	case uint64:
		h.value = new(big.Int).SetUint64(v)
		return nil
	case []byte:
		// Handle as bytes (big-endian)
		h.value = new(big.Int).SetBytes(v)
		return nil
	default:
		return fmt.Errorf("cannot scan %T into Hugeint", value)
	}
}