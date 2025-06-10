package types

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"strings"
)

// UUID represents a DuckDB UUID type (128-bit)
type UUID [16]byte

// NewUUID creates a UUID from a string
func NewUUID(s string) (*UUID, error) {
	// Remove hyphens
	s = strings.ReplaceAll(s, "-", "")

	// Must be 32 hex characters
	if len(s) != 32 {
		return nil, fmt.Errorf("invalid UUID length: %d", len(s))
	}

	// Decode hex
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID hex: %w", err)
	}

	var uuid UUID
	copy(uuid[:], bytes)
	return &uuid, nil
}

// NewUUIDFromBytes creates a UUID from bytes
func NewUUIDFromBytes(b []byte) (*UUID, error) {
	if len(b) != 16 {
		return nil, fmt.Errorf("invalid UUID bytes length: %d", len(b))
	}

	var uuid UUID
	copy(uuid[:], b)
	return &uuid, nil
}

// String returns the standard UUID string representation
func (u UUID) String() string {
	// Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		u[0:4],
		u[4:6],
		u[6:8],
		u[8:10],
		u[10:16])
}

// Bytes returns the UUID as a byte slice
func (u UUID) Bytes() []byte {
	return u[:]
}

// IsZero returns true if the UUID is all zeros
func (u UUID) IsZero() bool {
	for _, b := range u {
		if b != 0 {
			return false
		}
	}
	return true
}

// Value implements driver.Valuer
func (u UUID) Value() (driver.Value, error) {
	if u.IsZero() {
		return nil, nil
	}
	return u.String(), nil
}

// Scan implements sql.Scanner
func (u *UUID) Scan(value interface{}) error {
	if value == nil {
		*u = UUID{}
		return nil
	}

	switch v := value.(type) {
	case string:
		uuid, err := NewUUID(v)
		if err != nil {
			return err
		}
		*u = *uuid
		return nil
	case []byte:
		if len(v) == 16 {
			// Raw bytes
			copy(u[:], v)
			return nil
		} else if len(v) == 36 {
			// String representation as bytes
			uuid, err := NewUUID(string(v))
			if err != nil {
				return err
			}
			*u = *uuid
			return nil
		}
		return fmt.Errorf("invalid UUID byte length: %d", len(v))
	default:
		return fmt.Errorf("cannot scan %T into UUID", value)
	}
}

// MarshalJSON implements json.Marshaler
func (u UUID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + u.String() + `"`), nil
}

// UnmarshalJSON implements json.Unmarshaler
func (u *UUID) UnmarshalJSON(data []byte) error {
	// Remove quotes
	s := string(data)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}

	uuid, err := NewUUID(s)
	if err != nil {
		return err
	}
	*u = *uuid
	return nil
}
