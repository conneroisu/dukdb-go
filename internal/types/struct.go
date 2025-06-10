package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// Struct represents a DuckDB STRUCT type
type Struct struct {
	fields map[string]interface{}
	order  []string // Preserve field order
}

// NewStruct creates a new Struct
func NewStruct() *Struct {
	return &Struct{
		fields: make(map[string]interface{}),
		order:  make([]string, 0),
	}
}

// NewStructFromMap creates a Struct from a map
func NewStructFromMap(m map[string]interface{}) *Struct {
	s := NewStruct()
	for k, v := range m {
		s.Set(k, v)
	}
	return s
}

// Set sets a field value
func (s *Struct) Set(name string, value interface{}) {
	if s.fields == nil {
		s.fields = make(map[string]interface{})
	}
	
	// Track field order
	if _, exists := s.fields[name]; !exists {
		s.order = append(s.order, name)
	}
	
	s.fields[name] = value
}

// Get returns a field value
func (s *Struct) Get(name string) (interface{}, bool) {
	if s.fields == nil {
		return nil, false
	}
	val, ok := s.fields[name]
	return val, ok
}

// Fields returns all fields as a map
func (s *Struct) Fields() map[string]interface{} {
	if s == nil || s.fields == nil {
		return make(map[string]interface{})
	}
	// Return a copy
	result := make(map[string]interface{})
	for k, v := range s.fields {
		result[k] = v
	}
	return result
}

// FieldNames returns field names in order
func (s *Struct) FieldNames() []string {
	if s == nil {
		return nil
	}
	return s.order
}

// String returns a string representation
func (s *Struct) String() string {
	if s == nil || s.fields == nil {
		return "{}"
	}
	
	// Build string with ordered fields
	result := "{"
	for i, name := range s.order {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%s: %v", name, s.fields[name])
	}
	result += "}"
	return result
}

// Value implements driver.Valuer
func (s Struct) Value() (driver.Value, error) {
	if s.fields == nil {
		return nil, nil
	}
	// Return as JSON
	return json.Marshal(s.fields)
}

// Scan implements sql.Scanner
func (s *Struct) Scan(value interface{}) error {
	if value == nil {
		s.fields = nil
		s.order = nil
		return nil
	}
	
	switch v := value.(type) {
	case []byte:
		// Try to unmarshal as JSON
		var m map[string]interface{}
		if err := json.Unmarshal(v, &m); err != nil {
			return err
		}
		*s = *NewStructFromMap(m)
		return nil
	case string:
		// Try to unmarshal as JSON
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			return err
		}
		*s = *NewStructFromMap(m)
		return nil
	case map[string]interface{}:
		*s = *NewStructFromMap(v)
		return nil
	default:
		return fmt.Errorf("cannot scan %T into Struct", value)
	}
}