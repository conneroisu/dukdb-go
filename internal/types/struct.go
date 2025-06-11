package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
)

// FieldSchema defines the expected type for a struct field
type FieldSchema struct {
	Name     string
	Type     reflect.Type
	Required bool
}

// StructSchema defines the complete schema for a struct
type StructSchema struct {
	fields map[string]FieldSchema
	order  []string
}

// NewStructSchema creates a new struct schema
func NewStructSchema() *StructSchema {
	return &StructSchema{
		fields: make(map[string]FieldSchema),
		order:  make([]string, 0),
	}
}

// AddField adds a field to the schema
func (ss *StructSchema) AddField(name string, fieldType reflect.Type, required bool) {
	schema := FieldSchema{
		Name:     name,
		Type:     fieldType,
		Required: required,
	}

	// Track field order
	if _, exists := ss.fields[name]; !exists {
		ss.order = append(ss.order, name)
	}

	ss.fields[name] = schema
}

// ValidateValue validates a value against the field schema
func (fs FieldSchema) ValidateValue(value interface{}) error {
	if value == nil {
		if fs.Required {
			return fmt.Errorf("field %s is required but got nil", fs.Name)
		}
		return nil
	}

	valueType := reflect.TypeOf(value)
	if !valueType.AssignableTo(fs.Type) {
		return fmt.Errorf("field %s expects type %s but got %s", fs.Name, fs.Type, valueType)
	}

	return nil
}

// Struct represents a DuckDB STRUCT type with optional schema validation
type Struct struct {
	fields map[string]interface{}
	order  []string      // Preserve field order
	schema *StructSchema // Optional schema for validation
}

// NewStruct creates a new Struct
func NewStruct() *Struct {
	return &Struct{
		fields: make(map[string]interface{}),
		order:  make([]string, 0),
		schema: nil,
	}
}

// NewStructWithSchema creates a new Struct with schema validation
func NewStructWithSchema(schema *StructSchema) *Struct {
	return &Struct{
		fields: make(map[string]interface{}),
		order:  make([]string, 0),
		schema: schema,
	}
}

// NewStructFromMap creates a Struct from a map
func NewStructFromMap(m map[string]interface{}) (*Struct, error) {
	s := NewStruct()
	for k, v := range m {
		if err := s.Set(k, v); err != nil {
			return nil, fmt.Errorf("failed to set field %s: %w", k, err)
		}
	}
	return s, nil
}

// newStructFromMapUnsafe creates a Struct from a map without validation (for internal use)
func newStructFromMapUnsafe(m map[string]interface{}) *Struct {
	s := NewStruct()
	for k, v := range m {
		// Use unsafe direct assignment to avoid validation errors in legacy code
		if s.fields == nil {
			s.fields = make(map[string]interface{})
		}
		if _, exists := s.fields[k]; !exists {
			s.order = append(s.order, k)
		}
		s.fields[k] = v
	}
	return s
}

// Set sets a field value with optional schema validation
func (s *Struct) Set(name string, value interface{}) error {
	if s.fields == nil {
		s.fields = make(map[string]interface{})
	}

	// Validate against schema if present
	if s.schema != nil {
		fieldSchema, exists := s.schema.fields[name]
		if exists {
			if err := fieldSchema.ValidateValue(value); err != nil {
				return err
			}
		} else {
			// Field not in schema - could be an error or allowed depending on strictness
			// For now, we'll allow it but could add a strict mode later
		}
	}

	// Track field order
	if _, exists := s.fields[name]; !exists {
		s.order = append(s.order, name)
	}

	s.fields[name] = value
	return nil
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
		*s = *newStructFromMapUnsafe(m)
		return nil
	case string:
		// Try to unmarshal as JSON
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			return err
		}
		*s = *newStructFromMapUnsafe(m)
		return nil
	case map[string]interface{}:
		*s = *newStructFromMapUnsafe(v)
		return nil
	default:
		return fmt.Errorf("cannot scan %T into Struct", value)
	}
}
