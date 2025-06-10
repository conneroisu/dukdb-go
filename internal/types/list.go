package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
)

// List represents a DuckDB LIST type
type List struct {
	values []interface{}
}

// NewList creates a new List from a slice
func NewList(values interface{}) (*List, error) {
	v := reflect.ValueOf(values)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("expected slice or array, got %T", values)
	}

	list := &List{
		values: make([]interface{}, v.Len()),
	}

	for i := 0; i < v.Len(); i++ {
		list.values[i] = v.Index(i).Interface()
	}

	return list, nil
}

// Values returns the underlying slice
func (l *List) Values() []interface{} {
	if l == nil {
		return nil
	}
	return l.values
}

// Len returns the number of elements
func (l *List) Len() int {
	if l == nil {
		return 0
	}
	return len(l.values)
}

// Get returns the element at the given index
func (l *List) Get(i int) (interface{}, error) {
	if l == nil || i < 0 || i >= len(l.values) {
		return nil, fmt.Errorf("index out of bounds: %d", i)
	}
	return l.values[i], nil
}

// String returns a string representation
func (l *List) String() string {
	if l == nil {
		return "[]"
	}
	return fmt.Sprintf("%v", l.values)
}

// Value implements driver.Valuer
func (l List) Value() (driver.Value, error) {
	if l.values == nil {
		return nil, nil
	}
	// Return as JSON for now
	return json.Marshal(l.values)
}

// Scan implements sql.Scanner
func (l *List) Scan(value interface{}) error {
	if value == nil {
		l.values = nil
		return nil
	}

	switch v := value.(type) {
	case []byte:
		// Try to unmarshal as JSON
		return json.Unmarshal(v, &l.values)
	case string:
		// Try to unmarshal as JSON
		return json.Unmarshal([]byte(v), &l.values)
	case []interface{}:
		l.values = v
		return nil
	default:
		// Try to convert single value to list
		l.values = []interface{}{value}
		return nil
	}
}
