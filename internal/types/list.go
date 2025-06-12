package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
)

// List represents a DuckDB LIST type with type-safe generic support
type List[T any] struct {
	values []T
}

// ListAny is an alias for backward compatibility with untyped lists
type ListAny = List[interface{}]

// NewList creates a new type-safe List from a slice
func NewList[T any](values []T) *List[T] {
	listValues := make([]T, len(values))
	copy(listValues, values)
	return &List[T]{
		values: listValues,
	}
}

// NewListAny creates a new List from any slice type (backward compatibility)
func NewListAny(values interface{}) (*ListAny, error) {
	v := reflect.ValueOf(values)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, fmt.Errorf("expected slice or array, got %T", values)
	}

	list := &ListAny{
		values: make([]interface{}, v.Len()),
	}

	for i := 0; i < v.Len(); i++ {
		list.values[i] = v.Index(i).Interface()
	}

	return list, nil
}

// Values returns the underlying slice
func (l *List[T]) Values() []T {
	if l == nil {
		return nil
	}
	return l.values
}

// ValuesAny returns the underlying slice as []interface{} for backward compatibility
func (l *List[T]) ValuesAny() []interface{} {
	if l == nil {
		return nil
	}
	result := make([]interface{}, len(l.values))
	for i, v := range l.values {
		result[i] = v
	}
	return result
}

// Len returns the number of elements
func (l *List[T]) Len() int {
	if l == nil {
		return 0
	}
	return len(l.values)
}

// Get returns the element at the given index
func (l *List[T]) Get(i int) (T, error) {
	var zero T
	if l == nil || i < 0 || i >= len(l.values) {
		return zero, fmt.Errorf("index out of bounds: %d", i)
	}
	return l.values[i], nil
}

// Set sets the element at the given index
func (l *List[T]) Set(i int, value T) error {
	if l == nil || i < 0 || i >= len(l.values) {
		return fmt.Errorf("index out of bounds: %d", i)
	}
	l.values[i] = value
	return nil
}

// Append adds elements to the end of the list
func (l *List[T]) Append(values ...T) {
	if l == nil {
		return
	}
	l.values = append(l.values, values...)
}

// String returns a string representation
func (l *List[T]) String() string {
	if l == nil {
		return "[]"
	}
	return fmt.Sprintf("%v", l.values)
}

// Value implements driver.Valuer
func (l List[T]) Value() (driver.Value, error) {
	if l.values == nil {
		return nil, nil
	}
	// Return as JSON for now
	return json.Marshal(l.values)
}

// Scan implements sql.Scanner
func (l *List[T]) Scan(value interface{}) error {
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
	case []T:
		l.values = v
		return nil
	case []interface{}:
		// Convert []interface{} to []T if possible
		l.values = make([]T, len(v))
		for i, val := range v {
			if typedVal, ok := val.(T); ok {
				l.values[i] = typedVal
			} else {
				return fmt.Errorf("cannot convert %T to %T", val, *new(T))
			}
		}
		return nil
	default:
		// Try to convert single value to list if it matches type T
		if typedVal, ok := value.(T); ok {
			l.values = []T{typedVal}
			return nil
		}
		return fmt.Errorf("cannot scan %T into List[%T]", value, *new(T))
	}
}

// MarshalJSON implements json.Marshaler
func (l *List[T]) MarshalJSON() ([]byte, error) {
	if l == nil {
		return []byte("null"), nil
	}
	return json.Marshal(l.values)
}

// UnmarshalJSON implements json.Unmarshaler
func (l *List[T]) UnmarshalJSON(data []byte) error {
	if l == nil {
		return fmt.Errorf("cannot unmarshal into nil List")
	}
	return json.Unmarshal(data, &l.values)
}
