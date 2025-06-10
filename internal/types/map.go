package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
)

// Map represents a DuckDB MAP type with type-safe generic support
type Map[K comparable, V any] struct {
	entries map[K]V
	keys    []K // Preserve key order
}

// MapAny is an alias for backward compatibility with untyped maps
type MapAny = Map[interface{}, interface{}]

// NewMap creates a new type-safe Map
func NewMap[K comparable, V any]() *Map[K, V] {
	return &Map[K, V]{
		entries: make(map[K]V),
		keys:    make([]K, 0),
	}
}

// NewMapAny creates a new Map with untyped keys/values (backward compatibility)
func NewMapAny() *MapAny {
	return &MapAny{
		entries: make(map[interface{}]interface{}),
		keys:    make([]interface{}, 0),
	}
}

// NewMapFromGo creates a Map from a Go map
func NewMapFromGo[K comparable, V any](m map[K]V) *Map[K, V] {
	result := NewMap[K, V]()
	for k, v := range m {
		result.Set(k, v)
	}
	return result
}

// NewMapAnyFromGo creates a MapAny from any Go map type (backward compatibility)
func NewMapAnyFromGo(m interface{}) (*MapAny, error) {
	result := NewMapAny()

	// Use reflection to handle any map type
	v := reflect.ValueOf(m)
	if v.Kind() != reflect.Map {
		return nil, fmt.Errorf("expected map, got %T", m)
	}

	for _, key := range v.MapKeys() {
		result.Set(key.Interface(), v.MapIndex(key).Interface())
	}

	return result, nil
}

// Set sets a key-value pair
func (m *Map[K, V]) Set(key K, value V) {
	if m.entries == nil {
		m.entries = make(map[K]V)
	}

	// Track key order if new key
	if _, exists := m.entries[key]; !exists {
		m.keys = append(m.keys, key)
	}

	m.entries[key] = value
}

// Get returns the value for a key
func (m *Map[K, V]) Get(key K) (V, bool) {
	var zero V
	if m.entries == nil {
		return zero, false
	}
	val, ok := m.entries[key]
	return val, ok
}

// Delete removes a key-value pair
func (m *Map[K, V]) Delete(key K) {
	if m.entries == nil {
		return
	}

	if _, exists := m.entries[key]; exists {
		delete(m.entries, key)
		// Remove from keys slice
		for i, k := range m.keys {
			if reflect.DeepEqual(k, key) {
				m.keys = append(m.keys[:i], m.keys[i+1:]...)
				break
			}
		}
	}
}

// Keys returns all keys in order
func (m *Map[K, V]) Keys() []K {
	if m == nil {
		return nil
	}
	return m.keys
}

// Len returns the number of entries
func (m *Map[K, V]) Len() int {
	if m == nil || m.entries == nil {
		return 0
	}
	return len(m.entries)
}

// ToGoMap converts to a Go map[string]interface{} for JSON serialization
func (m *Map[K, V]) ToGoMap() map[string]interface{} {
	if m == nil || m.entries == nil {
		return make(map[string]interface{})
	}

	result := make(map[string]interface{})
	for k, v := range m.entries {
		// Convert key to string
		keyStr := fmt.Sprintf("%v", k)
		result[keyStr] = v
	}
	return result
}

// ToNativeGoMap returns the underlying Go map
func (m *Map[K, V]) ToNativeGoMap() map[K]V {
	if m == nil || m.entries == nil {
		return make(map[K]V)
	}
	// Return a copy
	result := make(map[K]V)
	for k, v := range m.entries {
		result[k] = v
	}
	return result
}

// String returns a string representation
func (m *Map[K, V]) String() string {
	if m == nil || m.entries == nil {
		return "{}"
	}

	result := "{"
	for i, key := range m.keys {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%v: %v", key, m.entries[key])
	}
	result += "}"
	return result
}

// Value implements driver.Valuer
func (m Map[K, V]) Value() (driver.Value, error) {
	if m.entries == nil {
		return nil, nil
	}
	// Convert to string map for JSON
	return json.Marshal(m.ToGoMap())
}

// Scan implements sql.Scanner
func (m *Map[K, V]) Scan(value interface{}) error {
	if value == nil {
		m.entries = nil
		m.keys = nil
		return nil
	}

	switch v := value.(type) {
	case []byte:
		// Try to unmarshal as JSON
		var jsonMap map[string]interface{}
		if err := json.Unmarshal(v, &jsonMap); err != nil {
			return err
		}
		*m = *NewMap[K, V]()
		for k, val := range jsonMap {
			// Type conversion for key
			var key K
			if convertedKey, ok := interface{}(k).(K); ok {
				key = convertedKey
			} else {
				return fmt.Errorf("cannot convert key %T to %T", k, key)
			}
			
			// Type conversion for value
			var value V
			if convertedValue, ok := val.(V); ok {
				value = convertedValue
			} else {
				return fmt.Errorf("cannot convert value %T to %T", val, value)
			}
			
			m.Set(key, value)
		}
		return nil
	case string:
		// Try to unmarshal as JSON
		var jsonMap map[string]interface{}
		if err := json.Unmarshal([]byte(v), &jsonMap); err != nil {
			return err
		}
		*m = *NewMap[K, V]()
		for k, val := range jsonMap {
			// Type conversion for key
			var key K
			if convertedKey, ok := interface{}(k).(K); ok {
				key = convertedKey
			} else {
				return fmt.Errorf("cannot convert key %T to %T", k, key)
			}
			
			// Type conversion for value
			var value V
			if convertedValue, ok := val.(V); ok {
				value = convertedValue
			} else {
				return fmt.Errorf("cannot convert value %T to %T", val, value)
			}
			
			m.Set(key, value)
		}
		return nil
	case map[K]V:
		// Direct assignment for matching types
		*m = *NewMap[K, V]()
		for k, v := range v {
			m.Set(k, v)
		}
		return nil
	case map[string]interface{}:
		*m = *NewMap[K, V]()
		for k, val := range v {
			// Type conversion for key
			var key K
			if convertedKey, ok := interface{}(k).(K); ok {
				key = convertedKey
			} else {
				return fmt.Errorf("cannot convert key %T to %T", k, key)
			}
			
			// Type conversion for value
			var value V
			if convertedValue, ok := val.(V); ok {
				value = convertedValue
			} else {
				return fmt.Errorf("cannot convert value %T to %T", val, value)
			}
			
			m.Set(key, value)
		}
		return nil
	default:
		return fmt.Errorf("cannot scan %T into Map[%T, %T]", value, *new(K), *new(V))
	}
}
