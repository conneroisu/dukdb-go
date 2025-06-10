package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
)

// Map represents a DuckDB MAP type
type Map struct {
	entries map[interface{}]interface{}
	keys    []interface{} // Preserve key order
}

// NewMap creates a new Map
func NewMap() *Map {
	return &Map{
		entries: make(map[interface{}]interface{}),
		keys:    make([]interface{}, 0),
	}
}

// NewMapFromGo creates a Map from a Go map
func NewMapFromGo(m interface{}) (*Map, error) {
	result := NewMap()
	
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
func (m *Map) Set(key, value interface{}) {
	if m.entries == nil {
		m.entries = make(map[interface{}]interface{})
	}
	
	// Track key order if new key
	if _, exists := m.entries[key]; !exists {
		m.keys = append(m.keys, key)
	}
	
	m.entries[key] = value
}

// Get returns the value for a key
func (m *Map) Get(key interface{}) (interface{}, bool) {
	if m.entries == nil {
		return nil, false
	}
	val, ok := m.entries[key]
	return val, ok
}

// Delete removes a key-value pair
func (m *Map) Delete(key interface{}) {
	if m.entries == nil {
		return
	}
	
	if _, exists := m.entries[key]; exists {
		delete(m.entries, key)
		// Remove from keys slice
		for i, k := range m.keys {
			if k == key {
				m.keys = append(m.keys[:i], m.keys[i+1:]...)
				break
			}
		}
	}
}

// Keys returns all keys in order
func (m *Map) Keys() []interface{} {
	if m == nil {
		return nil
	}
	return m.keys
}

// Len returns the number of entries
func (m *Map) Len() int {
	if m == nil || m.entries == nil {
		return 0
	}
	return len(m.entries)
}

// ToGoMap converts to a Go map[string]interface{}
func (m *Map) ToGoMap() map[string]interface{} {
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

// String returns a string representation
func (m *Map) String() string {
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
func (m Map) Value() (driver.Value, error) {
	if m.entries == nil {
		return nil, nil
	}
	// Convert to string map for JSON
	return json.Marshal(m.ToGoMap())
}

// Scan implements sql.Scanner
func (m *Map) Scan(value interface{}) error {
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
		*m = *NewMap()
		for k, v := range jsonMap {
			m.Set(k, v)
		}
		return nil
	case string:
		// Try to unmarshal as JSON
		var jsonMap map[string]interface{}
		if err := json.Unmarshal([]byte(v), &jsonMap); err != nil {
			return err
		}
		*m = *NewMap()
		for k, v := range jsonMap {
			m.Set(k, v)
		}
		return nil
	case map[string]interface{}:
		*m = *NewMap()
		for k, v := range v {
			m.Set(k, v)
		}
		return nil
	default:
		return fmt.Errorf("cannot scan %T into Map", value)
	}
}