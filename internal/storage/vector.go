package storage

import (
	"fmt"
	"time"
	"unsafe"

	"github.com/connerohnesorge/dukdb-go/internal/types"
)

// VectorFormat represents different vector storage formats
type VectorFormat int

const (
	// FlatVector - standard contiguous array storage
	FlatVector VectorFormat = iota
	// ConstantVector - single value repeated for all positions
	ConstantVector
	// DictionaryVector - compressed storage with indices into dictionary
	DictionaryVector
	// SequenceVector - generated sequence (e.g., 1,2,3,...)
	SequenceVector
)

// DefaultVectorSize is the default number of elements in a vector
const DefaultVectorSize = 1024

// LogicalType represents the logical data type of a vector
type LogicalType struct {
	ID        TypeID
	Width     uint8  // For DECIMAL types
	Scale     uint8  // For DECIMAL types
	Collation string // For STRING types
}

// TypeID represents the fundamental data type
type TypeID int

const (
	TypeInvalid TypeID = iota
	TypeBoolean
	TypeTinyInt
	TypeSmallInt
	TypeInteger
	TypeBigInt
	TypeUTinyInt
	TypeUSmallInt
	TypeUInteger
	TypeUBigInt
	TypeFloat
	TypeDouble
	TypeTimestamp
	TypeDate
	TypeTime
	TypeInterval
	TypeHugeInt
	TypeVarchar
	TypeBlob
	TypeDecimal
	TypeTimestampS
	TypeTimestampMS
	TypeTimestampNS
	TypeEnum
	TypeList
	TypeStruct
	TypeMap
	TypeUUID
	TypeJSON
)

// ValidityMask tracks null values in a vector
type ValidityMask struct {
	bits []uint64
}

// NewValidityMask creates a new validity mask for the given size
func NewValidityMask(size int) *ValidityMask {
	// Calculate number of uint64s needed (64 bits per uint64)
	numBits := (size + 63) / 64
	return &ValidityMask{
		bits: make([]uint64, numBits),
	}
}

// SetValid marks a position as valid (not null)
func (v *ValidityMask) SetValid(pos int) {
	idx := pos / 64
	bit := pos % 64
	v.bits[idx] |= (1 << bit)
}

// SetInvalid marks a position as invalid (null)
func (v *ValidityMask) SetInvalid(pos int) {
	idx := pos / 64
	bit := pos % 64
	v.bits[idx] &= ^(1 << bit)
}

// IsValid checks if a position is valid (not null)
func (v *ValidityMask) IsValid(pos int) bool {
	idx := pos / 64
	bit := pos % 64
	return (v.bits[idx] & (1 << bit)) != 0
}

// Vector represents a columnar storage unit
type Vector struct {
	logicalType LogicalType
	format      VectorFormat
	size        int
	capacity    int

	// Data storage - varies by format
	data      unsafe.Pointer
	validity  *ValidityMask
}

// GetLogicalType returns the logical type of the vector
func (v *Vector) GetLogicalType() LogicalType {
	return v.logicalType
}

// NewVector creates a new vector with the specified type and size
func NewVector(logicalType LogicalType, size int) *Vector {
	return &Vector{
		logicalType: logicalType,
		format:      FlatVector,
		size:        size,
		capacity:    size,
		validity:    NewValidityMask(size),
	}
}

// NewFlatVector creates a flat vector with allocated storage
func NewFlatVector(logicalType LogicalType, size int) *Vector {
	v := NewVector(logicalType, size)
	// Allocate data based on type
	switch logicalType.ID {
	case TypeBoolean:
		data := make([]bool, size)
		v.data = unsafe.Pointer(&data)
	case TypeTinyInt:
		data := make([]int8, size)
		v.data = unsafe.Pointer(&data)
	case TypeSmallInt:
		data := make([]int16, size)
		v.data = unsafe.Pointer(&data)
	case TypeInteger:
		data := make([]int32, size)
		v.data = unsafe.Pointer(&data)
	case TypeBigInt:
		data := make([]int64, size)
		v.data = unsafe.Pointer(&data)
	case TypeFloat:
		data := make([]float32, size)
		v.data = unsafe.Pointer(&data)
	case TypeDouble:
		data := make([]float64, size)
		v.data = unsafe.Pointer(&data)
	case TypeVarchar:
		// For strings, we'll use a slice of strings
		data := make([]string, size)
		v.data = unsafe.Pointer(&data)
	case TypeDecimal:
		// For decimals, store as string representations
		data := make([]string, size)
		v.data = unsafe.Pointer(&data)
	case TypeDate:
		// For dates, store as int32 (days since epoch)
		data := make([]int32, size)
		v.data = unsafe.Pointer(&data)
	case TypeTime:
		// For time, store as int64 (microseconds since midnight)
		data := make([]int64, size)
		v.data = unsafe.Pointer(&data)
	case TypeTimestamp:
		// For timestamp, store as int64 (microseconds since epoch)
		data := make([]int64, size)
		v.data = unsafe.Pointer(&data)
	case TypeBlob:
		// For BLOB, store as byte slice
		data := make([][]byte, size)
		v.data = unsafe.Pointer(&data)
	case TypeUUID, TypeList, TypeStruct, TypeMap:
		// For complex types, store as string (JSON or string representation)
		data := make([]string, size)
		v.data = unsafe.Pointer(&data)
	default:
		// Default to byte array
		data := make([]byte, size)
		v.data = unsafe.Pointer(&data)
	}
	
	return v
}

// GetValue retrieves the value at the specified position
func (v *Vector) GetValue(pos int) (interface{}, error) {
	if pos < 0 || pos >= v.size {
		return nil, fmt.Errorf("position %d out of bounds [0, %d)", pos, v.size)
	}

	// Check null
	if !v.validity.IsValid(pos) {
		return nil, nil
	}

	switch v.format {
	case FlatVector:
		return v.getFlatValue(pos)
	case ConstantVector:
		return v.getFlatValue(0) // Constant vectors store single value at position 0
	default:
		return nil, fmt.Errorf("unsupported vector format: %v", v.format)
	}
}

// getFlatValue retrieves a value from flat vector storage
func (v *Vector) getFlatValue(pos int) (interface{}, error) {
	switch v.logicalType.ID {
	case TypeBoolean:
		return (*(*[]bool)(v.data))[pos], nil
	case TypeTinyInt:
		return (*(*[]int8)(v.data))[pos], nil
	case TypeSmallInt:
		return (*(*[]int16)(v.data))[pos], nil
	case TypeInteger:
		return (*(*[]int32)(v.data))[pos], nil
	case TypeBigInt:
		return (*(*[]int64)(v.data))[pos], nil
	case TypeFloat:
		return (*(*[]float32)(v.data))[pos], nil
	case TypeDouble:
		return (*(*[]float64)(v.data))[pos], nil
	case TypeVarchar:
		return (*(*[]string)(v.data))[pos], nil
	case TypeDecimal:
		return (*(*[]string)(v.data))[pos], nil
	case TypeDate:
		return (*(*[]int32)(v.data))[pos], nil
	case TypeTime:
		return (*(*[]int64)(v.data))[pos], nil
	case TypeTimestamp:
		return (*(*[]int64)(v.data))[pos], nil
	case TypeBlob:
		return (*(*[][]byte)(v.data))[pos], nil
	case TypeUUID, TypeList, TypeStruct, TypeMap:
		// Complex types are stored as strings, return the string value
		return (*(*[]string)(v.data))[pos], nil
	default:
		return nil, fmt.Errorf("unsupported type for GetValue: %v", v.logicalType.ID)
	}
}

// SetValue sets the value at the specified position
func (v *Vector) SetValue(pos int, value interface{}) error {
	if pos < 0 || pos >= v.capacity {
		return fmt.Errorf("position %d out of bounds [0, %d)", pos, v.capacity)
	}

	if value == nil {
		v.validity.SetInvalid(pos)
		return nil
	}

	v.validity.SetValid(pos)

	switch v.format {
	case FlatVector:
		return v.setFlatValue(pos, value)
	case ConstantVector:
		if pos != 0 {
			return fmt.Errorf("constant vector can only set value at position 0")
		}
		return v.setFlatValue(0, value)
	default:
		return fmt.Errorf("unsupported vector format: %v", v.format)
	}
}

// setFlatValue sets a value in flat vector storage
func (v *Vector) setFlatValue(pos int, value interface{}) error {
	switch v.logicalType.ID {
	case TypeBoolean:
		val, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
		(*(*[]bool)(v.data))[pos] = val
	case TypeTinyInt:
		val, ok := value.(int8)
		if !ok {
			return fmt.Errorf("expected int8, got %T", value)
		}
		(*(*[]int8)(v.data))[pos] = val
	case TypeSmallInt:
		val, ok := value.(int16)
		if !ok {
			return fmt.Errorf("expected int16, got %T", value)
		}
		(*(*[]int16)(v.data))[pos] = val
	case TypeInteger:
		val, ok := value.(int32)
		if !ok {
			// Try to convert from int
			if intVal, ok := value.(int); ok {
				val = int32(intVal)
			} else if floatVal, ok := value.(float64); ok {
				// Convert float64 to int32 if it's a whole number
				if floatVal == float64(int32(floatVal)) {
					val = int32(floatVal)
				} else {
					return fmt.Errorf("float64 value %g cannot be converted to int32", floatVal)
				}
			} else {
				return fmt.Errorf("expected int32, got %T", value)
			}
		}
		(*(*[]int32)(v.data))[pos] = val
	case TypeBigInt:
		val, ok := value.(int64)
		if !ok {
			// Try to convert from int
			if intVal, ok := value.(int); ok {
				val = int64(intVal)
			} else {
				return fmt.Errorf("expected int64, got %T", value)
			}
		}
		(*(*[]int64)(v.data))[pos] = val
	case TypeFloat:
		val, ok := value.(float32)
		if !ok {
			return fmt.Errorf("expected float32, got %T", value)
		}
		(*(*[]float32)(v.data))[pos] = val
	case TypeDouble:
		val, ok := value.(float64)
		if !ok {
			return fmt.Errorf("expected float64, got %T", value)
		}
		(*(*[]float64)(v.data))[pos] = val
	case TypeVarchar:
		var val string
		switch v := value.(type) {
		case string:
			val = v
		case int32:
			val = fmt.Sprintf("%d", v)
		case int64:
			val = fmt.Sprintf("%d", v)
		case float64:
			// Convert float64 to string, but handle integers properly
			if v == float64(int64(v)) {
				val = fmt.Sprintf("%.0f", v)
			} else {
				val = fmt.Sprintf("%g", v)
			}
		case bool:
			if v {
				val = "true"
			} else {
				val = "false"
			}
		default:
			return fmt.Errorf("expected string, got %T (value: %v)", value, value)
		}
		(*(*[]string)(v.data))[pos] = val
	case TypeDecimal:
		// Accept string representation of decimal
		val, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string for decimal, got %T", value)
		}
		(*(*[]string)(v.data))[pos] = val
	case TypeDate:
		val, ok := value.(int32)
		if !ok {
			// Try to convert from int
			if intVal, ok := value.(int); ok {
				val = int32(intVal)
			} else if timeVal, ok := value.(time.Time); ok {
				// Convert time.Time to days since Unix epoch (DuckDB DATE format)
				val = int32(timeVal.Unix() / 86400) // 86400 seconds per day
			} else if strVal, ok := value.(string); ok {
				// Parse date string (YYYY-MM-DD format)
				parsedTime, err := time.Parse("2006-01-02", strVal)
				if err != nil {
					return fmt.Errorf("failed to parse date string '%s': %w", strVal, err)
				}
				val = int32(parsedTime.Unix() / 86400) // Convert to days since Unix epoch
			} else {
				return fmt.Errorf("expected int32, time.Time or date string for date, got %T", value)
			}
		}
		(*(*[]int32)(v.data))[pos] = val
	case TypeTime:
		switch val := value.(type) {
		case int64:
			(*(*[]int64)(v.data))[pos] = val
		case time.Time:
			// Convert time.Time to microseconds since midnight
			// For TIME, we only care about the time-of-day portion
			midnight := time.Date(val.Year(), val.Month(), val.Day(), 0, 0, 0, 0, val.Location())
			microsecondsSinceMidnight := val.Sub(midnight).Microseconds()
			(*(*[]int64)(v.data))[pos] = microsecondsSinceMidnight
		default:
			return fmt.Errorf("expected int64 or time.Time for time, got %T", value)
		}
	case TypeTimestamp:
		switch val := value.(type) {
		case int64:
			(*(*[]int64)(v.data))[pos] = val
		case time.Time:
			// Convert time.Time to microseconds since Unix epoch
			(*(*[]int64)(v.data))[pos] = val.UnixMicro()
		default:
			return fmt.Errorf("expected int64 or time.Time for timestamp, got %T", value)
		}
	case TypeBlob:
		// Accept []byte or string (hex/base64)
		switch val := value.(type) {
		case []byte:
			(*(*[][]byte)(v.data))[pos] = val
		case string:
			// If it's a string, treat it as the raw bytes (could be encoded)
			(*(*[][]byte)(v.data))[pos] = []byte(val)
		default:
			return fmt.Errorf("expected []byte or string for BLOB type, got %T", value)
		}
	case TypeUUID:
		// Accept UUID object or string representation
		switch val := value.(type) {
		case *types.UUID:
			(*(*[]string)(v.data))[pos] = val.String()
		case string:
			(*(*[]string)(v.data))[pos] = val
		default:
			return fmt.Errorf("expected UUID or string for UUID type, got %T", value)
		}
	case TypeList:
		// Accept List object, []interface{}, or JSON string
		switch val := value.(type) {
		case *types.ListAny:
			jsonBytes, err := val.MarshalJSON()
			if err != nil {
				return fmt.Errorf("failed to marshal List to JSON: %w", err)
			}
			(*(*[]string)(v.data))[pos] = string(jsonBytes)
		case []interface{}:
			// Convert []interface{} to ListAny and marshal
			list, err := types.NewListAny(val)
			if err != nil {
				return fmt.Errorf("failed to create List from []interface{}: %w", err)
			}
			jsonBytes, err := list.MarshalJSON()
			if err != nil {
				return fmt.Errorf("failed to marshal List to JSON: %w", err)
			}
			(*(*[]string)(v.data))[pos] = string(jsonBytes)
		case string:
			// Assume it's already JSON
			(*(*[]string)(v.data))[pos] = val
		default:
			return fmt.Errorf("expected List, []interface{}, or string for List type, got %T", value)
		}
	case TypeStruct:
		// Accept Struct object, map[string]interface{}, or JSON string
		switch val := value.(type) {
		case *types.Struct:
			jsonBytes, err := val.MarshalJSON()
			if err != nil {
				return fmt.Errorf("failed to marshal Struct to JSON: %w", err)
			}
			(*(*[]string)(v.data))[pos] = string(jsonBytes)
		case map[string]interface{}:
			// Convert map[string]interface{} to Struct and marshal
			structVal, err := types.NewStructFromMap(val)
			if err != nil {
				return fmt.Errorf("failed to create Struct from map: %w", err)
			}
			jsonBytes, err := structVal.MarshalJSON()
			if err != nil {
				return fmt.Errorf("failed to marshal Struct to JSON: %w", err)
			}
			(*(*[]string)(v.data))[pos] = string(jsonBytes)
		case string:
			// Assume it's already JSON
			(*(*[]string)(v.data))[pos] = val
		default:
			return fmt.Errorf("expected Struct, map[string]interface{}, or string for Struct type, got %T", value)
		}
	case TypeMap:
		// Accept Map object or JSON string
		switch val := value.(type) {
		case *types.MapAny:
			jsonBytes, err := val.MarshalJSON()
			if err != nil {
				return fmt.Errorf("failed to marshal Map to JSON: %w", err)
			}
			(*(*[]string)(v.data))[pos] = string(jsonBytes)
		case string:
			// Assume it's already JSON
			(*(*[]string)(v.data))[pos] = val
		default:
			return fmt.Errorf("expected Map or string for Map type, got %T", value)
		}
	default:
		return fmt.Errorf("unsupported type for SetValue: %v", v.logicalType.ID)
	}
	return nil
}

// DataChunk represents a collection of vectors forming a batch of rows
type DataChunk struct {
	vectors    []*Vector
	size       int
	capacity   int
	selection  []int // Optional selection vector for filtered results
}

// NewDataChunk creates a new data chunk with the specified schema
func NewDataChunk(types []LogicalType, capacity int) *DataChunk {
	vectors := make([]*Vector, len(types))
	for i, t := range types {
		vectors[i] = NewFlatVector(t, capacity)
	}
	
	return &DataChunk{
		vectors:  vectors,
		size:     0,
		capacity: capacity,
	}
}

// ColumnCount returns the number of columns
func (d *DataChunk) ColumnCount() int {
	return len(d.vectors)
}

// SetSize sets the active size of the chunk
func (d *DataChunk) SetSize(size int) error {
	if size > d.capacity {
		return fmt.Errorf("size %d exceeds capacity %d", size, d.capacity)
	}
	d.size = size
	return nil
}

// GetVector returns the vector at the specified column index
func (d *DataChunk) GetVector(col int) (*Vector, error) {
	if col < 0 || col >= len(d.vectors) {
		return nil, fmt.Errorf("column %d out of bounds [0, %d)", col, len(d.vectors))
	}
	return d.vectors[col], nil
}

// GetValue retrieves a value from the chunk
func (d *DataChunk) GetValue(col, row int) (interface{}, error) {
	vector, err := d.GetVector(col)
	if err != nil {
		return nil, err
	}
	
	// Apply selection vector if present
	if d.selection != nil {
		if row >= len(d.selection) {
			return nil, fmt.Errorf("row %d out of selection bounds", row)
		}
		row = d.selection[row]
	}
	
	return vector.GetValue(row)
}

// SetValue sets a value in the chunk
func (d *DataChunk) SetValue(col, row int, value interface{}) error {
	vector, err := d.GetVector(col)
	if err != nil {
		return err
	}
	
	// Apply selection vector if present
	if d.selection != nil {
		if row >= len(d.selection) {
			return fmt.Errorf("row %d out of selection bounds", row)
		}
		row = d.selection[row]
	}
	
	return vector.SetValue(row, value)
}

// Size returns the current number of active rows in the chunk
func (d *DataChunk) Size() int {
	return d.size
}

// Capacity returns the maximum capacity of the chunk
func (d *DataChunk) Capacity() int {
	return d.capacity
}