package storage

import (
	"fmt"
	"time"

	"github.com/connerohnesorge/dukdb-go/internal/purego"
	"github.com/connerohnesorge/dukdb-go/internal/types"
)

// VectorBuilder helps construct vectors from various data sources
type VectorBuilder struct {
	logicalType LogicalType
	vector      *Vector
	position    int
}

// NewVectorBuilder creates a new vector builder
func NewVectorBuilder(logicalType LogicalType, capacity int) *VectorBuilder {
	return &VectorBuilder{
		logicalType: logicalType,
		vector:      NewFlatVector(logicalType, capacity),
		position:    0,
	}
}

// AppendValue adds a value to the vector
func (vb *VectorBuilder) AppendValue(value interface{}) error {
	if vb.position >= vb.vector.capacity {
		// In a real implementation, we would resize here
		return fmt.Errorf("vector capacity exceeded")
	}

	err := vb.vector.SetValue(vb.position, value)
	if err != nil {
		return err
	}

	vb.position++
	vb.vector.size = vb.position
	return nil
}

// Build returns the constructed vector
func (vb *VectorBuilder) Build() *Vector {
	return vb.vector
}

// ConvertPuregoTypeToLogicalType converts purego types to logical types
func ConvertPuregoTypeToLogicalType(puregoType int) LogicalType {
	switch puregoType {
	case purego.TypeBoolean:
		return LogicalType{ID: TypeBoolean}
	case purego.TypeTinyint:
		return LogicalType{ID: TypeTinyInt}
	case purego.TypeSmallint:
		return LogicalType{ID: TypeSmallInt}
	case purego.TypeInteger:
		return LogicalType{ID: TypeInteger}
	case purego.TypeBigint:
		return LogicalType{ID: TypeBigInt}
	case purego.TypeUTinyint:
		return LogicalType{ID: TypeUTinyInt}
	case purego.TypeUSmallint:
		return LogicalType{ID: TypeUSmallInt}
	case purego.TypeUInteger:
		return LogicalType{ID: TypeUInteger}
	case purego.TypeUBigint:
		return LogicalType{ID: TypeUBigInt}
	case purego.TypeFloat:
		return LogicalType{ID: TypeFloat}
	case purego.TypeDouble:
		return LogicalType{ID: TypeDouble}
	case purego.TypeVarchar:
		return LogicalType{ID: TypeVarchar}
	case purego.TypeBlob:
		return LogicalType{ID: TypeBlob}
	case purego.TypeDecimal:
		return LogicalType{ID: TypeDecimal}
	case purego.TypeTimestamp:
		return LogicalType{ID: TypeTimestamp}
	case purego.TypeDate:
		return LogicalType{ID: TypeDate}
	case purego.TypeTime:
		return LogicalType{ID: TypeTime}
	case purego.TypeUUID:
		return LogicalType{ID: TypeUUID}
	case purego.TypeList:
		return LogicalType{ID: TypeList}
	case purego.TypeStruct:
		return LogicalType{ID: TypeStruct}
	case purego.TypeMap:
		return LogicalType{ID: TypeMap}
	default:
		return LogicalType{ID: TypeInvalid}
	}
}

// ResultToDataChunk converts a purego QueryResult to a DataChunk
func ResultToDataChunk(result *purego.QueryResult) (*DataChunk, error) {
	columns := result.Columns()
	rowCount := int(result.RowCount())
	
	// Create logical types from column info
	logicalTypes := make([]LogicalType, len(columns))
	for i, col := range columns {
		logicalTypes[i] = ConvertPuregoTypeToLogicalType(int(col.Type))
		// Set precision and scale for decimal types
		if col.Type == purego.TypeDecimal {
			logicalTypes[i].Width = col.Precision
			logicalTypes[i].Scale = col.Scale
		}
	}
	
	// Create data chunk
	chunk := NewDataChunk(logicalTypes, rowCount)
	
	// Fill the chunk with data
	for row := 0; row < rowCount; row++ {
		for col := 0; col < len(columns); col++ {
			value, err := result.GetValue(uint64(col), uint64(row))
			if err != nil {
				return nil, fmt.Errorf("failed to get value at col %d, row %d: %w", col, row, err)
			}
			
			// Convert types as needed
			convertedValue := convertValueForStorage(value, logicalTypes[col])
			
			err = chunk.SetValue(col, row, convertedValue)
			if err != nil {
				return nil, fmt.Errorf("failed to set value at col %d, row %d: %w", col, row, err)
			}
		}
	}
	
	chunk.SetSize(rowCount)
	return chunk, nil
}

// convertValueForStorage converts values to appropriate storage format
func convertValueForStorage(value interface{}, logicalType LogicalType) interface{} {
	if value == nil {
		return nil
	}
	
	switch logicalType.ID {
	case TypeDecimal:
		// Convert Decimal type to string for storage
		if dec, ok := value.(*types.Decimal); ok {
			return dec.String()
		}
	case TypeTimestamp, TypeDate, TypeTime:
		// Convert time.Time to appropriate format
		if t, ok := value.(time.Time); ok {
			// For now, store as ISO string
			return t.Format(time.RFC3339Nano)
		}
	}
	
	// Return as-is for other types
	return value
}

// DataChunkBuilder helps build data chunks incrementally
type DataChunkBuilder struct {
	logicalTypes []LogicalType
	builders     []*VectorBuilder
	capacity     int
}

// NewDataChunkBuilder creates a new data chunk builder
func NewDataChunkBuilder(logicalTypes []LogicalType, capacity int) *DataChunkBuilder {
	builders := make([]*VectorBuilder, len(logicalTypes))
	for i, lt := range logicalTypes {
		builders[i] = NewVectorBuilder(lt, capacity)
	}
	
	return &DataChunkBuilder{
		logicalTypes: logicalTypes,
		builders:     builders,
		capacity:     capacity,
	}
}

// AppendRow adds a row of values to the chunk
func (dcb *DataChunkBuilder) AppendRow(values []interface{}) error {
	if len(values) != len(dcb.builders) {
		return fmt.Errorf("expected %d values, got %d", len(dcb.builders), len(values))
	}
	
	for i, value := range values {
		err := dcb.builders[i].AppendValue(value)
		if err != nil {
			return fmt.Errorf("failed to append value at column %d: %w", i, err)
		}
	}
	
	return nil
}

// Build returns the constructed data chunk
func (dcb *DataChunkBuilder) Build() *DataChunk {
	vectors := make([]*Vector, len(dcb.builders))
	size := 0
	
	for i, builder := range dcb.builders {
		vectors[i] = builder.Build()
		if i == 0 {
			size = vectors[i].size
		}
	}
	
	return &DataChunk{
		vectors:  vectors,
		size:     size,
		capacity: dcb.capacity,
	}
}