package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/connerohnesorge/dukdb-go/internal/engine"
	"github.com/connerohnesorge/dukdb-go/internal/storage"
	"github.com/connerohnesorge/dukdb-go/internal/types"
)

// Rows implements the database/sql/driver.Rows interface
type Rows struct {
	engineResult *engine.QueryResult
	columns      []engine.Column
	currentChunk *storage.DataChunk
	chunkRow     int
	stmt         *Stmt           // Optional: set if rows came from a prepared statement
	ctx          context.Context // Context for cancellation
}

// Columns returns the column names
func (r *Rows) Columns() []string {
	if r.columns == nil && r.engineResult != nil {
		r.columns = r.engineResult.Columns()
	}

	names := make([]string, len(r.columns))
	for i, col := range r.columns {
		names[i] = col.Name
	}
	return names
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	if r.engineResult != nil {
		_ = r.engineResult.Close() // Closing errors are not critical during cleanup
		r.engineResult = nil
	}

	// Close the statement if this came from a prepared statement
	if r.stmt != nil {
		_ = r.stmt.Close() // Closing errors are not critical during cleanup
		r.stmt = nil
	}

	return nil
}

// Next populates the provided slice with the next row values
func (r *Rows) Next(dest []driver.Value) error {
	// Check context cancellation
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}
	}

	// Get next chunk if needed
	if r.currentChunk == nil || r.chunkRow >= r.currentChunk.Size() {
		if !r.engineResult.Next() {
			return io.EOF
		}
		r.currentChunk = r.engineResult.GetChunk()
		r.chunkRow = 0
	}

	// Extract values from current row
	for i := range dest {
		if i >= len(r.columns) {
			return fmt.Errorf("destination has more columns than result")
		}

		val, err := r.currentChunk.GetValue(i, r.chunkRow)
		if err != nil {
			return err
		}

		// Convert to driver.Value
		dest[i] = convertToDriverValue(val, r.columns[i].Type)
	}

	r.chunkRow++
	return nil
}

// ColumnTypeDatabaseTypeName returns the database type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}

	switch r.columns[index].Type.ID {
	case storage.TypeBoolean:
		return "BOOLEAN"
	case storage.TypeTinyInt:
		return "TINYINT"
	case storage.TypeSmallInt:
		return "SMALLINT"
	case storage.TypeInteger:
		return "INTEGER"
	case storage.TypeBigInt:
		return "BIGINT"
	case storage.TypeFloat:
		return "FLOAT"
	case storage.TypeDouble:
		return "DOUBLE"
	case storage.TypeDecimal:
		return "DECIMAL"
	case storage.TypeVarchar:
		return "VARCHAR"
	case storage.TypeDate:
		return "DATE"
	case storage.TypeTime:
		return "TIME"
	case storage.TypeTimestamp:
		return "TIMESTAMP"
	case storage.TypeInterval:
		return "INTERVAL"
	case storage.TypeList:
		return "LIST"
	case storage.TypeStruct:
		return "STRUCT"
	case storage.TypeMap:
		return "MAP"
	default:
		return ""
	}
}

// ColumnTypeLength returns the column length
func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, false
	}

	colType := r.columns[index].Type
	switch colType.ID {
	case storage.TypeVarchar:
		if colType.Width > 0 {
			return int64(colType.Width), true
		}
	case storage.TypeDecimal:
		return int64(colType.Width), true
	}

	return 0, false
}

// ColumnTypeNullable returns whether the column can be null
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	// DuckDB columns are nullable by default unless specified otherwise
	return true, true
}

// ColumnTypePrecisionScale returns the precision and scale for decimal types
func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, 0, false
	}

	colType := r.columns[index].Type
	if colType.ID == storage.TypeDecimal {
		return int64(colType.Width), int64(colType.Scale), true
	}

	return 0, 0, false
}

// ColumnTypeScanType returns the Go type for scanning
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.columns) {
		return nil
	}

	switch r.columns[index].Type.ID {
	case storage.TypeBoolean:
		return reflect.TypeOf(false)
	case storage.TypeTinyInt:
		return reflect.TypeOf(int8(0))
	case storage.TypeSmallInt:
		return reflect.TypeOf(int16(0))
	case storage.TypeInteger:
		return reflect.TypeOf(int32(0))
	case storage.TypeBigInt:
		return reflect.TypeOf(int64(0))
	case storage.TypeFloat:
		return reflect.TypeOf(float32(0))
	case storage.TypeDouble:
		return reflect.TypeOf(float64(0))
	case storage.TypeDecimal:
		return reflect.TypeOf(&types.Decimal{})
	case storage.TypeVarchar:
		return reflect.TypeOf("")
	case storage.TypeDate:
		return reflect.TypeOf(int32(0))
	case storage.TypeTime:
		return reflect.TypeOf(int64(0))
	case storage.TypeTimestamp:
		return reflect.TypeOf(time.Time{})
	case storage.TypeInterval:
		return reflect.TypeOf(types.Interval{})
	case storage.TypeUUID:
		return reflect.TypeOf(types.UUID{})
	case storage.TypeList:
		return reflect.TypeOf(&types.ListAny{})
	case storage.TypeStruct:
		return reflect.TypeOf(&types.Struct{})
	case storage.TypeMap:
		return reflect.TypeOf(&types.MapAny{})
	default:
		return reflect.TypeOf(new(any)).Elem()
	}
}

// convertToDriverValue converts internal values to driver.Value
func convertToDriverValue(val any, colType storage.LogicalType) driver.Value {
	if val == nil {
		return nil
	}

	// Special handling for timestamp columns
	if colType.ID == storage.TypeTimestamp {
		if intVal, ok := val.(int64); ok {
			// Convert int64 microseconds to time.Time
			return time.UnixMicro(intVal)
		}
	}

	// Special handling for complex types stored as strings
	if strVal, ok := val.(string); ok {
		switch colType.ID {
		case storage.TypeUUID:
			// Convert string back to UUID
			uuid, err := types.NewUUID(strVal)
			if err != nil {
				return strVal // Return as string if conversion fails
			}
			return uuid
		case storage.TypeList:
			// Convert JSON string back to List
			list := &types.ListAny{}
			err := list.UnmarshalJSON([]byte(strVal))
			if err != nil {
				return strVal // Return as string if conversion fails
			}
			return list
		case storage.TypeStruct:
			// Convert JSON string back to Struct
			structVal := &types.Struct{}
			err := structVal.UnmarshalJSON([]byte(strVal))
			if err != nil {
				return strVal // Return as string if conversion fails
			}
			return structVal
		case storage.TypeMap:
			// Convert JSON string back to Map
			mapVal := &types.MapAny{}
			err := mapVal.UnmarshalJSON([]byte(strVal))
			if err != nil {
				return strVal // Return as string if conversion fails
			}
			return mapVal
		}
	}

	switch v := val.(type) {
	case bool, int8, int16, int32, int64, float32, float64, string:
		return v
	case []byte:
		return v
	case *types.Decimal:
		// Return decimal as string for compatibility
		return v.String()
	default:
		// For complex types, return as-is
		return v
	}
}
