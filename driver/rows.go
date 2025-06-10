package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"

	"github.com/connerohnesorge/dukdb-go/internal/purego"
)

// Rows implements the database/sql/driver.Rows interface
type Rows struct {
	duckdb  *purego.DuckDB
	result  *purego.QueryResult
	cols    []purego.Column
	current uint64
	stmt    *Stmt           // Optional: set if rows came from a prepared statement
	ctx     context.Context // Context for cancellation
}

// Columns returns the column names
func (r *Rows) Columns() []string {
	names := make([]string, len(r.cols))
	for i, col := range r.cols {
		names[i] = col.Name
	}
	return names
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	if r.result != nil {
		r.result.Close()
		r.result = nil
	}

	// Close the statement if this came from a prepared statement
	if r.stmt != nil {
		r.stmt.Close()
		r.stmt = nil
	}

	return nil
}

// Next populates the provided slice with the next row values
func (r *Rows) Next(dest []driver.Value) error {
	// Check for context cancellation
	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}
	}

	if r.current >= r.result.RowCount() {
		return io.EOF
	}

	// Fetch values for current row
	for i := range dest {
		val, err := r.result.GetValue(uint64(i), r.current)
		if err != nil {
			return err
		}
		dest[i] = val
	}

	r.current++
	return nil
}

// ColumnTypeDatabaseTypeName returns the database type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.cols) {
		return ""
	}

	return getTypeName(r.cols[index].Type)
}

// ColumnTypeLength returns the length of the column type
func (r *Rows) ColumnTypeLength(index int) (int64, bool) {
	if index < 0 || index >= len(r.cols) {
		return 0, false
	}
	
	switch r.cols[index].Type {
	case purego.TypeVarchar:
		// VARCHAR has variable length, return a reasonable default
		return 65535, true
	case purego.TypeBlob:
		// BLOB has variable length
		return 65535, true
	case purego.TypeDecimal:
		// For DECIMAL, length could be considered as precision
		return int64(r.cols[index].Precision), true
	case purego.TypeTinyint:
		return 1, true
	case purego.TypeSmallint:
		return 2, true
	case purego.TypeInteger:
		return 4, true
	case purego.TypeBigint:
		return 8, true
	case purego.TypeFloat:
		return 4, true
	case purego.TypeDouble:
		return 8, true
	case purego.TypeBoolean:
		return 1, true
	case purego.TypeDate:
		return 4, true
	case purego.TypeTime:
		return 8, true
	case purego.TypeTimestamp, purego.TypeTimestampS, purego.TypeTimestampMS, purego.TypeTimestampNS:
		return 8, true
	case purego.TypeUUID:
		return 16, true
	default:
		return 0, false
	}
}

// ColumnTypeNullable returns whether the column can be null
func (r *Rows) ColumnTypeNullable(index int) (bool, bool) {
	// DuckDB columns are nullable by default
	return true, true
}

// ColumnTypePrecisionScale returns the precision and scale for numeric types
func (r *Rows) ColumnTypePrecisionScale(index int) (int64, int64, bool) {
	if index < 0 || index >= len(r.cols) {
		return 0, 0, false
	}
	
	// Return precision and scale for DECIMAL types
	if r.cols[index].Type == purego.TypeDecimal {
		return int64(r.cols[index].Precision), int64(r.cols[index].Scale), true
	}
	
	// For other numeric types, return appropriate defaults
	switch r.cols[index].Type {
	case purego.TypeFloat:
		return 7, 0, true // Single precision float
	case purego.TypeDouble:
		return 15, 0, true // Double precision float
	default:
		return 0, 0, false
	}
}

// ColumnTypeScanType returns the Go type suitable for scanning
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.cols) {
		return reflect.TypeOf(interface{}(nil))
	}

	switch r.cols[index].Type {
	case purego.TypeBoolean:
		return reflect.TypeOf(bool(false))
	case purego.TypeTinyint:
		return reflect.TypeOf(int8(0))
	case purego.TypeSmallint:
		return reflect.TypeOf(int16(0))
	case purego.TypeInteger:
		return reflect.TypeOf(int32(0))
	case purego.TypeBigint:
		return reflect.TypeOf(int64(0))
	case purego.TypeFloat:
		return reflect.TypeOf(float32(0))
	case purego.TypeDouble:
		return reflect.TypeOf(float64(0))
	case purego.TypeDecimal:
		// Import the types package to reference Decimal
		return reflect.TypeOf(string("")) // Return string for now, can be scanned into types.Decimal
	case purego.TypeVarchar:
		return reflect.TypeOf(string(""))
	case purego.TypeBlob:
		return reflect.TypeOf([]byte(nil))
	default:
		return reflect.TypeOf(interface{}(nil))
	}
}

// getTypeName returns the string name for a DuckDB type
func getTypeName(typeID uint32) string {
	switch typeID {
	case purego.TypeBoolean:
		return "BOOLEAN"
	case purego.TypeTinyint:
		return "TINYINT"
	case purego.TypeSmallint:
		return "SMALLINT"
	case purego.TypeInteger:
		return "INTEGER"
	case purego.TypeBigint:
		return "BIGINT"
	case purego.TypeUTinyint:
		return "UTINYINT"
	case purego.TypeUSmallint:
		return "USMALLINT"
	case purego.TypeUInteger:
		return "UINTEGER"
	case purego.TypeUBigint:
		return "UBIGINT"
	case purego.TypeFloat:
		return "FLOAT"
	case purego.TypeDouble:
		return "DOUBLE"
	case purego.TypeTimestamp:
		return "TIMESTAMP"
	case purego.TypeDate:
		return "DATE"
	case purego.TypeTime:
		return "TIME"
	case purego.TypeInterval:
		return "INTERVAL"
	case purego.TypeHugeint:
		return "HUGEINT"
	case purego.TypeVarchar:
		return "VARCHAR"
	case purego.TypeBlob:
		return "BLOB"
	case purego.TypeDecimal:
		return "DECIMAL"
	case purego.TypeTimestampS:
		return "TIMESTAMP_S"
	case purego.TypeTimestampMS:
		return "TIMESTAMP_MS"
	case purego.TypeTimestampNS:
		return "TIMESTAMP_NS"
	case purego.TypeEnum:
		return "ENUM"
	case purego.TypeList:
		return "LIST"
	case purego.TypeStruct:
		return "STRUCT"
	case purego.TypeMap:
		return "MAP"
	case purego.TypeUUID:
		return "UUID"
	case purego.TypeUnion:
		return "UNION"
	case purego.TypeBit:
		return "BIT"
	case purego.TypeTimeTZ:
		return "TIME WITH TIME ZONE"
	case purego.TypeTimestampTZ:
		return "TIMESTAMP WITH TIME ZONE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", typeID)
	}
}
