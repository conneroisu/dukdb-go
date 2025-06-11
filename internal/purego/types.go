package purego

import "unsafe"

// DuckDB C API types represented as Go types
type (
	Database          uintptr
	Connection        uintptr
	Result            uintptr
	PreparedStatement uintptr
	DataChunk         uintptr
	Value             uintptr
	LogicalType       uintptr
	Config            uintptr
)

// DuckDB return states
const (
	StateSuccess = 0
	StateError   = 1
)

// DuckDB types enum
const (
	TypeInvalid = iota
	TypeBoolean
	TypeTinyint
	TypeSmallint
	TypeInteger
	TypeBigint
	TypeUTinyint
	TypeUSmallint
	TypeUInteger
	TypeUBigint
	TypeFloat
	TypeDouble
	TypeTimestamp
	TypeDate
	TypeTime
	TypeInterval
	TypeHugeint
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
	TypeUnion
	TypeBit
	TypeTimeTZ
	TypeTimestampTZ
)

// ResultData holds the data for a query result
type ResultData struct {
	Columns      []Column
	RowCount     uint64
	RowsChanged  uint64
	ErrorMessage string
}

// Column represents a column in a result set
type Column struct {
	Name         string
	Type         uint32
	LogicalType  LogicalType
	InternalType string
	Precision    uint8 // For DECIMAL types
	Scale        uint8 // For DECIMAL types
}

// Vector represents a column vector in DuckDB
type Vector struct {
	Data     unsafe.Pointer
	Validity unsafe.Pointer
	Type     LogicalType
}

// Hugeint represents a DuckDB HUGEINT type
type Hugeint struct {
	Lower uint64
	Upper int64
}

// Decimal represents a DuckDB DECIMAL type
type Decimal struct {
	Width uint8
	Scale uint8
	Value Hugeint
}

// Blob represents a DuckDB BLOB type
type Blob struct {
	Data unsafe.Pointer
	Size uint64
}

// toPtr converts a Go string to a C string pointer
func toPtr(s string) unsafe.Pointer {
	if s == "" {
		return nil
	}
	return unsafe.Pointer(&[]byte(s + "\x00")[0])
}
