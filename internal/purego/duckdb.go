package purego

import (
	"fmt"
	"unsafe"
)

// DuckDB represents a DuckDB instance with loaded functions
type DuckDB struct {
	lib *Library

	// Core database functions
	duckdbOpen       func(path unsafe.Pointer, db *Database) uint32
	duckdbClose      func(db *Database)
	duckdbConnect    func(db Database, conn *Connection) uint32
	duckdbDisconnect func(conn *Connection)

	// Query execution functions
	duckdbQuery           func(conn Connection, query unsafe.Pointer, result *Result) uint32
	duckdbPrepare         func(conn Connection, query unsafe.Pointer, stmt *PreparedStatement) uint32
	duckdbExecutePrepared func(stmt PreparedStatement, result *Result) uint32
	duckdbDestroyPrepare  func(stmt *PreparedStatement)
	duckdbDestroyResult   func(result *Result)

	// Result handling functions
	duckdbResultError       func(result *Result) unsafe.Pointer
	duckdbRowCount          func(result *Result) uint64
	duckdbColumnCount       func(result *Result) uint64
	duckdbColumnsChanged    func(result *Result) uint64
	duckdbColumnName        func(result *Result, col uint64) unsafe.Pointer
	duckdbColumnType        func(result *Result, col uint64) uint32
	duckdbColumnLogicalType func(result *Result, col uint64) LogicalType

	// Data access functions
	duckdbValueBoolean func(result *Result, col uint64, row uint64) bool
	duckdbValueInt8    func(result *Result, col uint64, row uint64) int8
	duckdbValueInt16   func(result *Result, col uint64, row uint64) int16
	duckdbValueInt32   func(result *Result, col uint64, row uint64) int32
	duckdbValueInt64   func(result *Result, col uint64, row uint64) int64
	duckdbValueUint8   func(result *Result, col uint64, row uint64) uint8
	duckdbValueUint16  func(result *Result, col uint64, row uint64) uint16
	duckdbValueUint32  func(result *Result, col uint64, row uint64) uint32
	duckdbValueUint64  func(result *Result, col uint64, row uint64) uint64
	duckdbValueFloat   func(result *Result, col uint64, row uint64) float32
	duckdbValueDouble  func(result *Result, col uint64, row uint64) float64
	duckdbValueVarchar func(result *Result, col uint64, row uint64) unsafe.Pointer
	duckdbValueBlob    func(result *Result, col uint64, row uint64) uintptr
	duckdbValueIsNull  func(result *Result, col uint64, row uint64) bool

	// Config functions
	duckdbCreateConfig  func(config *Config) uint32
	duckdbConfigCount   func() uint64
	duckdbGetConfigFlag func(idx uint64, name **byte, desc **byte) uint32
	duckdbSetConfig     func(config Config, name unsafe.Pointer, option unsafe.Pointer) uint32
	duckdbDestroyConfig func(config *Config)
	duckdbOpenExt       func(path unsafe.Pointer, db *Database, config Config, err **byte) uint32

	// Type functions
	duckdbCreateLogicalType  func(typeId uint32) LogicalType
	duckdbGetTypeId          func(logicalType LogicalType) uint32
	duckdbDestroyLogicalType func(logicalType *LogicalType)

	// Parameter binding functions
	duckdbBindBoolean           func(stmt PreparedStatement, idx uint64, val bool) uint32
	duckdbBindInt8              func(stmt PreparedStatement, idx uint64, val int8) uint32
	duckdbBindInt16             func(stmt PreparedStatement, idx uint64, val int16) uint32
	duckdbBindInt32             func(stmt PreparedStatement, idx uint64, val int32) uint32
	duckdbBindInt64             func(stmt PreparedStatement, idx uint64, val int64) uint32
	duckdbBindUint8             func(stmt PreparedStatement, idx uint64, val uint8) uint32
	duckdbBindUint16            func(stmt PreparedStatement, idx uint64, val uint16) uint32
	duckdbBindUint32            func(stmt PreparedStatement, idx uint64, val uint32) uint32
	duckdbBindUint64            func(stmt PreparedStatement, idx uint64, val uint64) uint32
	duckdbBindFloat             func(stmt PreparedStatement, idx uint64, val float32) uint32
	duckdbBindDouble            func(stmt PreparedStatement, idx uint64, val float64) uint32
	duckdbBindVarchar           func(stmt PreparedStatement, idx uint64, val unsafe.Pointer) uint32
	duckdbBindNull              func(stmt PreparedStatement, idx uint64) uint32
	duckdbBindBlob              func(stmt PreparedStatement, idx uint64, data unsafe.Pointer, length uint64) uint32
	duckdbClearBindings         func(stmt PreparedStatement) uint32
	duckdbPreparedStatementType func(stmt PreparedStatement) uint32

	// Date/Time functions
	duckdbValueDate      func(result *Result, col uint64, row uint64) int32
	duckdbValueTime      func(result *Result, col uint64, row uint64) int64
	duckdbValueTimestamp func(result *Result, col uint64, row uint64) int64
	duckdbFromDate       func(date int32) uintptr
	duckdbToDate         func(date uintptr) int32
	duckdbFromTime       func(time int64) uintptr
	duckdbToTime         func(time uintptr) int64
	duckdbFromTimestamp  func(ts int64) uintptr
	duckdbToTimestamp    func(ts uintptr) int64
	duckdbBindDate       func(stmt PreparedStatement, idx uint64, val int32) uint32
	duckdbBindTime       func(stmt PreparedStatement, idx uint64, val int64) uint32
	duckdbBindTimestamp  func(stmt PreparedStatement, idx uint64, val int64) uint32

	// Decimal functions
	duckdbDecimalWidth func(logicalType LogicalType) uint8
	duckdbDecimalScale func(logicalType LogicalType) uint8
	duckdbValueDecimal func(result *Result, col uint64, row uint64) uintptr
	duckdbBindDecimal  func(stmt PreparedStatement, idx uint64, val uintptr) uint32

	// List/Array functions
	duckdbListSize          func(result *Result, col uint64, row uint64) uint64
	duckdbListValue         func(result *Result, col uint64, row uint64) Value
	duckdbGetListChild      func(list Value) Value
	duckdbGetListSize       func(list Value) uint64
	duckdbCreateListType    func(childType LogicalType) LogicalType
	duckdbListTypeChildType func(listType LogicalType) LogicalType

	// Struct functions
	duckdbStructTypeChildCount func(structType LogicalType) uint64
	duckdbStructTypeChildName  func(structType LogicalType, index uint64) unsafe.Pointer
	duckdbStructTypeChildType  func(structType LogicalType, index uint64) LogicalType
	duckdbCreateStructType     func(memberTypes *LogicalType, memberNames **byte, memberCount uint64) LogicalType

	// Map functions
	duckdbCreateMapType    func(keyType LogicalType, valueType LogicalType) LogicalType
	duckdbMapTypeKeyType   func(mapType LogicalType) LogicalType
	duckdbMapTypeValueType func(mapType LogicalType) LogicalType

	// UUID functions
	duckdbUUIDToString func(value uintptr) unsafe.Pointer
	duckdbStringToUUID func(str unsafe.Pointer) uintptr
	duckdbBindUUID     func(stmt PreparedStatement, idx uint64, value uintptr) uint32
	duckdbValueUUID    func(result *Result, col uint64, row uint64) uintptr
}

// New creates a new DuckDB instance with loaded functions
func New() (*DuckDB, error) {
	lib, err := LoadLibrary()
	if err != nil {
		return nil, err
	}

	db := &DuckDB{lib: lib}

	// Register all required functions
	err = db.registerFunctions()
	if err != nil {
		_ = lib.Close() // Library closing errors not critical in error path
		return nil, err
	}

	return db, nil
}

// registerFunctions registers all DuckDB C API functions
func (d *DuckDB) registerFunctions() error {
	// Core database functions
	if err := d.lib.RegisterFunc(&d.duckdbOpen, "duckdb_open"); err != nil {
		return fmt.Errorf("failed to register duckdb_open: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbClose, "duckdb_close"); err != nil {
		return fmt.Errorf("failed to register duckdb_close: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbConnect, "duckdb_connect"); err != nil {
		return fmt.Errorf("failed to register duckdb_connect: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbDisconnect, "duckdb_disconnect"); err != nil {
		return fmt.Errorf("failed to register duckdb_disconnect: %w", err)
	}

	// Query execution functions
	if err := d.lib.RegisterFunc(&d.duckdbQuery, "duckdb_query"); err != nil {
		return fmt.Errorf("failed to register duckdb_query: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbPrepare, "duckdb_prepare"); err != nil {
		return fmt.Errorf("failed to register duckdb_prepare: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbExecutePrepared, "duckdb_execute_prepared"); err != nil {
		return fmt.Errorf("failed to register duckdb_execute_prepared: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbDestroyPrepare, "duckdb_destroy_prepare"); err != nil {
		return fmt.Errorf("failed to register duckdb_destroy_prepare: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbDestroyResult, "duckdb_destroy_result"); err != nil {
		return fmt.Errorf("failed to register duckdb_destroy_result: %w", err)
	}

	// Result handling functions
	if err := d.lib.RegisterFunc(&d.duckdbResultError, "duckdb_result_error"); err != nil {
		return fmt.Errorf("failed to register duckdb_result_error: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbRowCount, "duckdb_row_count"); err != nil {
		return fmt.Errorf("failed to register duckdb_row_count: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbColumnCount, "duckdb_column_count"); err != nil {
		return fmt.Errorf("failed to register duckdb_column_count: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbColumnsChanged, "duckdb_rows_changed"); err != nil {
		return fmt.Errorf("failed to register duckdb_rows_changed: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbColumnName, "duckdb_column_name"); err != nil {
		return fmt.Errorf("failed to register duckdb_column_name: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbColumnType, "duckdb_column_type"); err != nil {
		return fmt.Errorf("failed to register duckdb_column_type: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbColumnLogicalType, "duckdb_column_logical_type"); err != nil {
		return fmt.Errorf("failed to register duckdb_column_logical_type: %w", err)
	}

	// Data access functions
	if err := d.lib.RegisterFunc(&d.duckdbValueBoolean, "duckdb_value_boolean"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_boolean: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueInt8, "duckdb_value_int8"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_int8: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueInt16, "duckdb_value_int16"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_int16: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueInt32, "duckdb_value_int32"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_int32: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueInt64, "duckdb_value_int64"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_int64: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueUint8, "duckdb_value_uint8"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_uint8: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueUint16, "duckdb_value_uint16"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_uint16: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueUint32, "duckdb_value_uint32"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_uint32: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueUint64, "duckdb_value_uint64"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_uint64: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueFloat, "duckdb_value_float"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_float: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueDouble, "duckdb_value_double"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_double: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueVarchar, "duckdb_value_varchar"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_varchar: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueBlob, "duckdb_value_blob"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_blob: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueIsNull, "duckdb_value_is_null"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_is_null: %w", err)
	}

	// Config functions
	if err := d.lib.RegisterFunc(&d.duckdbCreateConfig, "duckdb_create_config"); err != nil {
		return fmt.Errorf("failed to register duckdb_create_config: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbConfigCount, "duckdb_config_count"); err != nil {
		return fmt.Errorf("failed to register duckdb_config_count: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbGetConfigFlag, "duckdb_get_config_flag"); err != nil {
		return fmt.Errorf("failed to register duckdb_get_config_flag: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbSetConfig, "duckdb_set_config"); err != nil {
		return fmt.Errorf("failed to register duckdb_set_config: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbDestroyConfig, "duckdb_destroy_config"); err != nil {
		return fmt.Errorf("failed to register duckdb_destroy_config: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbOpenExt, "duckdb_open_ext"); err != nil {
		return fmt.Errorf("failed to register duckdb_open_ext: %w", err)
	}

	// Type functions
	if err := d.lib.RegisterFunc(&d.duckdbCreateLogicalType, "duckdb_create_logical_type"); err != nil {
		return fmt.Errorf("failed to register duckdb_create_logical_type: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbGetTypeId, "duckdb_get_type_id"); err != nil {
		return fmt.Errorf("failed to register duckdb_get_type_id: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbDestroyLogicalType, "duckdb_destroy_logical_type"); err != nil {
		return fmt.Errorf("failed to register duckdb_destroy_logical_type: %w", err)
	}

	// Parameter binding functions
	if err := d.lib.RegisterFunc(&d.duckdbBindBoolean, "duckdb_bind_boolean"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_boolean: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindInt8, "duckdb_bind_int8"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_int8: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindInt16, "duckdb_bind_int16"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_int16: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindInt32, "duckdb_bind_int32"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_int32: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindInt64, "duckdb_bind_int64"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_int64: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindUint8, "duckdb_bind_uint8"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_uint8: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindUint16, "duckdb_bind_uint16"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_uint16: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindUint32, "duckdb_bind_uint32"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_uint32: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindUint64, "duckdb_bind_uint64"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_uint64: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindFloat, "duckdb_bind_float"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_float: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindDouble, "duckdb_bind_double"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_double: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindVarchar, "duckdb_bind_varchar"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_varchar: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindNull, "duckdb_bind_null"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_null: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindBlob, "duckdb_bind_blob"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_blob: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbClearBindings, "duckdb_clear_bindings"); err != nil {
		return fmt.Errorf("failed to register duckdb_clear_bindings: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbPreparedStatementType, "duckdb_prepared_statement_type"); err != nil {
		return fmt.Errorf("failed to register duckdb_prepared_statement_type: %w", err)
	}

	// Date/Time functions
	if err := d.lib.RegisterFunc(&d.duckdbValueDate, "duckdb_value_date"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_date: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueTime, "duckdb_value_time"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_time: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueTimestamp, "duckdb_value_timestamp"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_timestamp: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindDate, "duckdb_bind_date"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_date: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindTime, "duckdb_bind_time"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_time: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindTimestamp, "duckdb_bind_timestamp"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_timestamp: %w", err)
	}

	// Decimal functions
	if err := d.lib.RegisterFunc(&d.duckdbDecimalWidth, "duckdb_decimal_width"); err != nil {
		return fmt.Errorf("failed to register duckdb_decimal_width: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbDecimalScale, "duckdb_decimal_scale"); err != nil {
		return fmt.Errorf("failed to register duckdb_decimal_scale: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbValueDecimal, "duckdb_value_decimal"); err != nil {
		return fmt.Errorf("failed to register duckdb_value_decimal: %w", err)
	}
	if err := d.lib.RegisterFunc(&d.duckdbBindDecimal, "duckdb_bind_decimal"); err != nil {
		return fmt.Errorf("failed to register duckdb_bind_decimal: %w", err)
	}

	return nil
}

// Close closes the DuckDB library
func (d *DuckDB) Close() error {
	if d.lib != nil {
		return d.lib.Close()
	}
	return nil
}
