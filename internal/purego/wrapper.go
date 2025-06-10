package purego

import (
	"fmt"
	"time"
	"unsafe"
)

// Open opens a database connection
func (d *DuckDB) Open(path string) (Database, error) {
	var db Database
	pathPtr := toPtr(path)

	if result := d.duckdbOpen(pathPtr, &db); result != StateSuccess {
		return 0, fmt.Errorf("failed to open database: %s", path)
	}

	return db, nil
}

// Close closes a database
func (d *DuckDB) CloseDatabase(db Database) {
	if db != 0 {
		d.duckdbClose(&db)
	}
}

// Connect creates a new connection to the database
func (d *DuckDB) Connect(db Database) (Connection, error) {
	var conn Connection

	if result := d.duckdbConnect(db, &conn); result != StateSuccess {
		return 0, fmt.Errorf("failed to connect to database")
	}

	return conn, nil
}

// Disconnect closes a connection
func (d *DuckDB) Disconnect(conn Connection) {
	if conn != 0 {
		d.duckdbDisconnect(&conn)
	}
}

// Query executes a SQL query and returns the result
func (d *DuckDB) Query(conn Connection, query string) (*QueryResult, error) {
	var result Result
	queryPtr := toPtr(query)

	if status := d.duckdbQuery(conn, queryPtr, &result); status != StateSuccess {
		errMsg := d.getResultError(result)
		d.duckdbDestroyResult(&result)
		return nil, fmt.Errorf("query failed: %s", errMsg)
	}

	return d.createQueryResult(result), nil
}

// Execute executes a SQL statement (no result expected)
func (d *DuckDB) Execute(conn Connection, query string) error {
	var result Result
	queryPtr := toPtr(query)

	if status := d.duckdbExecute(conn, queryPtr, &result); status != StateSuccess {
		errMsg := d.getResultError(result)
		d.duckdbDestroyResult(&result)
		return fmt.Errorf("execute failed: %s", errMsg)
	}

	rowsChanged := d.duckdbColumnsChanged(result)
	d.duckdbDestroyResult(&result)

	// Store rows changed if needed
	_ = rowsChanged

	return nil
}

// Prepare prepares a SQL statement
func (d *DuckDB) Prepare(conn Connection, query string) (PreparedStatement, error) {
	var stmt PreparedStatement
	queryPtr := toPtr(query)

	if status := d.duckdbPrepare(conn, queryPtr, &stmt); status != StateSuccess {
		return 0, fmt.Errorf("failed to prepare statement: %s", query)
	}

	return stmt, nil
}

// ExecutePrepared executes a prepared statement
func (d *DuckDB) ExecutePrepared(stmt PreparedStatement) (*QueryResult, error) {
	var result Result

	if status := d.duckdbExecutePrepared(stmt, &result); status != StateSuccess {
		errMsg := d.getResultError(result)
		d.duckdbDestroyResult(&result)
		return nil, fmt.Errorf("prepared statement execution failed: %s", errMsg)
	}

	return d.createQueryResult(result), nil
}

// DestroyPrepared destroys a prepared statement
func (d *DuckDB) DestroyPrepared(stmt PreparedStatement) {
	if stmt != 0 {
		d.duckdbDestroyPrepare(&stmt)
	}
}

// QueryResult represents the result of a query
type QueryResult struct {
	duckdb   *DuckDB
	result   Result
	columns  []Column
	rowCount uint64
}

// createQueryResult creates a QueryResult from a Result
func (d *DuckDB) createQueryResult(result Result) *QueryResult {
	qr := &QueryResult{
		duckdb:   d,
		result:   result,
		rowCount: d.duckdbRowCount(result),
	}

	// Get column information
	colCount := d.duckdbColumnCount(result)
	qr.columns = make([]Column, colCount)

	for i := uint64(0); i < colCount; i++ {
		namePtr := d.duckdbColumnName(result, i)
		qr.columns[i] = Column{
			Name:        ptrToString(namePtr),
			Type:        d.duckdbColumnType(result, i),
			LogicalType: d.duckdbColumnLogicalType(result, i),
		}
	}

	return qr
}

// Close destroys the query result
func (qr *QueryResult) Close() {
	if qr.result != 0 {
		qr.duckdb.duckdbDestroyResult(&qr.result)
		qr.result = 0
	}
}

// Columns returns the column information
func (qr *QueryResult) Columns() []Column {
	return qr.columns
}

// RowCount returns the number of rows in the result
func (qr *QueryResult) RowCount() uint64 {
	return qr.rowCount
}

// GetValue retrieves a value from the result set
func (qr *QueryResult) GetValue(col, row uint64) (interface{}, error) {
	if qr.result == 0 {
		return nil, fmt.Errorf("result already closed")
	}

	if row >= qr.rowCount || col >= uint64(len(qr.columns)) {
		return nil, fmt.Errorf("invalid row or column index")
	}

	// Check for NULL
	if qr.duckdb.duckdbValueIsNull(qr.result, col, row) {
		return nil, nil
	}

	// Get value based on type
	switch qr.columns[col].Type {
	case TypeBoolean:
		return qr.duckdb.duckdbValueBoolean(qr.result, col, row), nil
	case TypeTinyint:
		return qr.duckdb.duckdbValueInt8(qr.result, col, row), nil
	case TypeSmallint:
		return qr.duckdb.duckdbValueInt16(qr.result, col, row), nil
	case TypeInteger:
		return qr.duckdb.duckdbValueInt32(qr.result, col, row), nil
	case TypeBigint:
		return qr.duckdb.duckdbValueInt64(qr.result, col, row), nil
	case TypeUTinyint:
		return qr.duckdb.duckdbValueUint8(qr.result, col, row), nil
	case TypeUSmallint:
		return qr.duckdb.duckdbValueUint16(qr.result, col, row), nil
	case TypeUInteger:
		return qr.duckdb.duckdbValueUint32(qr.result, col, row), nil
	case TypeUBigint:
		return qr.duckdb.duckdbValueUint64(qr.result, col, row), nil
	case TypeFloat:
		return qr.duckdb.duckdbValueFloat(qr.result, col, row), nil
	case TypeDouble:
		return qr.duckdb.duckdbValueDouble(qr.result, col, row), nil
	case TypeVarchar:
		ptr := qr.duckdb.duckdbValueVarchar(qr.result, col, row)
		return ptrToString(ptr), nil
	case TypeBlob:
		blobPtr := qr.duckdb.duckdbValueBlob(qr.result, col, row)
		if blobPtr == 0 {
			return []byte{}, nil
		}
		size := qr.duckdb.duckdbBlobSize(blobPtr)
		if size == 0 {
			return []byte{}, nil
		}
		data := qr.duckdb.duckdbBlobData(blobPtr)
		// Copy the data to a Go slice
		result := make([]byte, size)
		copy(result, (*[1 << 30]byte)(data)[:size:size])
		return result, nil
	case TypeDate:
		days := qr.duckdb.duckdbValueDate(qr.result, col, row)
		return duckdbDateToTime(days), nil
	case TypeTime:
		microseconds := qr.duckdb.duckdbValueTime(qr.result, col, row)
		return duckdbTimeToTime(microseconds), nil
	case TypeTimestamp, TypeTimestampS, TypeTimestampMS, TypeTimestampNS, TypeTimestampTZ:
		microseconds := qr.duckdb.duckdbValueTimestamp(qr.result, col, row)
		return duckdbTimestampToTime(microseconds), nil
	case TypeDecimal:
		// Get decimal as double for now
		// TODO: Implement proper decimal with scale/precision
		return qr.duckdb.duckdbValueDouble(qr.result, col, row), nil
	case TypeHugeint:
		// Get as string and parse
		ptr := qr.duckdb.duckdbValueVarchar(qr.result, col, row)
		return ptrToString(ptr), nil
	case TypeUUID:
		// Get UUID as bytes
		uuidPtr := qr.duckdb.duckdbValueUUID(qr.result, col, row)
		if uuidPtr == 0 {
			return nil, nil
		}
		// Convert to string representation
		strPtr := qr.duckdb.duckdbUUIDToString(uuidPtr)
		return ptrToString(strPtr), nil
	case TypeList:
		// For now, get as JSON string
		ptr := qr.duckdb.duckdbValueVarchar(qr.result, col, row)
		return ptrToString(ptr), nil
	case TypeStruct:
		// For now, get as JSON string
		ptr := qr.duckdb.duckdbValueVarchar(qr.result, col, row)
		return ptrToString(ptr), nil
	case TypeMap:
		// For now, get as JSON string
		ptr := qr.duckdb.duckdbValueVarchar(qr.result, col, row)
		return ptrToString(ptr), nil
	default:
		// Try to get as string for unsupported types
		ptr := qr.duckdb.duckdbValueVarchar(qr.result, col, row)
		return ptrToString(ptr), nil
	}
}

// Helper functions

// getResultError retrieves the error message from a result
func (d *DuckDB) getResultError(result Result) string {
	if result == 0 {
		return "unknown error"
	}
	errPtr := d.duckdbResultError(result)
	if errPtr == nil {
		return "unknown error"
	}
	return ptrToString(errPtr)
}

// ptrToString converts a C string pointer to a Go string
func ptrToString(ptr unsafe.Pointer) string {
	if ptr == nil {
		return ""
	}

	// Find the null terminator
	var length int
	for p := (*byte)(ptr); *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + uintptr(length))) != 0; length++ {
	}

	// Create a byte slice from the pointer
	return string((*[1 << 30]byte)(ptr)[:length:length])
}

// BindValue binds a value to a prepared statement parameter
func (d *DuckDB) BindValue(stmt PreparedStatement, idx uint64, value interface{}) error {
	// DuckDB uses 1-based indexing for parameters
	paramIdx := idx + 1

	if value == nil {
		if d.duckdbBindNull(stmt, paramIdx) != StateSuccess {
			return fmt.Errorf("failed to bind null parameter at index %d", idx)
		}
		return nil
	}

	switch v := value.(type) {
	case bool:
		if d.duckdbBindBoolean(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind boolean parameter at index %d", idx)
		}
	case int8:
		if d.duckdbBindInt8(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind int8 parameter at index %d", idx)
		}
	case int16:
		if d.duckdbBindInt16(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind int16 parameter at index %d", idx)
		}
	case int32:
		if d.duckdbBindInt32(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind int32 parameter at index %d", idx)
		}
	case int:
		// Convert int to int64
		if d.duckdbBindInt64(stmt, paramIdx, int64(v)) != StateSuccess {
			return fmt.Errorf("failed to bind int parameter at index %d", idx)
		}
	case int64:
		if d.duckdbBindInt64(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind int64 parameter at index %d", idx)
		}
	case uint8:
		if d.duckdbBindUint8(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind uint8 parameter at index %d", idx)
		}
	case uint16:
		if d.duckdbBindUint16(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind uint16 parameter at index %d", idx)
		}
	case uint32:
		if d.duckdbBindUint32(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind uint32 parameter at index %d", idx)
		}
	case uint:
		// Convert uint to uint64
		if d.duckdbBindUint64(stmt, paramIdx, uint64(v)) != StateSuccess {
			return fmt.Errorf("failed to bind uint parameter at index %d", idx)
		}
	case uint64:
		if d.duckdbBindUint64(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind uint64 parameter at index %d", idx)
		}
	case float32:
		if d.duckdbBindFloat(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind float32 parameter at index %d", idx)
		}
	case float64:
		if d.duckdbBindDouble(stmt, paramIdx, v) != StateSuccess {
			return fmt.Errorf("failed to bind float64 parameter at index %d", idx)
		}
	case string:
		strPtr := toPtr(v)
		if d.duckdbBindVarchar(stmt, paramIdx, strPtr) != StateSuccess {
			return fmt.Errorf("failed to bind string parameter at index %d", idx)
		}
	case []byte:
		if len(v) == 0 {
			if d.duckdbBindBlob(stmt, paramIdx, nil, 0) != StateSuccess {
				return fmt.Errorf("failed to bind empty blob parameter at index %d", idx)
			}
		} else {
			if d.duckdbBindBlob(stmt, paramIdx, unsafe.Pointer(&v[0]), uint64(len(v))) != StateSuccess {
				return fmt.Errorf("failed to bind blob parameter at index %d", idx)
			}
		}
	case time.Time:
		// Bind as timestamp - convert to microseconds since epoch
		microseconds := timeToDuckDBTimestamp(v)
		if d.duckdbBindTimestamp(stmt, paramIdx, microseconds) != StateSuccess {
			return fmt.Errorf("failed to bind time parameter at index %d", idx)
		}
	default:
		// Try to bind as string for complex types
		strVal := fmt.Sprintf("%v", value)
		strPtr := toPtr(strVal)
		if d.duckdbBindVarchar(stmt, paramIdx, strPtr) != StateSuccess {
			return fmt.Errorf("failed to bind parameter as string at index %d", idx)
		}
	}

	return nil
}

// ClearBindings clears all parameter bindings on a prepared statement
func (d *DuckDB) ClearBindings(stmt PreparedStatement) error {
	if d.duckdbClearBindings(stmt) != StateSuccess {
		return fmt.Errorf("failed to clear bindings")
	}
	return nil
}
