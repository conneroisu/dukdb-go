# Known Issues with Benchmarks

## Type Conversion Issue in Pure-Go Implementation

**Status**: 🔴 Blocking benchmark execution

**Description**: 
The pure-Go DuckDB implementation has a type handling issue where integer parameters passed to SQL queries are being converted to strings somewhere in the driver/engine layer, causing type mismatch errors.

**Error Message**:
```
insert failed: failed to set value at col 0: expected int32, got string
```

**Location**:
- Error originates from: `/internal/storage/vector.go:283-292` in `setFlatValue` method
- Called from: `/internal/engine/catalog.go:218` in `SetValue`

**Impact**:
- All benchmarks that use parameterized INSERT statements fail
- Prevents proper performance comparison between pure-Go and CGO implementations

**Workaround**:
Until the type conversion issue is fixed in the driver, benchmarks cannot run properly. The issue needs to be addressed in the driver's parameter handling code.

**Suggested Fix**:
1. Check the driver's `Exec` and `Query` methods to see how SQL parameters are processed
2. Ensure proper type preservation from the database/sql interface to the storage layer
3. Add automatic type coercion for common conversions (int → int32, int64 → int32, etc.)

## CGO Driver Availability

**Status**: ⚠️ Warning

**Description**:
The CGO DuckDB driver benchmarks are automatically skipped when:
- CGO is disabled (`CGO_ENABLED=0`)
- The marcboeker/go-duckdb package is not installed

**Workaround**:
```bash
# Install the CGO driver
go get github.com/marcboeker/go-duckdb

# Run benchmarks with CGO enabled
CGO_ENABLED=1 go test -bench=. ./benchmarks/...
```