// Package duckdb provides a pure-Go DuckDB driver for database/sql
//
// This driver uses purego for FFI instead of CGO, allowing for easier
// cross-compilation and deployment. It maintains compatibility with
// the standard database/sql interface.
//
// Usage:
//
//	import (
//	    "database/sql"
//	    _ "github.com/connerohnesorge/dukdb-go"
//	)
//	
//	func main() {
//	    db, err := sql.Open("duckdb", "mydb.db")
//	    if err != nil {
//	        log.Fatal(err)
//	    }
//	    defer db.Close()
//	    
//	    // Use db as normal...
//	}
package duckdb

import (
	// Register the driver
	_ "github.com/connerohnesorge/dukdb-go/driver"
)

// Version returns the version of the DuckDB Go driver
const Version = "0.1.0"