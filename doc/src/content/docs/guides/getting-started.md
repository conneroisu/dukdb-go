# Getting Started

Welcome to **dukdb-go**, a pure-Go implementation of the DuckDB database driver. This guide will help you get started using the driver in your Go applications.

## Prerequisites

- **Go 1.21+** - Required for purego support and modern Go features
- **DuckDB shared library** - The driver interfaces with DuckDB's C library
- **Nix (recommended)** - For reproducible development environment

## Installation

### Using Go Modules

Add the driver to your Go project:

```bash
go get github.com/connerohnesorge/dukdb-go
```

### Installing DuckDB Library

The driver requires the DuckDB shared library to be available at runtime.

#### Option 1: Using Nix (Recommended)

If you have Nix installed, the flake provides everything needed:

```bash
# Enter the development shell
nix develop

# DuckDB library is automatically available
```

#### Option 2: Manual Installation

**macOS (Homebrew):**
```bash
brew install duckdb
```

**Ubuntu/Debian:**
```bash
# Add DuckDB APT repository
curl -L https://github.com/duckdb/duckdb/releases/download/v0.10.0/duckdb_cli-linux-amd64.zip -o duckdb.zip
unzip duckdb.zip
sudo mv duckdb /usr/local/bin/
```

**From Source:**
```bash
git clone https://github.com/duckdb/duckdb.git
cd duckdb
make release
# Library will be in build/release/src/libduckdb.so
```

#### Option 3: Environment Variables

Set the `DUCKDB_LIB_DIR` environment variable:

```bash
export DUCKDB_LIB_DIR="/path/to/duckdb/lib"
```

## Basic Usage

### Importing the Driver

Import the driver in your Go code. The underscore import registers the driver with Go's sql package:

```go
import (
    "database/sql"
    _ "github.com/connerohnesorge/dukdb-go/driver"
)
```

### Opening a Database Connection

```go
package main

import (
    "database/sql"
    "log"
    
    _ "github.com/connerohnesorge/dukdb-go/driver"
)

func main() {
    // In-memory database
    db, err := sql.Open("duckdb", ":memory:")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Test the connection
    err = db.Ping()
    if err != nil {
        log.Fatal("Failed to connect to DuckDB:", err)
    }
    
    log.Println("Connected to DuckDB successfully!")
}
```

### File-based Database

```go
// Persistent database file
db, err := sql.Open("duckdb", "/path/to/database.db")
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

### Connection String Options

The driver supports various connection string options:

```go
// Read-only database
db, err := sql.Open("duckdb", "/path/to/database.db?access_mode=read_only")

// Set memory limit
db, err := sql.Open("duckdb", ":memory:?memory_limit=1GB")

// Enable threading
db, err := sql.Open("duckdb", ":memory:?threads=4")
```

## Executing Queries

### Simple Queries

```go
// Execute a simple query
rows, err := db.Query("SELECT 42 as answer, 'Hello' as greeting")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var answer int
    var greeting string
    err := rows.Scan(&answer, &greeting)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Answer: %d, Greeting: %s\n", answer, greeting)
}
```

### Creating Tables and Inserting Data

```go
// Create a table
_, err := db.Exec(`
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR,
        email VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
`)
if err != nil {
    log.Fatal(err)
}

// Insert data
_, err = db.Exec(`
    INSERT INTO users (name, email) VALUES 
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com')
`)
if err != nil {
    log.Fatal(err)
}

// Query the data
rows, err := db.Query("SELECT id, name, email FROM users")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var name, email string
    err := rows.Scan(&id, &name, &email)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("User %d: %s (%s)\n", id, name, email)
}
```

## Using Prepared Statements

Prepared statements improve performance and security:

```go
// Prepare a statement
stmt, err := db.Prepare("INSERT INTO users (name, email) VALUES (?, ?)")
if err != nil {
    log.Fatal(err)
}
defer stmt.Close()

// Execute multiple times
users := [][]interface{}{
    {"Charlie", "charlie@example.com"},
    {"Diana", "diana@example.com"},
}

for _, user := range users {
    _, err := stmt.Exec(user[0], user[1])
    if err != nil {
        log.Fatal(err)
    }
}
```

## Working with Transactions

```go
// Begin a transaction
tx, err := db.Begin()
if err != nil {
    log.Fatal(err)
}

// Execute operations within the transaction
_, err = tx.Exec("INSERT INTO users (name, email) VALUES (?, ?)", "Eve", "eve@example.com")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

_, err = tx.Exec("UPDATE users SET email = ? WHERE name = ?", "alice.new@example.com", "Alice")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Commit the transaction
err = tx.Commit()
if err != nil {
    log.Fatal(err)
}
```

## Context Support

The driver supports Go's context for cancellation and timeouts:

```go
import (
    "context"
    "time"
)

// Query with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

rows, err := db.QueryContext(ctx, "SELECT * FROM large_table")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

// Process results...
```

## Error Handling

The driver provides detailed error information:

```go
_, err := db.Exec("SELECT * FROM non_existent_table")
if err != nil {
    // Cast to driver-specific error for more details
    if duckdbErr, ok := err.(*driver.Error); ok {
        fmt.Printf("DuckDB Error Code: %d\n", duckdbErr.Code)
        fmt.Printf("DuckDB Error Message: %s\n", duckdbErr.Message)
    }
    log.Fatal(err)
}
```

## Performance Configuration

### Connection Pooling

Configure connection pool settings:

```go
db, err := sql.Open("duckdb", ":memory:")
if err != nil {
    log.Fatal(err)
}

// Configure connection pool
db.SetMaxOpenConns(10)       // Maximum number of open connections
db.SetMaxIdleConns(5)        // Maximum number of idle connections
db.SetConnMaxLifetime(time.Hour) // Maximum connection lifetime
```

### Statement Caching

The driver automatically caches prepared statements. Configure cache size:

```go
import "github.com/connerohnesorge/dukdb-go/driver"

// Set global cache size (default: 100)
driver.SetStatementCacheSize(200)
```

## Next Steps

- [Build Process](/guides/build-process/) - Learn how to build the driver from source
- [Complex Types](/guides/complex-types/) - Working with DuckDB's advanced data types
- [Performance Optimization](/guides/performance/) - Tips for optimal performance
- [Migration Guide](/guides/migration/) - Migrating from the CGO driver
- [API Reference](/reference/api/) - Complete API documentation

## Common Issues

### Library Not Found

If you get a "library not found" error:

1. Ensure DuckDB is installed and in your system's library path
2. Set the `DUCKDB_LIB_DIR` environment variable
3. Use the Nix development environment which handles this automatically

### Build Errors

For build issues:

1. Ensure you're using Go 1.21 or later
2. Run `go mod tidy` to ensure dependencies are up to date
3. Check the [Build Process](/guides/build-process/) guide for detailed instructions