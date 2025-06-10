# Working with Complex Types

DuckDB supports advanced data types beyond traditional SQL types. This guide covers how to work with these complex types using the **dukdb-go** driver.

## Overview

DuckDB's complex types enable rich data modeling and analytics:

- **LIST** - Arrays of values
- **STRUCT** - Named field records
- **MAP** - Key-value pairs
- **UUID** - Universally unique identifiers
- **ENUM** - Enumerated values
- **UNION** - Tagged union types
- **JSON** - Semi-structured data

## LIST Types

Lists (arrays) can contain any DuckDB type, including nested lists.

### Creating LIST Columns

```sql
CREATE TABLE events (
    id INTEGER,
    tags LIST(VARCHAR),
    scores LIST(DOUBLE),
    nested_lists LIST(LIST(INTEGER))
);
```

### Inserting LIST Data

```go
// Insert list data using JSON representation
_, err := db.Exec(`
    INSERT INTO events (id, tags, scores, nested_lists) VALUES 
    (1, ['web', 'api', 'go'], [95.5, 87.2, 92.1], [[1, 2], [3, 4, 5]]),
    (2, ['database', 'analytics'], [88.0, 91.5], [[10], [20, 30]])
`)
if err != nil {
    log.Fatal(err)
}
```

### Querying LIST Data

```go
rows, err := db.Query("SELECT id, tags, scores FROM events")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var tagsJSON, scoresJSON string
    
    err := rows.Scan(&id, &tagsJSON, &scoresJSON)
    if err != nil {
        log.Fatal(err)
    }
    
    // Parse JSON representations
    var tags []string
    var scores []float64
    
    json.Unmarshal([]byte(tagsJSON), &tags)
    json.Unmarshal([]byte(scoresJSON), &scores)
    
    fmt.Printf("Event %d: tags=%v, scores=%v\n", id, tags, scores)
}
```

### Working with LIST Elements

```go
// Access list elements using SQL functions
rows, err := db.Query(`
    SELECT 
        id,
        tags[1] as first_tag,
        len(tags) as tag_count,
        list_contains(tags, 'api') as has_api_tag
    FROM events
`)
```

### Using Helper Functions

The driver provides helper functions for LIST types:

```go
import "github.com/connerohnesorge/dukdb-go/internal/types"

// Convert Go slice to DuckDB LIST format
tagList := types.NewList([]string{"web", "api", "go"})
scoreList := types.NewList([]float64{95.5, 87.2, 92.1})

_, err := db.Exec(
    "INSERT INTO events (id, tags, scores) VALUES (?, ?, ?)", 
    3, tagList.String(), scoreList.String(),
)
```

## STRUCT Types

Structs represent records with named fields.

### Creating STRUCT Columns

```sql
CREATE TABLE users (
    id INTEGER,
    profile STRUCT(name VARCHAR, age INTEGER, email VARCHAR),
    address STRUCT(street VARCHAR, city VARCHAR, zipcode INTEGER)
);
```

### Inserting STRUCT Data

```go
// Insert struct data using JSON-like syntax
_, err := db.Exec(`
    INSERT INTO users (id, profile, address) VALUES 
    (1, {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'}, 
        {'street': '123 Main St', 'city': 'Boston', 'zipcode': 02101}),
    (2, {'name': 'Bob', 'age': 25, 'email': 'bob@example.com'}, 
        {'street': '456 Oak Ave', 'city': 'Cambridge', 'zipcode': 02139})
`)
```

### Querying STRUCT Data

```go
rows, err := db.Query(`
    SELECT 
        id,
        profile,
        profile.name,
        profile.age,
        address.city
    FROM users
`)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var profileJSON, name string
    var age int
    var city string
    
    err := rows.Scan(&id, &profileJSON, &name, &age, &city)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("User %d: %s (%d) from %s\n", id, name, age, city)
}
```

### Using Helper Functions

```go
import "github.com/connerohnesorge/dukdb-go/internal/types"

// Create struct from Go map
profile := types.NewStruct(map[string]interface{}{
    "name":  "Charlie",
    "age":   35,
    "email": "charlie@example.com",
})

address := types.NewStruct(map[string]interface{}{
    "street":  "789 Pine St",
    "city":    "Somerville", 
    "zipcode": 02144,
})

_, err := db.Exec(
    "INSERT INTO users (id, profile, address) VALUES (?, ?, ?)",
    3, profile.String(), address.String(),
)
```

## MAP Types

Maps represent key-value pairs where keys must be of the same type.

### Creating MAP Columns

```sql
CREATE TABLE products (
    id INTEGER,
    attributes MAP(VARCHAR, VARCHAR),
    metrics MAP(VARCHAR, DOUBLE)
);
```

### Inserting MAP Data

```go
_, err := db.Exec(`
    INSERT INTO products (id, attributes, metrics) VALUES 
    (1, map(['color', 'size'], ['red', 'large']), 
        map(['weight', 'price'], [2.5, 19.99])),
    (2, map(['color', 'material'], ['blue', 'cotton']), 
        map(['weight', 'price'], [1.8, 24.99]))
`)
```

### Querying MAP Data

```go
rows, err := db.Query(`
    SELECT 
        id,
        attributes,
        attributes['color'] as color,
        metrics['price'] as price
    FROM products
`)
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var attributesJSON string
    var color string
    var price float64
    
    err := rows.Scan(&id, &attributesJSON, &color, &price)
    if err != nil {
        log.Fatal(err)
    }
    
    // Parse full map
    var attributes map[string]string
    json.Unmarshal([]byte(attributesJSON), &attributes)
    
    fmt.Printf("Product %d: color=%s, price=%.2f, attrs=%v\n", 
        id, color, price, attributes)
}
```

### Using Helper Functions

```go
import "github.com/connerohnesorge/dukdb-go/internal/types"

// Create map from Go map
attributes := types.NewMap(map[string]string{
    "color":    "green",
    "material": "polyester",
})

metrics := types.NewMap(map[string]float64{
    "weight": 2.1,
    "price":  29.99,
})

_, err := db.Exec(
    "INSERT INTO products (id, attributes, metrics) VALUES (?, ?, ?)",
    3, attributes.String(), metrics.String(),
)
```

## UUID Types

UUIDs are first-class types in DuckDB.

### Creating UUID Columns

```sql
CREATE TABLE sessions (
    id UUID PRIMARY KEY,
    user_id UUID,
    created_at TIMESTAMP
);
```

### Working with UUIDs

```go
import (
    "github.com/google/uuid"
    "github.com/connerohnesorge/dukdb-go/internal/types"
)

// Generate UUIDs
sessionID := uuid.New()
userID := uuid.New()

// Insert UUID data
_, err := db.Exec(`
    INSERT INTO sessions (id, user_id, created_at) VALUES (?, ?, ?)
`, sessionID.String(), userID.String(), time.Now())

// Query UUID data
rows, err := db.Query("SELECT id, user_id FROM sessions")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var sessionIDStr, userIDStr string
    err := rows.Scan(&sessionIDStr, &userIDStr)
    if err != nil {
        log.Fatal(err)
    }
    
    // Parse back to UUID
    sessionID, _ := uuid.Parse(sessionIDStr)
    userID, _ := uuid.Parse(userIDStr)
    
    fmt.Printf("Session: %s, User: %s\n", sessionID, userID)
}
```

## ENUM Types

Enums define a fixed set of values.

### Creating ENUM Types

```sql
-- Create ENUM type
CREATE TYPE status AS ENUM ('pending', 'processing', 'completed', 'failed');

CREATE TABLE orders (
    id INTEGER,
    status status,
    priority ENUM ('low', 'medium', 'high')
);
```

### Working with ENUMs

```go
// Insert enum values
_, err := db.Exec(`
    INSERT INTO orders (id, status, priority) VALUES 
    (1, 'pending', 'high'),
    (2, 'processing', 'medium'),
    (3, 'completed', 'low')
`)

// Query enum values
rows, err := db.Query("SELECT id, status, priority FROM orders")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var status, priority string
    
    err := rows.Scan(&id, &status, &priority)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Order %d: %s (%s priority)\n", id, status, priority)
}
```

## JSON Support

While DuckDB has a JSON extension, you can also work with JSON as strings.

### Storing JSON Data

```sql
CREATE TABLE documents (
    id INTEGER,
    data VARCHAR  -- JSON as string
);
```

```go
// Insert JSON data
jsonData := `{"name": "John", "scores": [85, 92, 78], "active": true}`
_, err := db.Exec("INSERT INTO documents (id, data) VALUES (?, ?)", 1, jsonData)

// Query and parse JSON
rows, err := db.Query("SELECT id, data FROM documents")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var jsonStr string
    
    err := rows.Scan(&id, &jsonStr)
    if err != nil {
        log.Fatal(err)
    }
    
    // Parse JSON in Go
    var data map[string]interface{}
    json.Unmarshal([]byte(jsonStr), &data)
    
    fmt.Printf("Document %d: %+v\n", id, data)
}
```

## Nested Complex Types

DuckDB supports arbitrarily nested complex types.

### Complex Nested Example

```sql
CREATE TABLE analytics (
    id INTEGER,
    user_sessions LIST(STRUCT(
        session_id UUID,
        events LIST(STRUCT(
            timestamp TIMESTAMP,
            event_type VARCHAR,
            properties MAP(VARCHAR, VARCHAR)
        ))
    ))
);
```

```go
// Insert nested complex data
_, err := db.Exec(`
    INSERT INTO analytics (id, user_sessions) VALUES (
        1,
        [
            {
                'session_id': 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
                'events': [
                    {
                        'timestamp': '2024-01-01 10:00:00',
                        'event_type': 'page_view',
                        'properties': map(['page'], ['/home'])
                    },
                    {
                        'timestamp': '2024-01-01 10:05:00',
                        'event_type': 'click',
                        'properties': map(['element', 'page'], ['button', '/home'])
                    }
                ]
            }
        ]
    )
`)
```

## Type Conversion Utilities

The driver provides utilities for type conversion:

```go
import "github.com/connerohnesorge/dukdb-go/internal/types"

// Convert Go types to DuckDB format
func convertToListString(slice interface{}) string {
    return types.NewList(slice).String()
}

func convertToStructString(m map[string]interface{}) string {
    return types.NewStruct(m).String()
}

func convertToMapString(m interface{}) string {
    return types.NewMap(m).String()
}

// Example usage
tags := []string{"web", "api", "go"}
profile := map[string]interface{}{
    "name": "Alice",
    "age":  30,
}
metrics := map[string]float64{
    "score": 95.5,
    "time":  123.4,
}

_, err := db.Exec(`
    INSERT INTO complex_table (tags, profile, metrics) VALUES (?, ?, ?)
`, convertToListString(tags), convertToStructString(profile), convertToMapString(metrics))
```

## Performance Considerations

### Indexing Complex Types

```sql
-- Index on struct field
CREATE INDEX idx_profile_name ON users (profile.name);

-- Index on list element
CREATE INDEX idx_first_tag ON events (tags[1]);

-- Index on map value
CREATE INDEX idx_color ON products (attributes['color']);
```

### Memory Usage

Complex types can consume significant memory. Consider:

- Use appropriate nesting levels
- Limit list sizes where possible
- Consider normalizing very large complex structures

### Query Optimization

```sql
-- Efficient: filter on indexed fields
SELECT * FROM users WHERE profile.name = 'Alice';

-- Less efficient: full complex type comparisons
SELECT * FROM users WHERE profile = {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'};
```

## Best Practices

1. **Use Appropriate Types**: Choose the right complex type for your use case
1. **Validate Data**: Ensure complex type data is valid before insertion
1. **Index Strategically**: Index frequently queried fields within complex types
1. **Consider Alternatives**: Sometimes normalization is better than complex nesting
1. **Test Performance**: Complex types can impact query performance

## Error Handling

```go
// Handle type conversion errors
_, err := db.Exec("INSERT INTO events (tags) VALUES (?)", "invalid_json")
if err != nil {
    if strings.Contains(err.Error(), "Invalid JSON") {
        log.Printf("JSON parsing error: %v", err)
        // Handle invalid JSON format
    }
}

// Validate before insertion
func validateListData(data interface{}) error {
    list := types.NewList(data)
    if list == nil {
        return fmt.Errorf("invalid list data: %v", data)
    }
    return nil
}
```

## Next Steps

- [Performance Guide](/guides/performance/) - Optimizing complex type queries
- [Migration Guide](/guides/migration/) - Migrating complex types from other drivers
- [API Reference](/reference/types/) - Complete type system documentation
