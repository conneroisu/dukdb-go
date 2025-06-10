# Complex Types Example

This example demonstrates DuckDB's advanced complex data types that go beyond traditional SQL databases. These types enable sophisticated data modeling and analysis directly in the database.

## Features Demonstrated

- **UUID Type**: Universally unique identifiers with proper formatting
- **LIST/ARRAY Types**: Variable-length arrays with rich manipulation functions
- **STRUCT Types**: Named composite types for structured data
- **MAP Types**: Key-value mappings with dynamic schemas
- **Nested Complex Types**: Deep nesting and composition of complex types
- **Type-specific Functions**: Built-in functions for each complex type

## Running the Example

```bash
# Run the example
go run main.go

# Run tests
go test -v

# Run benchmarks
go test -bench=. -benchmem
```

## Code Structure

- `main.go` - Main example showcasing all complex types
- `main_test.go` - Comprehensive tests for complex type operations
- `README.md` - This documentation

## What You'll Learn

### 1. UUID Type
```sql
CREATE TABLE users (
    id UUID PRIMARY KEY,
    name VARCHAR
);

INSERT INTO users VALUES (uuid(), 'Alice');
```
- Generate and validate UUIDs
- Use UUIDs as primary keys
- UUID comparison and sorting

### 2. LIST/ARRAY Types
```sql
CREATE TABLE products (
    name VARCHAR,
    tags VARCHAR[]
);

INSERT INTO products VALUES ('Laptop', ['electronics', 'computer']);

-- Array functions
SELECT list_contains(tags, 'electronics') FROM products;
SELECT array_length(tags) FROM products;
SELECT tags[1] FROM products; -- First element
```

### 3. STRUCT Types
```sql
CREATE TABLE employees (
    name VARCHAR,
    address STRUCT(street VARCHAR, city VARCHAR, zip VARCHAR)
);

INSERT INTO employees VALUES (
    'Alice',
    {'street': '123 Main St', 'city': 'New York', 'zip': '10001'}
);

-- Struct field access
SELECT address.city FROM employees;
```

### 4. MAP Types
```sql
CREATE TABLE user_settings (
    user_id INTEGER,
    preferences MAP(VARCHAR, VARCHAR)
);

INSERT INTO user_settings VALUES (
    1,
    MAP(['theme', 'language'], ['dark', 'en'])
);

-- Map access and functions
SELECT preferences['theme'] FROM user_settings;
SELECT map_keys(preferences) FROM user_settings;
SELECT cardinality(preferences) FROM user_settings;
```

### 5. Nested Complex Types
```sql
CREATE TABLE orders (
    order_id UUID,
    customer STRUCT(
        name VARCHAR,
        addresses STRUCT(
            billing STRUCT(street VARCHAR, city VARCHAR),
            shipping STRUCT(street VARCHAR, city VARCHAR)
        )
    ),
    items STRUCT(
        product_ids INTEGER[],
        metadata MAP(VARCHAR, VARCHAR)
    )
);

-- Deep nested access
SELECT customer.addresses.billing.city FROM orders;
SELECT items.metadata['priority'] FROM orders;
```

## Key Concepts

### Type Safety
```go
// Go types for DuckDB complex types
var uuid types.UUID
var list types.List
var struct_ types.Struct
var map_ types.Map
```

### Performance Benefits
- **Columnar Storage**: Complex types stored efficiently
- **Pushdown Optimization**: Operations pushed to storage layer
- **Vectorized Processing**: Bulk operations on complex types
- **Memory Efficiency**: Reduced serialization overhead

### Advanced Functions

#### Array Functions
```sql
-- Array manipulation
array_length(arr)          -- Get array length
list_contains(arr, value)  -- Check if value exists
array_concat(arr1, arr2)   -- Concatenate arrays
array_slice(arr, start, end) -- Extract subarray
```

#### Struct Functions
```sql
-- Struct operations
struct_extract(struct, 'field')  -- Extract field value
struct_pack(a := 1, b := 'text') -- Create struct
```

#### Map Functions
```sql
-- Map operations
map_keys(map)              -- Get all keys
map_values(map)            -- Get all values
cardinality(map)           -- Get map size
map_extract(map, key)      -- Extract value by key
```

## Use Cases

### 1. Document Storage
Store semi-structured data like JSON documents:
```sql
CREATE TABLE documents (
    id UUID,
    metadata MAP(VARCHAR, VARCHAR),
    content STRUCT(title VARCHAR, body VARCHAR, tags VARCHAR[])
);
```

### 2. User Profiles
Complex user data with preferences and settings:
```sql
CREATE TABLE user_profiles (
    user_id UUID,
    profile STRUCT(
        personal STRUCT(name VARCHAR, email VARCHAR),
        preferences MAP(VARCHAR, VARCHAR),
        tags VARCHAR[]
    )
);
```

### 3. IoT Data
Time-series data with complex nested structures:
```sql
CREATE TABLE sensor_data (
    device_id UUID,
    timestamp TIMESTAMP,
    readings MAP(VARCHAR, DOUBLE),
    metadata STRUCT(location VARCHAR, status VARCHAR)
);
```

### 4. E-commerce
Product catalogs with variable attributes:
```sql
CREATE TABLE products (
    product_id UUID,
    name VARCHAR,
    attributes MAP(VARCHAR, VARCHAR),
    variants STRUCT(
        colors VARCHAR[],
        sizes VARCHAR[],
        prices MAP(VARCHAR, DECIMAL)
    )
);
```

## Advanced Patterns

### Data Normalization vs Denormalization
Complex types allow controlled denormalization while maintaining query performance:

```sql
-- Instead of multiple tables
CREATE TABLE order_items (...);
CREATE TABLE order_addresses (...);

-- Use nested structures
CREATE TABLE orders (
    order_id UUID,
    items STRUCT(products INTEGER[], quantities INTEGER[]),
    addresses STRUCT(billing STRUCT(...), shipping STRUCT(...))
);
```

### Schema Evolution
Complex types provide flexibility for evolving schemas:

```sql
-- Add new fields to existing structs
ALTER TABLE users ADD COLUMN profile STRUCT(bio VARCHAR, avatar_url VARCHAR);

-- Extend maps dynamically
UPDATE user_settings SET preferences = map_concat(preferences, MAP(['new_setting'], ['value']));
```

This example showcases DuckDB's unique strength in handling complex, nested data structures while maintaining SQL familiarity and analytical performance.