package engine

import (
	"fmt"
	"strings"
	"sync"

	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

// Catalog manages database schemas and tables
type Catalog struct {
	schemas map[string]*Schema
	mu      sync.RWMutex
}

// NewCatalog creates a new catalog
func NewCatalog() *Catalog {
	c := &Catalog{
		schemas: make(map[string]*Schema),
	}
	
	// Create default schemas
	c.CreateSchema("main")
	c.CreateSchema("temp")
	c.CreateSchema("information_schema")
	
	return c
}

// CreateSchema creates a new schema
func (c *Catalog) CreateSchema(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	name = strings.ToLower(name)
	if _, exists := c.schemas[name]; exists {
		return fmt.Errorf("schema %s already exists", name)
	}
	
	c.schemas[name] = &Schema{
		name:   name,
		tables: make(map[string]*Table),
	}
	
	return nil
}

// GetSchema retrieves a schema by name
func (c *Catalog) GetSchema(name string) (*Schema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	name = strings.ToLower(name)
	schema, exists := c.schemas[name]
	if !exists {
		return nil, fmt.Errorf("schema %s not found", name)
	}
	
	return schema, nil
}

// CreateTable creates a new table in the specified schema
func (c *Catalog) CreateTable(schemaName, tableName string, columns []ColumnDefinition) error {
	schema, err := c.GetSchema(schemaName)
	if err != nil {
		return err
	}
	
	return schema.CreateTable(tableName, columns)
}

// GetTable retrieves a table from the catalog
func (c *Catalog) GetTable(schemaName, tableName string) (*Table, error) {
	schema, err := c.GetSchema(schemaName)
	if err != nil {
		return nil, err
	}
	
	return schema.GetTable(tableName)
}

// DropTable drops a table from the catalog
func (c *Catalog) DropTable(schemaName, tableName string, ifExists bool) error {
	schema, err := c.GetSchema(schemaName)
	if err != nil {
		if ifExists {
			return nil // Schema doesn't exist, but IF EXISTS was specified
		}
		return err
	}
	
	return schema.DropTable(tableName, ifExists)
}

// Schema represents a database schema
type Schema struct {
	name   string
	tables map[string]*Table
	mu     sync.RWMutex
}

// CreateTable creates a new table in the schema
func (s *Schema) CreateTable(name string, columns []ColumnDefinition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	name = strings.ToLower(name)
	if _, exists := s.tables[name]; exists {
		return fmt.Errorf("table %s already exists in schema %s", name, s.name)
	}
	
	table := &Table{
		schema:  s.name,
		name:    name,
		columns: columns,
		data:    NewTableData(columns),
	}
	
	s.tables[name] = table
	return nil
}

// GetTable retrieves a table from the schema
func (s *Schema) GetTable(name string) (*Table, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	name = strings.ToLower(name)
	table, exists := s.tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s not found in schema %s", name, s.name)
	}
	
	return table, nil
}

// DropTable drops a table from the schema
func (s *Schema) DropTable(name string, ifExists bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	name = strings.ToLower(name)
	_, exists := s.tables[name]
	if !exists {
		if ifExists {
			return nil // Table doesn't exist, but IF EXISTS was specified
		}
		return fmt.Errorf("table %s not found in schema %s", name, s.name)
	}
	
	delete(s.tables, name)
	return nil
}

// Table represents a database table
type Table struct {
	schema  string
	name    string
	columns []ColumnDefinition
	data    *TableData
	mu      sync.RWMutex
}

// ColumnDefinition defines a table column
type ColumnDefinition struct {
	Name     string
	Type     storage.LogicalType
	Nullable bool
	Default  interface{}
}

// GetColumns returns the table's column definitions
func (t *Table) GetColumns() []ColumnDefinition {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.columns
}

// Insert inserts data into the table
func (t *Table) Insert(values [][]interface{}) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	return t.data.Insert(values)
}

// Scan performs a full table scan
func (t *Table) Scan() ([]*storage.DataChunk, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	return t.data.Scan()
}

// TableData manages the actual data storage for a table
type TableData struct {
	columns []ColumnDefinition
	chunks  []*storage.DataChunk
	rowCount int64
	mu      sync.RWMutex
}

// NewTableData creates a new table data storage
func NewTableData(columns []ColumnDefinition) *TableData {
	return &TableData{
		columns: columns,
		chunks:  make([]*storage.DataChunk, 0),
	}
}

// Insert inserts rows into the table
func (td *TableData) Insert(rows [][]interface{}) error {
	td.mu.Lock()
	defer td.mu.Unlock()
	
	// Convert column definitions to logical types
	types := make([]storage.LogicalType, len(td.columns))
	for i, col := range td.columns {
		types[i] = col.Type
	}
	
	// Create a new chunk if needed or append to existing
	chunkSize := storage.DefaultVectorSize
	
	for _, row := range rows {
		if len(row) != len(td.columns) {
			return fmt.Errorf("row has %d values, expected %d", len(row), len(td.columns))
		}
		
		// Find or create a chunk with space
		var targetChunk *storage.DataChunk
		for _, chunk := range td.chunks {
			if chunk.Size() < chunkSize {
				targetChunk = chunk
				break
			}
		}
		
		if targetChunk == nil {
			targetChunk = storage.NewDataChunk(types, chunkSize)
			td.chunks = append(td.chunks, targetChunk)
		}
		
		// Insert the row
		rowIdx := targetChunk.Size()
		for colIdx, value := range row {
			if err := targetChunk.SetValue(colIdx, rowIdx, value); err != nil {
				return fmt.Errorf("failed to set value at col %d: %w", colIdx, err)
			}
		}
		
		targetChunk.SetSize(rowIdx + 1)
		td.rowCount++
	}
	
	return nil
}

// Scan returns all data chunks
func (td *TableData) Scan() ([]*storage.DataChunk, error) {
	td.mu.RLock()
	defer td.mu.RUnlock()
	
	// Return a copy of the chunks slice
	result := make([]*storage.DataChunk, len(td.chunks))
	copy(result, td.chunks)
	
	return result, nil
}

// GetRowCount returns the total number of rows
func (td *TableData) GetRowCount() int64 {
	td.mu.RLock()
	defer td.mu.RUnlock()
	return td.rowCount
}

// ReplaceChunks replaces all data chunks with new ones
func (t *Table) ReplaceChunks(newChunks []*storage.DataChunk) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	return t.data.ReplaceChunks(newChunks)
}

// ReplaceChunks replaces all data chunks with new ones
func (td *TableData) ReplaceChunks(newChunks []*storage.DataChunk) error {
	td.mu.Lock()
	defer td.mu.Unlock()
	
	// Calculate new row count
	newRowCount := int64(0)
	for _, chunk := range newChunks {
		newRowCount += int64(chunk.Size())
	}
	
	td.chunks = newChunks
	td.rowCount = newRowCount
	
	return nil
}

