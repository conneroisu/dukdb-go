package ast

import (
	"fmt"
	"strings"
)

// Node represents a node in the abstract syntax tree
type Node interface {
	String() string
	Accept(visitor Visitor) interface{}
}

// Visitor defines the visitor pattern interface for AST traversal
type Visitor interface {
	VisitSelectStmt(*SelectStmt) interface{}
	VisitInsertStmt(*InsertStmt) interface{}
	VisitUpdateStmt(*UpdateStmt) interface{}
	VisitDeleteStmt(*DeleteStmt) interface{}
	VisitCreateTableStmt(*CreateTableStmt) interface{}
	VisitDropTableStmt(*DropTableStmt) interface{}
	VisitExpression(Expression) interface{}
}

// Statement represents a SQL statement
type Statement interface {
	Node
	statement()
}

// Expression represents a SQL expression
type Expression interface {
	Node
	expression()
	DataType() DataType
}

// DataType represents SQL data types
type DataType interface {
	String() string
	IsNumeric() bool
	IsComparable() bool
}

// Basic data types
type ScalarType struct {
	Name string
	Size int
}

func (t *ScalarType) String() string     { return t.Name }
func (t *ScalarType) IsNumeric() bool    { return strings.Contains(strings.ToUpper(t.Name), "INT") || strings.Contains(strings.ToUpper(t.Name), "FLOAT") || strings.Contains(strings.ToUpper(t.Name), "DOUBLE") || strings.Contains(strings.ToUpper(t.Name), "DECIMAL") }
func (t *ScalarType) IsComparable() bool { return true }

// Complex data types
type ListType struct {
	ElementType DataType
}

func (t *ListType) String() string     { return fmt.Sprintf("LIST<%s>", t.ElementType.String()) }
func (t *ListType) IsNumeric() bool    { return false }
func (t *ListType) IsComparable() bool { return false }

type StructType struct {
	Fields []StructField
}

type StructField struct {
	Name string
	Type DataType
}

func (t *StructType) String() string {
	var fields []string
	for _, field := range t.Fields {
		fields = append(fields, fmt.Sprintf("%s: %s", field.Name, field.Type.String()))
	}
	return fmt.Sprintf("STRUCT<%s>", strings.Join(fields, ", "))
}
func (t *StructType) IsNumeric() bool    { return false }
func (t *StructType) IsComparable() bool { return false }

type MapType struct {
	KeyType   DataType
	ValueType DataType
}

func (t *MapType) String() string     { return fmt.Sprintf("MAP<%s, %s>", t.KeyType.String(), t.ValueType.String()) }
func (t *MapType) IsNumeric() bool    { return false }
func (t *MapType) IsComparable() bool { return false }

// Predefined types
var (
	Boolean   = &ScalarType{Name: "BOOLEAN", Size: 1}
	TinyInt   = &ScalarType{Name: "TINYINT", Size: 1}
	SmallInt  = &ScalarType{Name: "SMALLINT", Size: 2}
	Integer   = &ScalarType{Name: "INTEGER", Size: 4}
	BigInt    = &ScalarType{Name: "BIGINT", Size: 8}
	Float     = &ScalarType{Name: "FLOAT", Size: 4}
	Double    = &ScalarType{Name: "DOUBLE", Size: 8}
	Decimal   = &ScalarType{Name: "DECIMAL", Size: 16}
	Varchar   = &ScalarType{Name: "VARCHAR", Size: -1}
	Date      = &ScalarType{Name: "DATE", Size: 4}
	Time      = &ScalarType{Name: "TIME", Size: 8}
	Timestamp = &ScalarType{Name: "TIMESTAMP", Size: 8}
	Interval  = &ScalarType{Name: "INTERVAL", Size: 16}
	Blob      = &ScalarType{Name: "BLOB", Size: -1}
	UUID      = &ScalarType{Name: "UUID", Size: 16}
)

// Base implementations
func (s *SelectStmt) statement()      {}
func (s *InsertStmt) statement()      {}
func (s *UpdateStmt) statement()      {}
func (s *DeleteStmt) statement()      {}
func (s *CreateTableStmt) statement() {}
func (s *DropTableStmt) statement()   {}

func (e *BinaryExpr) expression()   {}
func (e *UnaryExpr) expression()    {}
func (e *LiteralExpr) expression()  {}
func (e *ColumnRef) expression()    {}
func (e *FunctionCall) expression() {}
func (e *SubqueryExpr) expression() {}
func (e *ListExpr) expression()     {}
func (e *StructExpr) expression()   {}
func (e *CaseExpr) expression()     {}