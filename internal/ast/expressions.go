package ast

import (
	"fmt"
	"strings"
)

// BinaryExpr represents a binary expression (e.g., a + b, a = b)
type BinaryExpr struct {
	Left     Expression
	Operator BinaryOperator
	Right    Expression
}

type BinaryOperator int

const (
	// Arithmetic operators
	Add BinaryOperator = iota
	Subtract
	Multiply
	Divide
	Modulo
	Power
	
	// Comparison operators
	Equal
	NotEqual
	LessThan
	LessThanOrEqual
	GreaterThan
	GreaterThanOrEqual
	
	// Logical operators
	And
	Or
	
	// String operators
	Concat
	Like
	ILike
	
	// Other operators
	In
	NotIn
	Is
	IsNot
)

func (o BinaryOperator) String() string {
	switch o {
	case Add:
		return "+"
	case Subtract:
		return "-"
	case Multiply:
		return "*"
	case Divide:
		return "/"
	case Modulo:
		return "%"
	case Power:
		return "^"
	case Equal:
		return "="
	case NotEqual:
		return "<>"
	case LessThan:
		return "<"
	case LessThanOrEqual:
		return "<="
	case GreaterThan:
		return ">"
	case GreaterThanOrEqual:
		return ">="
	case And:
		return "AND"
	case Or:
		return "OR"
	case Concat:
		return "||"
	case Like:
		return "LIKE"
	case ILike:
		return "ILIKE"
	case In:
		return "IN"
	case NotIn:
		return "NOT IN"
	case Is:
		return "IS"
	case IsNot:
		return "IS NOT"
	default:
		return "UNKNOWN"
	}
}

func (b *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left.String(), b.Operator.String(), b.Right.String())
}

func (b *BinaryExpr) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(b)
}

func (b *BinaryExpr) DataType() DataType {
	// Simplified type inference - in a real implementation this would be more sophisticated
	switch b.Operator {
	case Equal, NotEqual, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual, And, Or, Like, ILike, In, NotIn, Is, IsNot:
		return Boolean
	case Add, Subtract, Multiply, Divide, Modulo, Power:
		// Return the "larger" type between left and right
		if b.Left.DataType() == Double || b.Right.DataType() == Double {
			return Double
		}
		if b.Left.DataType() == Float || b.Right.DataType() == Float {
			return Float
		}
		if b.Left.DataType() == BigInt || b.Right.DataType() == BigInt {
			return BigInt
		}
		return Integer
	case Concat:
		return Varchar
	default:
		return b.Left.DataType()
	}
}

// UnaryExpr represents a unary expression (e.g., NOT a, -a)
type UnaryExpr struct {
	Operator UnaryOperator
	Operand  Expression
}

type UnaryOperator int

const (
	Not UnaryOperator = iota
	Minus
	Plus
	IsNull
	IsNotNull
)

func (o UnaryOperator) String() string {
	switch o {
	case Not:
		return "NOT"
	case Minus:
		return "-"
	case Plus:
		return "+"
	case IsNull:
		return "IS NULL"
	case IsNotNull:
		return "IS NOT NULL"
	default:
		return "UNKNOWN"
	}
}

func (u *UnaryExpr) String() string {
	if u.Operator == IsNull || u.Operator == IsNotNull {
		return fmt.Sprintf("(%s %s)", u.Operand.String(), u.Operator.String())
	}
	return fmt.Sprintf("(%s%s)", u.Operator.String(), u.Operand.String())
}

func (u *UnaryExpr) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(u)
}

func (u *UnaryExpr) DataType() DataType {
	switch u.Operator {
	case Not, IsNull, IsNotNull:
		return Boolean
	case Minus, Plus:
		return u.Operand.DataType()
	default:
		return u.Operand.DataType()
	}
}

// LiteralExpr represents a literal value
type LiteralExpr struct {
	Value interface{}
	Type  DataType
}

func (l *LiteralExpr) String() string {
	switch v := l.Value.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (l *LiteralExpr) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(l)
}

func (l *LiteralExpr) DataType() DataType {
	return l.Type
}

// ColumnRef represents a column reference
type ColumnRef struct {
	Table  string
	Column string
	Type   DataType
}

func (c *ColumnRef) String() string {
	if c.Table != "" {
		return fmt.Sprintf("%s.%s", c.Table, c.Column)
	}
	return c.Column
}

func (c *ColumnRef) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(c)
}

func (c *ColumnRef) DataType() DataType {
	return c.Type
}

// FunctionCall represents a function call
type FunctionCall struct {
	Name      string
	Arguments []Expression
	Distinct  bool
	ReturnType DataType
}

func (f *FunctionCall) String() string {
	var args []string
	for _, arg := range f.Arguments {
		args = append(args, arg.String())
	}
	
	result := f.Name + "("
	if f.Distinct {
		result += "DISTINCT "
	}
	result += strings.Join(args, ", ") + ")"
	
	return result
}

func (f *FunctionCall) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(f)
}

func (f *FunctionCall) DataType() DataType {
	return f.ReturnType
}

// SubqueryExpr represents a subquery expression
type SubqueryExpr struct {
	Query *SelectStmt
}

func (s *SubqueryExpr) String() string {
	return fmt.Sprintf("(%s)", s.Query.String())
}

func (s *SubqueryExpr) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(s)
}

func (s *SubqueryExpr) DataType() DataType {
	// For simplicity, assume subqueries return the type of their first column
	if len(s.Query.SelectList) > 0 {
		return s.Query.SelectList[0].Expression.DataType()
	}
	return Varchar
}

// ListExpr represents a DuckDB LIST expression
type ListExpr struct {
	Elements    []Expression
	ElementType DataType
}

func (l *ListExpr) String() string {
	var elements []string
	for _, elem := range l.Elements {
		elements = append(elements, elem.String())
	}
	return fmt.Sprintf("[%s]", strings.Join(elements, ", "))
}

func (l *ListExpr) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(l)
}

func (l *ListExpr) DataType() DataType {
	return &ListType{ElementType: l.ElementType}
}

// StructExpr represents a DuckDB STRUCT expression
type StructExpr struct {
	Fields map[string]Expression
}

func (s *StructExpr) String() string {
	var fields []string
	for name, expr := range s.Fields {
		fields = append(fields, fmt.Sprintf("%s: %s", name, expr.String()))
	}
	return fmt.Sprintf("{%s}", strings.Join(fields, ", "))
}

func (s *StructExpr) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(s)
}

func (s *StructExpr) DataType() DataType {
	var structFields []StructField
	for name, expr := range s.Fields {
		structFields = append(structFields, StructField{
			Name: name,
			Type: expr.DataType(),
		})
	}
	return &StructType{Fields: structFields}
}

// CaseExpr represents a CASE expression
type CaseExpr struct {
	Expression Expression        // Optional CASE expression
	WhenClauses []WhenClause
	ElseClause  Expression       // Optional ELSE clause
}

type WhenClause struct {
	Condition Expression
	Result    Expression
}

func (c *CaseExpr) String() string {
	result := "CASE"
	
	if c.Expression != nil {
		result += " " + c.Expression.String()
	}
	
	for _, when := range c.WhenClauses {
		result += fmt.Sprintf(" WHEN %s THEN %s", when.Condition.String(), when.Result.String())
	}
	
	if c.ElseClause != nil {
		result += " ELSE " + c.ElseClause.String()
	}
	
	result += " END"
	return result
}

func (c *CaseExpr) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(c)
}

func (c *CaseExpr) DataType() DataType {
	// Return the type of the first WHEN clause result
	if len(c.WhenClauses) > 0 {
		return c.WhenClauses[0].Result.DataType()
	}
	if c.ElseClause != nil {
		return c.ElseClause.DataType()
	}
	return Varchar
}

// WindowExpr represents a window function expression
type WindowExpr struct {
	Function   *FunctionCall
	PartitionBy []Expression
	OrderBy     []OrderItem
	Frame       *WindowFrame
}

type WindowFrame struct {
	Type  WindowFrameType
	Start WindowFrameBound
	End   WindowFrameBound
}

type WindowFrameType int

const (
	RowsFrame WindowFrameType = iota
	RangeFrame
)

func (w WindowFrameType) String() string {
	switch w {
	case RowsFrame:
		return "ROWS"
	case RangeFrame:
		return "RANGE"
	default:
		return "ROWS"
	}
}

type WindowFrameBound struct {
	Type WindowFrameBoundType
	Offset Expression
}

type WindowFrameBoundType int

const (
	UnboundedPreceding WindowFrameBoundType = iota
	UnboundedFollowing
	CurrentRow
	Preceding
	Following
)

func (w WindowFrameBoundType) String() string {
	switch w {
	case UnboundedPreceding:
		return "UNBOUNDED PRECEDING"
	case UnboundedFollowing:
		return "UNBOUNDED FOLLOWING"
	case CurrentRow:
		return "CURRENT ROW"
	case Preceding:
		return "PRECEDING"
	case Following:
		return "FOLLOWING"
	default:
		return "CURRENT ROW"
	}
}

func (w *WindowExpr) String() string {
	result := w.Function.String() + " OVER ("
	
	if len(w.PartitionBy) > 0 {
		var partitions []string
		for _, expr := range w.PartitionBy {
			partitions = append(partitions, expr.String())
		}
		result += "PARTITION BY " + strings.Join(partitions, ", ")
	}
	
	if len(w.OrderBy) > 0 {
		if len(w.PartitionBy) > 0 {
			result += " "
		}
		var orders []string
		for _, order := range w.OrderBy {
			orders = append(orders, order.String())
		}
		result += "ORDER BY " + strings.Join(orders, ", ")
	}
	
	if w.Frame != nil {
		if len(w.PartitionBy) > 0 || len(w.OrderBy) > 0 {
			result += " "
		}
		result += w.Frame.Type.String() + " BETWEEN " + 
				  w.Frame.Start.Type.String() + " AND " + 
				  w.Frame.End.Type.String()
	}
	
	result += ")"
	return result
}

func (w *WindowExpr) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(w)
}

func (w *WindowExpr) DataType() DataType {
	return w.Function.DataType()
}

func (w *WindowExpr) expression() {}

// TableName implements Expression for subqueries in FROM clauses
func (t *TableName) DataType() DataType {
	return Varchar  // Tables don't really have a data type, but we need to satisfy the interface
}
func (t *TableName) expression() {}

// JoinExpr implements Expression for complex joins
func (j *JoinExpr) DataType() DataType {
	return Varchar  // Joins don't have a data type
}
func (j *JoinExpr) expression() {}