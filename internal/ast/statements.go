package ast

import (
	"fmt"
	"strings"
)

// SelectStmt represents a SELECT statement
type SelectStmt struct {
	SelectList []SelectItem
	From       []TableRef
	Where      Expression
	GroupBy    []Expression
	Having     Expression
	OrderBy    []OrderItem
	Limit      *int64
	Offset     *int64
	Distinct   bool
}

func (s *SelectStmt) String() string {
	var parts []string
	
	selectClause := "SELECT"
	if s.Distinct {
		selectClause += " DISTINCT"
	}
	
	var selectItems []string
	for _, item := range s.SelectList {
		selectItems = append(selectItems, item.String())
	}
	parts = append(parts, selectClause+" "+strings.Join(selectItems, ", "))
	
	if len(s.From) > 0 {
		var fromItems []string
		for _, table := range s.From {
			fromItems = append(fromItems, table.String())
		}
		parts = append(parts, "FROM "+strings.Join(fromItems, ", "))
	}
	
	if s.Where != nil {
		parts = append(parts, "WHERE "+s.Where.String())
	}
	
	if len(s.GroupBy) > 0 {
		var groupItems []string
		for _, expr := range s.GroupBy {
			groupItems = append(groupItems, expr.String())
		}
		parts = append(parts, "GROUP BY "+strings.Join(groupItems, ", "))
	}
	
	if s.Having != nil {
		parts = append(parts, "HAVING "+s.Having.String())
	}
	
	if len(s.OrderBy) > 0 {
		var orderItems []string
		for _, item := range s.OrderBy {
			orderItems = append(orderItems, item.String())
		}
		parts = append(parts, "ORDER BY "+strings.Join(orderItems, ", "))
	}
	
	if s.Limit != nil {
		parts = append(parts, fmt.Sprintf("LIMIT %d", *s.Limit))
	}
	
	if s.Offset != nil {
		parts = append(parts, fmt.Sprintf("OFFSET %d", *s.Offset))
	}
	
	return strings.Join(parts, " ")
}

func (s *SelectStmt) Accept(visitor Visitor) interface{} {
	return visitor.VisitSelectStmt(s)
}

// SelectItem represents an item in the SELECT list
type SelectItem struct {
	Expression Expression
	Alias      string
}

func (s *SelectItem) String() string {
	if s.Alias != "" {
		return fmt.Sprintf("%s AS %s", s.Expression.String(), s.Alias)
	}
	return s.Expression.String()
}

// TableRef represents a table reference in the FROM clause
type TableRef interface {
	Node
	tableRef()
}

// TableName represents a simple table name
type TableName struct {
	Schema string
	Name   string
	Alias  string
}

func (t *TableName) String() string {
	name := t.Name
	if t.Schema != "" {
		name = t.Schema + "." + name
	}
	if t.Alias != "" {
		name += " AS " + t.Alias
	}
	return name
}

func (t *TableName) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(t)
}

func (t *TableName) tableRef() {}

// JoinExpr represents a JOIN expression
type JoinExpr struct {
	Left      TableRef
	Right     TableRef
	JoinType  JoinType
	Condition Expression
}

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
	CrossJoin
)

func (j JoinType) String() string {
	switch j {
	case InnerJoin:
		return "INNER JOIN"
	case LeftJoin:
		return "LEFT JOIN"
	case RightJoin:
		return "RIGHT JOIN"
	case FullJoin:
		return "FULL JOIN"
	case CrossJoin:
		return "CROSS JOIN"
	default:
		return "JOIN"
	}
}

func (j *JoinExpr) String() string {
	result := fmt.Sprintf("%s %s %s", j.Left.String(), j.JoinType.String(), j.Right.String())
	if j.Condition != nil {
		result += " ON " + j.Condition.String()
	}
	return result
}

func (j *JoinExpr) Accept(visitor Visitor) interface{} {
	return visitor.VisitExpression(j)
}

func (j *JoinExpr) tableRef() {}

// OrderItem represents an item in the ORDER BY clause
type OrderItem struct {
	Expression Expression
	Direction  OrderDirection
	NullsOrder NullsOrder
}

type OrderDirection int
type NullsOrder int

const (
	Ascending OrderDirection = iota
	Descending
)

const (
	NullsDefault NullsOrder = iota
	NullsFirst
	NullsLast
)

func (o OrderDirection) String() string {
	if o == Descending {
		return "DESC"
	}
	return "ASC"
}

func (n NullsOrder) String() string {
	switch n {
	case NullsFirst:
		return "NULLS FIRST"
	case NullsLast:
		return "NULLS LAST"
	default:
		return ""
	}
}

func (o *OrderItem) String() string {
	result := o.Expression.String() + " " + o.Direction.String()
	if o.NullsOrder != NullsDefault {
		result += " " + o.NullsOrder.String()
	}
	return result
}

// InsertStmt represents an INSERT statement
type InsertStmt struct {
	Table   TableName
	Columns []string
	Values  [][]Expression
	Select  *SelectStmt
}

func (i *InsertStmt) String() string {
	result := fmt.Sprintf("INSERT INTO %s", i.Table.String())
	
	if len(i.Columns) > 0 {
		result += fmt.Sprintf(" (%s)", strings.Join(i.Columns, ", "))
	}
	
	if i.Select != nil {
		result += " " + i.Select.String()
	} else if len(i.Values) > 0 {
		result += " VALUES "
		var valueStrs []string
		for _, row := range i.Values {
			var exprStrs []string
			for _, expr := range row {
				exprStrs = append(exprStrs, expr.String())
			}
			valueStrs = append(valueStrs, "("+strings.Join(exprStrs, ", ")+")")
		}
		result += strings.Join(valueStrs, ", ")
	}
	
	return result
}

func (i *InsertStmt) Accept(visitor Visitor) interface{} {
	return visitor.VisitInsertStmt(i)
}

// UpdateStmt represents an UPDATE statement
type UpdateStmt struct {
	Table TableName
	Set   []UpdateAssignment
	Where Expression
}

type UpdateAssignment struct {
	Column string
	Value  Expression
}

func (u *UpdateStmt) String() string {
	result := fmt.Sprintf("UPDATE %s SET ", u.Table.String())
	
	var assignments []string
	for _, assignment := range u.Set {
		assignments = append(assignments, fmt.Sprintf("%s = %s", assignment.Column, assignment.Value.String()))
	}
	result += strings.Join(assignments, ", ")
	
	if u.Where != nil {
		result += " WHERE " + u.Where.String()
	}
	
	return result
}

func (u *UpdateStmt) Accept(visitor Visitor) interface{} {
	return visitor.VisitUpdateStmt(u)
}

// DeleteStmt represents a DELETE statement
type DeleteStmt struct {
	Table TableName
	Where Expression
}

func (d *DeleteStmt) String() string {
	result := fmt.Sprintf("DELETE FROM %s", d.Table.String())
	
	if d.Where != nil {
		result += " WHERE " + d.Where.String()
	}
	
	return result
}

func (d *DeleteStmt) Accept(visitor Visitor) interface{} {
	return visitor.VisitDeleteStmt(d)
}

// CreateTableStmt represents a CREATE TABLE statement
type CreateTableStmt struct {
	Name     TableName
	Columns  []ColumnDef
	IfExists bool
}

type ColumnDef struct {
	Name       string
	Type       DataType
	NotNull    bool
	PrimaryKey bool
	DefaultVal Expression
}

func (c *CreateTableStmt) String() string {
	result := "CREATE TABLE "
	if c.IfExists {
		result += "IF NOT EXISTS "
	}
	result += c.Name.String() + " ("
	
	var columnStrs []string
	for _, col := range c.Columns {
		colStr := fmt.Sprintf("%s %s", col.Name, col.Type.String())
		if col.NotNull {
			colStr += " NOT NULL"
		}
		if col.PrimaryKey {
			colStr += " PRIMARY KEY"
		}
		if col.DefaultVal != nil {
			colStr += " DEFAULT " + col.DefaultVal.String()
		}
		columnStrs = append(columnStrs, colStr)
	}
	result += strings.Join(columnStrs, ", ")
	result += ")"
	
	return result
}

func (c *CreateTableStmt) Accept(visitor Visitor) interface{} {
	return visitor.VisitCreateTableStmt(c)
}

// DropTableStmt represents a DROP TABLE statement
type DropTableStmt struct {
	Name     TableName
	IfExists bool
}

func (d *DropTableStmt) String() string {
	result := "DROP TABLE "
	if d.IfExists {
		result += "IF EXISTS "
	}
	result += d.Name.String()
	return result
}

func (d *DropTableStmt) Accept(visitor Visitor) interface{} {
	return visitor.VisitDropTableStmt(d)
}

// Missing ASC/DESC constants for parser
const (
	ASC  = "ASC"
	DESC = "DESC"
)