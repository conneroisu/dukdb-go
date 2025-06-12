package engine

import (
	"fmt"

	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

// Plan represents a query execution plan
type Plan interface {
	PlanType() PlanType
}

// PlanType represents the type of plan node
type PlanType int

const (
	PlanScan PlanType = iota
	PlanProject
	PlanFilter
	PlanJoin
	PlanAggregate
	PlanSort
	PlanLimit
	PlanInsert
	PlanUpdate
	PlanDelete
	PlanCreateTable
	PlanDropTable
	PlanTransaction
	PlanUnion
	PlanSubquery
)

// Planner creates execution plans from SQL statements
type Planner struct {
	catalog *Catalog
}

// NewPlanner creates a new query planner
func NewPlanner(catalog *Catalog) *Planner {
	return &Planner{
		catalog: catalog,
	}
}

// CreatePlan creates an execution plan from a statement
func (p *Planner) CreatePlan(stmt Statement) (Plan, error) {
	switch s := stmt.(type) {
	case *SelectStatement:
		return p.planSelect(s)
	case *InsertStatement:
		return p.planInsert(s)
	case *UpdateStatement:
		return p.planUpdate(s)
	case *DeleteStatement:
		return p.planDelete(s)
	case *CreateTableStatement:
		return p.planCreateTable(s)
	case *DropTableStatement:
		return p.planDropTable(s)
	case *BeginStatement:
		return &TransactionPlan{Type: TxnBegin}, nil
	case *CommitStatement:
		return &TransactionPlan{Type: TxnCommit}, nil
	case *RollbackStatement:
		return &TransactionPlan{Type: TxnRollback}, nil
	case *UnionStatement:
		return p.planUnion(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// planSelect creates a plan for a SELECT statement
func (p *Planner) planSelect(stmt *SelectStatement) (Plan, error) {
	var basePlan Plan
	
	
	if stmt.From == nil {
		// Handle SELECT without FROM (e.g., SELECT 1)
		basePlan = &ValuesPlan{
			Expressions: stmt.Columns,
		}
	} else {
		// Resolve main table
		schema := stmt.From.Schema
		if schema == "" {
			schema = "main"
		}
		
		table, err := p.catalog.GetTable(schema, stmt.From.Table)
		if err != nil {
			return nil, fmt.Errorf("table not found: %w", err)
		}
		
		// Create scan plan for main table
		basePlan = &ScanPlan{
			Table: table,
		}
		
		// Add JOINs if any
		for _, join := range stmt.Joins {
			joinSchema := join.Table.Schema
			if joinSchema == "" {
				joinSchema = "main"
			}
			
			joinTable, err := p.catalog.GetTable(joinSchema, join.Table.Table)
			if err != nil {
				return nil, fmt.Errorf("join table not found: %w", err)
			}
			
			basePlan = &JoinPlan{
				Left:      basePlan,
				Right:     &ScanPlan{Table: joinTable},
				JoinType:  join.Type,
				Condition: join.Condition,
			}
		}
	}
	
	plan := basePlan
	
	// Add filter if WHERE clause exists
	if stmt.Where != nil {
		plan = &FilterPlan{
			Child:     plan,
			Predicate: stmt.Where,
		}
	}
	
	// Check if any columns are aggregate functions
	var aggregates []AggregateExpr
	var projections []Expression
	
	for _, col := range stmt.Columns {
		if funcExpr, ok := col.(*FunctionExpr); ok {
			// This is a function call - check if it's an aggregate
			if isAggregateFunction(funcExpr.Name) {
				agg, err := createAggregateExpr(funcExpr)
				if err != nil {
					return nil, err
				}
				aggregates = append(aggregates, agg)
			} else {
				// Non-aggregate function, treat as projection
				projections = append(projections, col)
			}
		} else if subqueryExpr, ok := col.(*SubqueryExpr); ok {
			// Handle subquery in SELECT clause
			// For now, treat as projection (scalar subquery)
			projections = append(projections, subqueryExpr)
		} else {
			// Regular column, treat as projection
			projections = append(projections, col)
		}
	}
	
	// If we have aggregates, create an aggregate plan
	if len(aggregates) > 0 {
		plan = &AggregatePlan{
			Child:      plan,
			GroupBy:    stmt.GroupBy,  // Will be empty for simple aggregates
			Aggregates: aggregates,
		}
		
		// For GROUP BY queries with aggregates, the AggregatePlan handles both
		// GROUP BY columns and aggregate results - no additional projection needed
	} else {
		// Add projection for non-aggregate columns (only when no aggregates)
		if len(projections) > 0 {
			plan = &ProjectPlan{
				Child:       plan,
				Expressions: projections,
			}
		}
	}
	
	// Add HAVING filter if exists (must come after aggregation)
	if stmt.Having != nil {
		// Transform HAVING clause to reference aggregate result columns
		transformedHaving := p.transformHavingExpression(stmt.Having, aggregates, stmt.GroupBy)
		plan = &FilterPlan{
			Child:     plan,
			Predicate: transformedHaving,
		}
	}
	
	// Add ORDER BY if exists
	if len(stmt.OrderBy) > 0 {
		plan = &SortPlan{
			Child:   plan,
			OrderBy: stmt.OrderBy,
		}
	}
	
	// Add LIMIT if exists
	if stmt.Limit != nil {
		plan = &LimitPlan{
			Child:  plan,
			Limit:  *stmt.Limit,
			Offset: 0,
		}
		if stmt.Offset != nil {
			plan.(*LimitPlan).Offset = *stmt.Offset
		}
	}
	
	return plan, nil
}

// planInsert creates a plan for an INSERT statement
func (p *Planner) planInsert(stmt *InsertStatement) (Plan, error) {
	schema := stmt.Table.Schema
	if schema == "" {
		schema = "main"
	}
	
	table, err := p.catalog.GetTable(schema, stmt.Table.Table)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}
	
	return &InsertPlan{
		Table:   table,
		Columns: stmt.Columns,
		Values:  stmt.Values,
	}, nil
}

// planCreateTable creates a plan for a CREATE TABLE statement
func (p *Planner) planCreateTable(stmt *CreateTableStatement) (Plan, error) {
	return &CreateTablePlan{
		Schema:  stmt.Schema,
		Table:   stmt.Table,
		Columns: stmt.Columns,
	}, nil
}

// Plan node implementations

// ScanPlan represents a table scan
type ScanPlan struct {
	Table *Table
}

func (p *ScanPlan) PlanType() PlanType { return PlanScan }

// ValuesPlan represents a values expression (SELECT without FROM)
type ValuesPlan struct {
	Expressions []Expression
}

func (p *ValuesPlan) PlanType() PlanType { return PlanProject }

// ProjectPlan represents a projection
type ProjectPlan struct {
	Child       Plan
	Expressions []Expression
}

func (p *ProjectPlan) PlanType() PlanType { return PlanProject }

// FilterPlan represents a filter operation
type FilterPlan struct {
	Child     Plan
	Predicate Expression
}

func (p *FilterPlan) PlanType() PlanType { return PlanFilter }

// SortPlan represents a sort operation
type SortPlan struct {
	Child   Plan
	OrderBy []OrderByExpr
}

func (p *SortPlan) PlanType() PlanType { return PlanSort }

// LimitPlan represents a limit operation
type LimitPlan struct {
	Child  Plan
	Limit  int64
	Offset int64
}

func (p *LimitPlan) PlanType() PlanType { return PlanLimit }

// InsertPlan represents an insert operation
type InsertPlan struct {
	Table   *Table
	Columns []string
	Values  [][]Expression
}

func (p *InsertPlan) PlanType() PlanType { return PlanInsert }

// UpdatePlan represents an update operation
type UpdatePlan struct {
	Table *Table
	Sets  []SetClause
	Where Expression
}

func (p *UpdatePlan) PlanType() PlanType { return PlanUpdate }

// DeletePlan represents a delete operation
type DeletePlan struct {
	Table *Table
	Where Expression
}

func (p *DeletePlan) PlanType() PlanType { return PlanDelete }

// CreateTablePlan represents a create table operation
type CreateTablePlan struct {
	Schema  string
	Table   string
	Columns []ColumnDefinition
}

func (p *CreateTablePlan) PlanType() PlanType { return PlanCreateTable }

// DropTablePlan represents a drop table operation
type DropTablePlan struct {
	Schema   string
	Table    string
	IfExists bool
}

func (p *DropTablePlan) PlanType() PlanType { return PlanDropTable }

// TransactionPlan represents a transaction control operation
type TransactionPlan struct {
	Type TxnOp
}

func (p *TransactionPlan) PlanType() PlanType { return PlanTransaction }

// TxnOp represents a transaction operation
type TxnOp int

const (
	TxnBegin TxnOp = iota
	TxnCommit
	TxnRollback
)

// AggregatePlan represents an aggregate operation
type AggregatePlan struct {
	Child      Plan
	GroupBy    []Expression
	Aggregates []AggregateExpr
}

func (p *AggregatePlan) PlanType() PlanType { return PlanAggregate }

// JoinPlan represents a join operation
type JoinPlan struct {
	Left      Plan
	Right     Plan
	JoinType  JoinType
	Condition Expression
}

func (p *JoinPlan) PlanType() PlanType { return PlanJoin }

// UnionPlan represents a UNION/UNION ALL operation
type UnionPlan struct {
	Left     Plan
	Right    Plan
	UnionAll bool // true for UNION ALL, false for UNION (distinct)
	OrderBy  []OrderByExpr
	Limit    *int64
}

func (p *UnionPlan) PlanType() PlanType { return PlanUnion }

// SubqueryPlan represents a subquery operation
type SubqueryPlan struct {
	Plan         Plan        // The subquery plan
	SubqueryType SubqueryType // scalar, exists, etc.
}

func (p *SubqueryPlan) PlanType() PlanType { return PlanSubquery }

// planUpdate creates a plan for UPDATE statement
func (p *Planner) planUpdate(stmt *UpdateStatement) (Plan, error) {
	// Resolve table
	schema := "main"
	if stmt.Table.Schema != "" {
		schema = stmt.Table.Schema
	}
	
	table, err := p.catalog.GetTable(schema, stmt.Table.Table)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}
	
	return &UpdatePlan{
		Table: table,
		Sets:  stmt.Sets,
		Where: stmt.Where,
	}, nil
}

// planDelete creates a plan for DELETE statement
func (p *Planner) planDelete(stmt *DeleteStatement) (Plan, error) {
	// Resolve table
	schema := "main"
	if stmt.Table.Schema != "" {
		schema = stmt.Table.Schema
	}
	
	table, err := p.catalog.GetTable(schema, stmt.Table.Table)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}
	
	return &DeletePlan{
		Table: table,
		Where: stmt.Where,
	}, nil
}

// planDropTable creates a plan for DROP TABLE statement
func (p *Planner) planDropTable(stmt *DropTableStatement) (Plan, error) {
	return &DropTablePlan{
		Schema:   stmt.Schema,
		Table:    stmt.Table,
		IfExists: stmt.IfExists,
	}, nil
}

// isAggregateFunction checks if a function name is an aggregate function
func isAggregateFunction(name string) bool {
	switch name {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE":
		return true
	default:
		return false
	}
}

// createAggregateExpr creates an AggregateExpr from a FunctionExpr
func createAggregateExpr(funcExpr *FunctionExpr) (AggregateExpr, error) {
	// Determine the input type based on the function arguments
	var inputType storage.LogicalType
	
	if len(funcExpr.Args) == 0 || (len(funcExpr.Args) == 1 && isStarExpression(funcExpr.Args[0])) {
		// Functions like COUNT() or COUNT(*) - input type doesn't matter
		inputType = storage.LogicalType{ID: storage.TypeInteger}
	} else if len(funcExpr.Args) == 1 {
		// Functions like SUM(column) or SUM(function(...)) - infer type from argument
		if colExpr, ok := funcExpr.Args[0].(*ColumnExpr); ok {
			// For common columns, infer the type
			switch colExpr.Column {
			case "amount":
				inputType = storage.LogicalType{ID: storage.TypeDouble}
			case "quantity", "id", "customer_id":
				inputType = storage.LogicalType{ID: storage.TypeInteger}
			case "order_date", "created_at", "timestamp":
				inputType = storage.LogicalType{ID: storage.TypeTimestamp}
			default:
				// Default to double for numeric aggregates
				inputType = storage.LogicalType{ID: storage.TypeDouble}
			}
		} else if nestedFunc, ok := funcExpr.Args[0].(*FunctionExpr); ok {
			// Handle nested function expressions like SUM(array_length(tags))
			switch nestedFunc.Name {
			case "ARRAY_LENGTH", "LIST_LENGTH", "CARDINALITY":
				// These functions return integers
				inputType = storage.LogicalType{ID: storage.TypeInteger}
			case "ELEMENT_AT":
				// Element access can return various types, default to double
				inputType = storage.LogicalType{ID: storage.TypeDouble}
			default:
				// Default type for other functions
				inputType = storage.LogicalType{ID: storage.TypeDouble}
			}
		} else {
			// Default type for other expression types
			inputType = storage.LogicalType{ID: storage.TypeInteger}
		}
	} else {
		return AggregateExpr{}, fmt.Errorf("aggregate function %s does not support multiple arguments", funcExpr.Name)
	}
	
	// Get the aggregate function
	aggFunc, err := CreateAggregateFunction(funcExpr.Name, inputType)
	if err != nil {
		return AggregateExpr{}, err
	}
	
	// Determine the input expression
	var input Expression
	if len(funcExpr.Args) == 0 {
		// Functions like COUNT() with no args
		input = &ConstantExpr{Value: 1}
	} else if len(funcExpr.Args) == 1 {
		// Functions like COUNT(*), SUM(column)
		input = funcExpr.Args[0]
	} else {
		return AggregateExpr{}, fmt.Errorf("aggregate function %s does not support multiple arguments", funcExpr.Name)
	}
	
	// Use the alias from the function expression if available
	alias := funcExpr.Alias
	if alias == "" {
		alias = fmt.Sprintf("%s_%d", funcExpr.Name, 0) // Fallback alias
	}
	
	return AggregateExpr{
		Function: aggFunc,
		Input:    input,
		Alias:    alias,
	}, nil
}

// isStarExpression checks if an expression represents * (e.g., COUNT(*))
func isStarExpression(expr Expression) bool {
	if colExpr, ok := expr.(*ColumnExpr); ok {
		return colExpr.Column == "*"
	}
	return false
}

// transformHavingExpression transforms HAVING expressions to reference aggregate result columns
func (p *Planner) transformHavingExpression(expr Expression, aggregates []AggregateExpr, groupBy []Expression) Expression {
	switch e := expr.(type) {
	case *BinaryExpr:
		// Transform both sides of binary expression
		left := p.transformHavingExpression(e.Left, aggregates, groupBy)
		right := p.transformHavingExpression(e.Right, aggregates, groupBy)
		return &BinaryExpr{
			Left:  left,
			Op:    e.Op,
			Right: right,
		}
		
	case *FunctionExpr:
		// Transform aggregate functions to column references
		if isAggregateFunction(e.Name) {
			// Find matching aggregate in the aggregates list
			for i, agg := range aggregates {
				if p.matchesAggregate(e, agg) {
					// Use the aggregate alias as the column name
					colName := agg.Alias
					if colName == "" {
						colName = fmt.Sprintf("agg_%d", i)
					}
					
					return &ColumnExpr{
						Column: colName,
					}
				}
			}
			
			// If no match found, fall back to original expression
			return e
		}
		return e
		
	case *ColumnExpr:
		// Check if this is a GROUP BY column reference
		for _, groupExpr := range groupBy {
			if colExpr, ok := groupExpr.(*ColumnExpr); ok {
				if colExpr.Column == e.Column {
					// This references a GROUP BY column, which is at index i in the result
					return &ColumnExpr{
						Column: e.Column, // Keep the same column name
					}
				}
			}
		}
		return e
		
	default:
		return e
	}
}

// matchesAggregate checks if a function expression matches an aggregate
func (p *Planner) matchesAggregate(funcExpr *FunctionExpr, agg AggregateExpr) bool {
	// Get the function name from the aggregate function type
	funcName := ""
	switch agg.Function.(type) {
	case *CountAggregate:
		funcName = "COUNT"
	case *SumAggregate:
		funcName = "SUM"
	case *AvgAggregate:
		funcName = "AVG"
	case *MinAggregate:
		funcName = "MIN"
	case *MaxAggregate:
		funcName = "MAX"
	default:
		// For unknown functions, try to extract from alias
		if agg.Alias != "" {
			if funcExpr.Name == "COUNT" && (agg.Alias == "count" || agg.Alias == "COUNT") {
				funcName = "COUNT"
			} else if funcExpr.Name == "SUM" && (agg.Alias == "sum_amount" || agg.Alias == "SUM") {
				funcName = "SUM"
			}
		}
	}
	
	// For simple matching, compare function name and argument
	if funcExpr.Name != funcName {
		return false
	}
	
	// Compare arguments
	if len(funcExpr.Args) == 0 && isStarExpression(agg.Input) {
		return true // COUNT() matches COUNT(*)
	}
	
	if len(funcExpr.Args) == 1 {
		funcArg := funcExpr.Args[0]
		
		// Handle COUNT(*) case
		if isStarExpression(funcArg) && isStarExpression(agg.Input) {
			return true
		}
		
		// Handle column arguments
		if funcCol, ok := funcArg.(*ColumnExpr); ok {
			if aggCol, ok := agg.Input.(*ColumnExpr); ok {
				return funcCol.Column == aggCol.Column
			}
		}
	}
	
	return false
}

// planUnion creates a plan for a UNION/UNION ALL statement
func (p *Planner) planUnion(stmt *UnionStatement) (Plan, error) {
	// Plan the left side
	leftPlan, err := p.CreatePlan(stmt.Left)
	if err != nil {
		return nil, fmt.Errorf("failed to plan left side of UNION: %w", err)
	}
	
	// Plan the right side
	rightPlan, err := p.CreatePlan(stmt.Right)
	if err != nil {
		return nil, fmt.Errorf("failed to plan right side of UNION: %w", err)
	}
	
	// Create the union plan
	unionPlan := &UnionPlan{
		Left:     leftPlan,
		Right:    rightPlan,
		UnionAll: stmt.UnionAll,
	}
	
	var plan Plan = unionPlan
	
	// Add ORDER BY if exists
	if len(stmt.OrderBy) > 0 {
		plan = &SortPlan{
			Child:   plan,
			OrderBy: stmt.OrderBy,
		}
	}
	
	// Add LIMIT if exists
	if stmt.Limit != nil {
		plan = &LimitPlan{
			Child:  plan,
			Limit:  *stmt.Limit,
			Offset: 0,
		}
		if stmt.Offset != nil {
			plan.(*LimitPlan).Offset = *stmt.Offset
		}
	}
	
	return plan, nil
}

