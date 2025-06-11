package engine

import (
	"fmt"
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
	PlanCreateTable
	PlanDropTable
	PlanTransaction
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
		// Resolve table
		schema := stmt.From.Schema
		if schema == "" {
			schema = "main"
		}
		
		table, err := p.catalog.GetTable(schema, stmt.From.Table)
		if err != nil {
			return nil, fmt.Errorf("table not found: %w", err)
		}
		
		// Create scan plan
		basePlan = &ScanPlan{
			Table: table,
		}
	}
	
	var plan Plan = basePlan
	
	// Add filter if WHERE clause exists
	if stmt.Where != nil {
		plan = &FilterPlan{
			Child:     plan,
			Predicate: stmt.Where,
		}
	}
	
	// Add projection
	if len(stmt.Columns) > 0 {
		plan = &ProjectPlan{
			Child:       plan,
			Expressions: stmt.Columns,
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
// planDropTable creates a plan for DROP TABLE statement
func (p *Planner) planDropTable(stmt *DropTableStatement) (Plan, error) {
	return &DropTablePlan{
		Schema:   stmt.Schema,
		Table:    stmt.Table,
		IfExists: stmt.IfExists,
	}, nil
}
