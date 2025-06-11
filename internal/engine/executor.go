package engine

import (
	"context"
	"fmt"

	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

// Executor executes query plans
type Executor struct {
	connection *Connection
}

// NewExecutor creates a new query executor
func NewExecutor(conn *Connection) *Executor {
	return &Executor{
		connection: conn,
	}
}

// Execute executes a plan and returns the result
func (e *Executor) Execute(ctx context.Context, plan Plan) (*QueryResult, error) {
	switch p := plan.(type) {
	case *ScanPlan:
		return e.executeScan(ctx, p)
	case *ValuesPlan:
		return e.executeValues(ctx, p)
	case *ProjectPlan:
		return e.executeProject(ctx, p)
	case *FilterPlan:
		return e.executeFilter(ctx, p)
	case *SortPlan:
		return e.executeSort(ctx, p)
	case *LimitPlan:
		return e.executeLimit(ctx, p)
	case *InsertPlan:
		return e.executeInsert(ctx, p)
	case *CreateTablePlan:
		return e.executeCreateTable(ctx, p)
	case *DropTablePlan:
		return e.executeDropTable(ctx, p)
	case *TransactionPlan:
		return e.executeTransaction(ctx, p)
	case *AggregatePlan:
		return e.executeAggregate(ctx, p)
	default:
		return nil, fmt.Errorf("unsupported plan type: %T", plan)
	}
}

// executeScan executes a table scan
func (e *Executor) executeScan(ctx context.Context, plan *ScanPlan) (*QueryResult, error) {
	// Get data chunks from table
	chunks, err := plan.Table.Scan()
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}
	
	// Create column metadata
	columnDefs := plan.Table.GetColumns()
	columns := make([]Column, len(columnDefs))
	for i, def := range columnDefs {
		columns[i] = Column{
			Name:      def.Name,
			Type:      def.Type,
			Nullable:  def.Nullable,
			Precision: def.Type.Width,
			Scale:     def.Type.Scale,
		}
	}
	
	return &QueryResult{
		chunks:  chunks,
		columns: columns,
		current: -1, // Start before first chunk
	}, nil
}

// executeValues executes a values expression (SELECT without FROM)
func (e *Executor) executeValues(ctx context.Context, plan *ValuesPlan) (*QueryResult, error) {
	// Create a single-row chunk with the expressions
	schema := make([]storage.LogicalType, len(plan.Expressions))
	for i, expr := range plan.Expressions {
		schema[i] = inferExpressionType(expr, nil)
	}
	
	chunk := storage.NewDataChunk(schema, 1)
	
	// Evaluate expressions
	for i, expr := range plan.Expressions {
		val, err := evaluateExpression(expr, nil, 0)
		if err != nil {
			return nil, err
		}
		chunk.SetValue(i, 0, val)
	}
	chunk.SetSize(1)
	
	// Create column metadata
	columns := make([]Column, len(plan.Expressions))
	for i := range plan.Expressions {
		columns[i] = Column{
			Name: fmt.Sprintf("col_%d", i),
			Type: schema[i],
		}
	}
	
	return &QueryResult{
		chunks:  []*storage.DataChunk{chunk},
		columns: columns,
		current: -1,
	}, nil
}

// executeProject executes a projection
func (e *Executor) executeProject(ctx context.Context, plan *ProjectPlan) (*QueryResult, error) {
	// Execute child plan
	childResult, err := e.Execute(ctx, plan.Child)
	if err != nil {
		return nil, err
	}
	defer childResult.Close()
	
	// If projecting all columns (*), return child result as-is
	if len(plan.Expressions) == 1 {
		if col, ok := plan.Expressions[0].(*ColumnExpr); ok && col.Column == "*" {
			// Transfer ownership of chunks
			result := &QueryResult{
				chunks:  childResult.chunks,
				columns: childResult.columns,
				current: childResult.current,
			}
			childResult.chunks = nil // Prevent cleanup
			return result, nil
		}
	}
	
	// Create projection operator
	inputSchema := make([]storage.LogicalType, len(childResult.columns))
	for i, col := range childResult.columns {
		inputSchema[i] = col.Type
	}
	
	projOp := NewProjectOperator(plan.Expressions, inputSchema)
	
	// Process each chunk
	var outputChunks []*storage.DataChunk
	childResult.current = -1 // Reset to start
	
	for childResult.Next() {
		inputChunk := childResult.GetChunk()
		outputChunk, err := projOp.Execute(ctx, inputChunk)
		if err != nil {
			return nil, err
		}
		outputChunks = append(outputChunks, outputChunk)
	}
	
	// Create output columns metadata
	outputSchema := projOp.GetOutputSchema()
	columns := make([]Column, len(outputSchema))
	for i, typ := range outputSchema {
		columns[i] = Column{
			Name: fmt.Sprintf("col_%d", i), // Would need proper column names
			Type: typ,
		}
	}
	
	return &QueryResult{
		chunks:  outputChunks,
		columns: columns,
		current: -1,
	}, nil
}

// executeFilter executes a filter operation
func (e *Executor) executeFilter(ctx context.Context, plan *FilterPlan) (*QueryResult, error) {
	// Execute child plan
	childResult, err := e.Execute(ctx, plan.Child)
	if err != nil {
		return nil, err
	}
	defer childResult.Close()
	
	// Create filter operator with column context
	inputSchema := make([]storage.LogicalType, len(childResult.columns))
	columnNames := make([]string, len(childResult.columns))
	for i, col := range childResult.columns {
		inputSchema[i] = col.Type
		columnNames[i] = col.Name
	}
	
	filterOp := NewFilterOperatorWithContext(plan.Predicate, inputSchema, columnNames)
	
	// Process each chunk
	var outputChunks []*storage.DataChunk
	childResult.current = -1 // Reset to start
	
	for childResult.Next() {
		inputChunk := childResult.GetChunk()
		outputChunk, err := filterOp.Execute(ctx, inputChunk)
		if err != nil {
			return nil, err
		}
		if outputChunk.Size() > 0 {
			outputChunks = append(outputChunks, outputChunk)
		}
	}
	
	return &QueryResult{
		chunks:  outputChunks,
		columns: childResult.columns,
		current: -1,
	}, nil
}

// executeSort executes a sort operation
func (e *Executor) executeSort(ctx context.Context, plan *SortPlan) (*QueryResult, error) {
	// Execute child plan
	childResult, err := e.Execute(ctx, plan.Child)
	if err != nil {
		return nil, err
	}
	defer childResult.Close()
	
	// For sort, we need to materialize all data first
	// In a real implementation, we'd use external sort for large datasets
	
	// Create schema
	inputSchema := make([]storage.LogicalType, len(childResult.columns))
	for i, col := range childResult.columns {
		inputSchema[i] = col.Type
	}
	
	// Collect all chunks
	var allChunks []*storage.DataChunk
	childResult.current = -1
	for childResult.Next() {
		allChunks = append(allChunks, childResult.GetChunk())
	}
	
	if len(allChunks) == 0 {
		return &QueryResult{
			chunks:  []*storage.DataChunk{},
			columns: childResult.columns,
			current: -1,
		}, nil
	}
	
	// For simplicity, merge all chunks into one for sorting
	// In production, we'd use a more sophisticated approach
	totalSize := 0
	for _, chunk := range allChunks {
		totalSize += chunk.Size()
	}
	
	mergedChunk := storage.NewDataChunk(inputSchema, totalSize)
	destRow := 0
	
	for _, chunk := range allChunks {
		for row := 0; row < chunk.Size(); row++ {
			for col := 0; col < chunk.ColumnCount(); col++ {
				val, _ := chunk.GetValue(col, row)
				mergedChunk.SetValue(col, destRow, val)
			}
			destRow++
		}
	}
	mergedChunk.SetSize(totalSize)
	
	// Sort the merged chunk
	sortOp := NewSortOperator(plan.OrderBy, inputSchema)
	sortedChunk, err := sortOp.Execute(ctx, mergedChunk)
	if err != nil {
		return nil, err
	}
	
	return &QueryResult{
		chunks:  []*storage.DataChunk{sortedChunk},
		columns: childResult.columns,
		current: -1,
	}, nil
}

// executeLimit executes a limit operation
func (e *Executor) executeLimit(ctx context.Context, plan *LimitPlan) (*QueryResult, error) {
	// Execute child plan
	childResult, err := e.Execute(ctx, plan.Child)
	if err != nil {
		return nil, err
	}
	defer childResult.Close()
	
	// Create schema
	inputSchema := make([]storage.LogicalType, len(childResult.columns))
	for i, col := range childResult.columns {
		inputSchema[i] = col.Type
	}
	
	// Create limit operator
	limitOp := NewLimitOperator(plan.Limit, plan.Offset, inputSchema)
	
	// Process chunks until we reach the limit
	var outputChunks []*storage.DataChunk
	childResult.current = -1
	
	for childResult.Next() {
		inputChunk := childResult.GetChunk()
		outputChunk, err := limitOp.Execute(ctx, inputChunk)
		if err != nil {
			return nil, err
		}
		
		if outputChunk.Size() > 0 {
			outputChunks = append(outputChunks, outputChunk)
		}
		
		// Check if we've reached the limit
		if limitOp.consumed >= limitOp.offset+limitOp.limit {
			break
		}
	}
	
	return &QueryResult{
		chunks:  outputChunks,
		columns: childResult.columns,
		current: -1,
	}, nil
}

// executeInsert executes an insert operation
func (e *Executor) executeInsert(ctx context.Context, plan *InsertPlan) (*QueryResult, error) {
	// Convert expressions to values
	// For now, we'll handle only constant expressions
	rows := make([][]interface{}, len(plan.Values))
	for i, valueRow := range plan.Values {
		row := make([]interface{}, len(valueRow))
		for j, expr := range valueRow {
			if constExpr, ok := expr.(*ConstantExpr); ok {
				row[j] = constExpr.Value
			} else {
				return nil, fmt.Errorf("only constant values supported in INSERT")
			}
		}
		rows[i] = row
	}
	
	// Insert into table
	err := plan.Table.Insert(rows)
	if err != nil {
		return nil, fmt.Errorf("insert failed: %w", err)
	}
	
	// Return empty result with row count
	return &QueryResult{
		chunks:  []*storage.DataChunk{},
		columns: []Column{},
	}, nil
}

// executeCreateTable executes a create table operation
func (e *Executor) executeCreateTable(ctx context.Context, plan *CreateTablePlan) (*QueryResult, error) {
	// Create table in catalog
	err := e.connection.database.catalog.CreateTable(plan.Schema, plan.Table, plan.Columns)
	if err != nil {
		return nil, fmt.Errorf("create table failed: %w", err)
	}
	
	// Return empty result
	return &QueryResult{
		chunks:  []*storage.DataChunk{},
		columns: []Column{},
	}, nil
}

// executeTransaction executes a transaction control operation
func (e *Executor) executeTransaction(ctx context.Context, plan *TransactionPlan) (*QueryResult, error) {
	var err error
	
	switch plan.Type {
	case TxnBegin:
		err = e.connection.beginTransactionInternal()
	case TxnCommit:
		err = e.connection.commitInternal()
	case TxnRollback:
		err = e.connection.rollbackInternal()
	default:
		err = fmt.Errorf("unknown transaction operation: %v", plan.Type)
	}
	
	if err != nil {
		return nil, err
	}
	
	// Return empty result
	return &QueryResult{
		chunks:  []*storage.DataChunk{},
		columns: []Column{},
	}, nil
}

// executeDropTable executes a drop table operation
func (e *Executor) executeDropTable(ctx context.Context, plan *DropTablePlan) (*QueryResult, error) {
	// Drop table from catalog
	err := e.connection.database.catalog.DropTable(plan.Schema, plan.Table, plan.IfExists)
	if err != nil {
		return nil, fmt.Errorf("drop table failed: %w", err)
	}
	
	// Return empty result
	return &QueryResult{
		chunks:  []*storage.DataChunk{},
		columns: []Column{},
	}, nil
}