package engine

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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
	case *UpdatePlan:
		return e.executeUpdate(ctx, p)
	case *DeletePlan:
		return e.executeDelete(ctx, p)
	case *CreateTablePlan:
		return e.executeCreateTable(ctx, p)
	case *DropTablePlan:
		return e.executeDropTable(ctx, p)
	case *TransactionPlan:
		return e.executeTransaction(ctx, p)
	case *AggregatePlan:
		return e.executeAggregate(ctx, p)
	case *JoinPlan:
		return e.executeJoin(ctx, p)
	case *UnionPlan:
		return e.executeUnion(ctx, p)
	case *SubqueryPlan:
		return e.executeSubquery(ctx, p)
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

	// fmt.Printf("DEBUG Scan: table=%s, chunks=%d\n", plan.Table.name, len(chunks))
	// for i, chunk := range chunks {
	//	fmt.Printf("DEBUG Scan chunk %d: size=%d\n", i, chunk.Size())
	// }

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
		if err := chunk.SetValue(i, 0, val); err != nil {
			return nil, fmt.Errorf("failed to set chunk value: %w", err)
		}
	}
	if err := chunk.SetSize(1); err != nil {
		return nil, fmt.Errorf("failed to set chunk size: %w", err)
	}

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
	defer func() { _ = childResult.Close() }() // Closing errors not critical in defer

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
	columnNames := make([]string, len(childResult.columns))
	for i, col := range childResult.columns {
		inputSchema[i] = col.Type
		columnNames[i] = col.Name
	}

	projOp := NewProjectOperatorWithContext(plan.Expressions, inputSchema, columnNames)

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

	// Create output columns metadata with proper names
	outputSchema := projOp.GetOutputSchema()
	columns := make([]Column, len(outputSchema))
	for i, typ := range outputSchema {
		// Try to get proper column name from expression
		var colName string
		if i < len(plan.Expressions) {
			if col, ok := plan.Expressions[i].(*ColumnExpr); ok {
				colName = col.Column
			} else if funcExpr, ok := plan.Expressions[i].(*FunctionExpr); ok {
				if funcExpr.Alias != "" {
					colName = funcExpr.Alias
				} else {
					// Create column name based on function and arguments
					if len(funcExpr.Args) > 0 {
						if argCol, ok := funcExpr.Args[0].(*ColumnExpr); ok {
							colName = fmt.Sprintf("%s(%s)", strings.ToLower(funcExpr.Name), argCol.Column)
						} else {
							colName = strings.ToLower(funcExpr.Name)
						}
					} else {
						colName = strings.ToLower(funcExpr.Name)
					}
				}
			} else if _, ok := plan.Expressions[i].(*BinaryExpr); ok {
				// For computed expressions like oi.quantity * oi.unit_price
				colName = fmt.Sprintf("expr_%d", i)
			} else {
				colName = fmt.Sprintf("col_%d", i)
			}
		} else {
			colName = fmt.Sprintf("col_%d", i)
		}

		columns[i] = Column{
			Name: colName,
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
	defer func() { _ = childResult.Close() }() // Closing errors not critical in defer

	// Create filter operator with column context
	inputSchema := make([]storage.LogicalType, len(childResult.columns))
	columnNames := make([]string, len(childResult.columns))
	for i, col := range childResult.columns {
		inputSchema[i] = col.Type
		columnNames[i] = col.Name
	}

	// fmt.Printf("DEBUG Filter Plan: predicate=%T, inputSchema=%v, columnNames=%v\n", plan.Predicate, inputSchema, columnNames)
	filterOp := NewFilterOperatorWithContext(plan.Predicate, inputSchema, columnNames)

	// Process each chunk
	var outputChunks []*storage.DataChunk
	childResult.current = -1 // Reset to start

	// fmt.Printf("DEBUG Filter: About to process child chunks\n")

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
	defer func() { _ = childResult.Close() }() // Closing errors not critical in defer

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
				if err := mergedChunk.SetValue(col, destRow, val); err != nil {
					return nil, fmt.Errorf("failed to set merged chunk value: %w", err)
				}
			}
			destRow++
		}
	}
	if err := mergedChunk.SetSize(totalSize); err != nil {
		return nil, fmt.Errorf("failed to set merged chunk size: %w", err)
	}

	// Sort the merged chunk with column names for alias resolution
	columnNames := make([]string, len(childResult.columns))
	for i, col := range childResult.columns {
		columnNames[i] = col.Name
	}

	sortOp := NewSortOperatorWithContext(plan.OrderBy, inputSchema, columnNames)
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
	defer func() { _ = childResult.Close() }() // Closing errors not critical in defer

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
			switch e := expr.(type) {
			case *ConstantExpr:
				// Recursively evaluate any function calls in the constant value
				evaluatedValue, err := evaluateConstantValue(e.Value)
				if err != nil {
					return nil, fmt.Errorf("failed to evaluate constant value in INSERT: %w", err)
				}
				row[j] = evaluatedValue
			case *FunctionExpr:
				// Evaluate function expressions (like MAP(), LIST())
				value, err := evaluateExpressionWithContext(e, nil, 0, &EvaluationContext{})
				if err != nil {
					return nil, fmt.Errorf("failed to evaluate function in INSERT: %w", err)
				}
				row[j] = value
			default:
				return nil, fmt.Errorf("only constant values and functions supported in INSERT, got %T", expr)
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

// executeUpdate executes an update operation
func (e *Executor) executeUpdate(ctx context.Context, plan *UpdatePlan) (*QueryResult, error) {
	// For now, implement a simple update by scanning all rows,
	// evaluating WHERE clause, and updating matching rows

	// Get all data chunks
	chunks, err := plan.Table.Scan()
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	// Get column definitions
	columnDefs := plan.Table.GetColumns()
	columnMap := make(map[string]int)
	for i, col := range columnDefs {
		columnMap[col.Name] = i
	}

	// Count updated rows
	updatedRows := 0

	// Process each chunk
	for _, chunk := range chunks {
		for row := 0; row < chunk.Size(); row++ {
			// Evaluate WHERE clause if present
			shouldUpdate := true
			if plan.Where != nil {
				match, err := evaluateWhereClause(plan.Where, chunk, row, columnDefs)
				if err != nil {
					return nil, fmt.Errorf("WHERE clause error at row %d: %w", row, err)
				}
				shouldUpdate = match
			}

			if shouldUpdate {
				// Apply SET clauses
				for _, set := range plan.Sets {
					colIdx, exists := columnMap[set.Column]
					if !exists {
						return nil, fmt.Errorf("column %s not found", set.Column)
					}

					// Evaluate the value expression
					value, err := evaluateExpression(set.Value, chunk, row)
					if err != nil {
						return nil, err
					}

					// Update the value
					if err := chunk.SetValue(colIdx, row, value); err != nil {
						return nil, err
					}
				}

				updatedRows++
			}
		}
	}

	// Return result with row count
	return &QueryResult{
		chunks:       []*storage.DataChunk{},
		columns:      []Column{},
		rowsAffected: int64(updatedRows),
	}, nil
}

// executeDelete executes a delete operation
func (e *Executor) executeDelete(ctx context.Context, plan *DeletePlan) (*QueryResult, error) {
	// For now, implement a simple delete by scanning all rows,
	// evaluating WHERE clause, and removing matching rows

	// Get all data chunks
	chunks, err := plan.Table.Scan()
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	// Get column definitions
	columnDefs := plan.Table.GetColumns()

	// Create new chunks without deleted rows
	newChunks := make([]*storage.DataChunk, 0)
	deletedRows := 0

	// Get logical types from column definitions
	types := make([]storage.LogicalType, len(columnDefs))
	for i, col := range columnDefs {
		types[i] = col.Type
	}

	// Process each chunk
	for _, chunk := range chunks {
		// Create a new chunk for non-deleted rows
		newChunk := storage.NewDataChunk(types, chunk.Capacity())
		newRowIdx := 0

		for row := 0; row < chunk.Size(); row++ {
			// Evaluate WHERE clause if present
			shouldDelete := true
			if plan.Where != nil {
				match, err := evaluateWhereClause(plan.Where, chunk, row, columnDefs)
				if err != nil {
					return nil, err
				}
				shouldDelete = match
			}

			if shouldDelete {
				deletedRows++
			} else {
				// Copy row to new chunk
				for col := 0; col < chunk.ColumnCount(); col++ {
					val, err := chunk.GetValue(col, row)
					if err != nil {
						return nil, err
					}
					if err := newChunk.SetValue(col, newRowIdx, val); err != nil {
						return nil, err
					}
				}
				newRowIdx++
			}
		}

		if newRowIdx > 0 {
			_ = newChunk.SetSize(newRowIdx) // Size setting errors are not critical here
			newChunks = append(newChunks, newChunk)
		}
	}

	// Update table data with new chunks
	if err := plan.Table.ReplaceChunks(newChunks); err != nil {
		return nil, fmt.Errorf("failed to update table: %w", err)
	}

	// Return result with row count
	return &QueryResult{
		chunks:       []*storage.DataChunk{},
		columns:      []Column{},
		rowsAffected: int64(deletedRows),
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

// evaluateWhereClause evaluates a WHERE clause expression for a specific row
func evaluateWhereClause(where Expression, chunk *storage.DataChunk, row int, columnDefs []ColumnDefinition) (bool, error) {
	// Create column name mapping for evaluation context
	columnNames := make([]string, len(columnDefs))
	for i, col := range columnDefs {
		columnNames[i] = col.Name
	}

	// Create evaluation context
	ctx := &EvaluationContext{
		columnNames: columnNames,
	}

	// Evaluate the expression
	result, err := evaluateExpressionWithContext(where, chunk, row, ctx)
	if err != nil {
		return false, err
	}

	// Convert result to boolean
	if result == nil {
		return false, nil // NULL is treated as false
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case int32:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to boolean for WHERE clause", result)
	}
}

// executeJoin executes a JOIN operation
func (e *Executor) executeJoin(ctx context.Context, plan *JoinPlan) (*QueryResult, error) {
	// Execute left and right child plans
	leftResult, err := e.Execute(ctx, plan.Left)
	if err != nil {
		return nil, fmt.Errorf("join left child failed: %w", err)
	}

	rightResult, err := e.Execute(ctx, plan.Right)
	if err != nil {
		return nil, fmt.Errorf("join right child failed: %w", err)
	}

	// Build output schema (left columns + right columns)
	leftColumns := leftResult.columns
	rightColumns := rightResult.columns
	outputColumns := make([]Column, 0, len(leftColumns)+len(rightColumns))
	outputColumns = append(outputColumns, leftColumns...)
	outputColumns = append(outputColumns, rightColumns...)

	// Check if this is an equi-join that can use hash join
	if plan.Condition != nil && (plan.JoinType == InnerJoin || plan.JoinType == LeftJoin) {
		if equiJoinKeys := e.extractEquiJoinKeys(plan.Condition, leftColumns, rightColumns); equiJoinKeys != nil {
			return e.executeHashJoin(ctx, plan, leftResult, rightResult, equiJoinKeys, outputColumns)
		}
	}

	// Fall back to nested loop join for non-equi joins or other join types
	return e.executeNestedLoopJoin(ctx, plan, leftResult, rightResult, outputColumns)
}

// evaluateJoinCondition evaluates a join condition between two rows
func (e *Executor) evaluateJoinCondition(condition Expression, leftChunk *storage.DataChunk, leftRow int, rightChunk *storage.DataChunk, rightRow int, leftColumns, rightColumns []Column) (bool, error) {
	if condition == nil {
		return true, nil // No condition = cross join
	}

	// Create a combined evaluation context
	combinedColumns := make([]string, 0, len(leftColumns)+len(rightColumns))
	for _, col := range leftColumns {
		combinedColumns = append(combinedColumns, col.Name)
	}
	for _, col := range rightColumns {
		combinedColumns = append(combinedColumns, col.Name)
	}

	// Create a combined chunk for evaluation
	combinedSchema := make([]storage.LogicalType, len(combinedColumns))
	for i, col := range leftColumns {
		combinedSchema[i] = col.Type
	}
	for i, col := range rightColumns {
		combinedSchema[len(leftColumns)+i] = col.Type
	}

	combinedChunk := storage.NewDataChunk(combinedSchema, 1)

	// Copy left values
	for i := 0; i < leftChunk.ColumnCount(); i++ {
		val, err := leftChunk.GetValue(i, leftRow)
		if err != nil {
			return false, err
		}
		if err := combinedChunk.SetValue(i, 0, val); err != nil {
			return false, err
		}
	}

	// Copy right values
	for i := 0; i < rightChunk.ColumnCount(); i++ {
		val, err := rightChunk.GetValue(i, rightRow)
		if err != nil {
			return false, err
		}
		if err := combinedChunk.SetValue(leftChunk.ColumnCount()+i, 0, val); err != nil {
			return false, err
		}
	}

	_ = combinedChunk.SetSize(1) // Size setting errors are not critical here

	// Evaluate condition with combined context
	ctx := &EvaluationContext{
		columnNames: combinedColumns,
	}

	result, err := evaluateExpressionWithContext(condition, combinedChunk, 0, ctx)
	if err != nil {
		return false, err
	}

	// Convert to boolean
	if result == nil {
		return false, nil
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case int32:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to boolean for join condition", result)
	}
}

// collectAllChunks collects all data chunks from a query result
func collectAllChunks(result *QueryResult) ([]*storage.DataChunk, error) {
	var chunks []*storage.DataChunk

	for result.Next() {
		chunk := result.GetChunk()
		if chunk != nil {
			chunks = append(chunks, chunk)
		}
	}

	return chunks, nil
}

// equiJoinKey represents a pair of columns for equi-join
type equiJoinKey struct {
	leftCol  int
	rightCol int
}

// extractEquiJoinKeys extracts equi-join keys from a join condition
func (e *Executor) extractEquiJoinKeys(condition Expression, leftColumns, rightColumns []Column) []equiJoinKey {
	switch expr := condition.(type) {
	case *BinaryExpr:
		if expr.Op == OpEq {
			// Check if it's column = column
			leftColExpr, leftOk := expr.Left.(*ColumnExpr)
			rightColExpr, rightOk := expr.Right.(*ColumnExpr)

			if leftOk && rightOk {
				// Helper to extract column name without table prefix
				extractColumnName := func(fullName string) string {
					// Handle table.column format
					if idx := len(fullName) - 1; idx >= 0 {
						for i := idx; i >= 0; i-- {
							if fullName[i] == '.' {
								return fullName[i+1:]
							}
						}
					}
					return fullName
				}

				leftColName := extractColumnName(leftColExpr.Column)
				rightColName := extractColumnName(rightColExpr.Column)

				// Find column indices
				leftIdx := -1
				rightIdx := -1

				// First try: left expr maps to left columns, right expr maps to right columns
				for i, col := range leftColumns {
					if col.Name == leftColName || col.Name == leftColExpr.Column {
						leftIdx = i
						break
					}
				}

				for i, col := range rightColumns {
					if col.Name == rightColName || col.Name == rightColExpr.Column {
						rightIdx = i
						break
					}
				}

				// Check if we found both columns in the correct tables
				if leftIdx >= 0 && rightIdx >= 0 {
					return []equiJoinKey{{leftCol: leftIdx, rightCol: rightIdx}}
				}

				// Try the other way around
				leftIdx = -1
				rightIdx = -1

				for i, col := range leftColumns {
					if col.Name == rightColName || col.Name == rightColExpr.Column {
						leftIdx = i
						break
					}
				}

				for i, col := range rightColumns {
					if col.Name == leftColName || col.Name == leftColExpr.Column {
						rightIdx = i
						break
					}
				}

				if leftIdx >= 0 && rightIdx >= 0 {
					return []equiJoinKey{{leftCol: leftIdx, rightCol: rightIdx}}
				}
			}
		} else if expr.Op == OpAnd {
			// For AND, extract keys from both sides
			leftKeys := e.extractEquiJoinKeys(expr.Left, leftColumns, rightColumns)
			rightKeys := e.extractEquiJoinKeys(expr.Right, leftColumns, rightColumns)

			if leftKeys != nil && rightKeys != nil {
				return append(leftKeys, rightKeys...)
			} else if leftKeys != nil {
				return leftKeys
			} else {
				return rightKeys
			}
		}
	}

	return nil
}

// hashJoinEntry represents an entry in the hash table
type hashJoinEntry struct {
	chunk *storage.DataChunk
	row   int
}

// hashJoinValue creates an efficient hash key from a value
func hashJoinValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}

	switch v := val.(type) {
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int:
		return strconv.Itoa(v)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32)
	case string:
		return v
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// executeHashJoin performs a hash join for equi-joins
func (e *Executor) executeHashJoin(ctx context.Context, plan *JoinPlan, leftResult, rightResult *QueryResult, joinKeys []equiJoinKey, outputColumns []Column) (*QueryResult, error) {
	// Collect right side chunks (build side)
	rightChunks, err := collectAllChunks(rightResult)
	if err != nil {
		return nil, fmt.Errorf("collecting right chunks: %w", err)
	}

	// Build hash table from right side
	hashTable := make(map[string][]hashJoinEntry)

	for _, chunk := range rightChunks {
		for row := 0; row < chunk.Size(); row++ {
			// Build hash key from join columns
			keyParts := make([]string, len(joinKeys))
			skipRow := false
			for i, key := range joinKeys {
				val, err := chunk.GetValue(key.rightCol, row)
				if err != nil {
					return nil, err
				}
				if val == nil {
					// Skip NULL join keys
					skipRow = true
					break
				}
				keyParts[i] = hashJoinValue(val)
			}
			if skipRow {
				continue
			}
			hashKey := strings.Join(keyParts, "|") // More efficient key generation

			hashTable[hashKey] = append(hashTable[hashKey], hashJoinEntry{
				chunk: chunk,
				row:   row,
			})
		}
	}

	// Prepare output
	outputSchema := make([]storage.LogicalType, len(outputColumns))
	for i, col := range outputColumns {
		outputSchema[i] = col.Type
	}

	var resultChunks []*storage.DataChunk
	const chunkSize = 1000
	var currentChunk *storage.DataChunk
	currentSize := 0

	// Probe phase: scan left side and probe hash table
	leftResult.current = -1 // Reset to start

	for leftResult.Next() {
		leftChunk := leftResult.GetChunk()

		for leftRow := 0; leftRow < leftChunk.Size(); leftRow++ {
			// Build hash key from left side
			keyParts := make([]string, len(joinKeys))
			skipRow := false
			for i, key := range joinKeys {
				val, err := leftChunk.GetValue(key.leftCol, leftRow)
				if err != nil {
					return nil, err
				}
				if val == nil {
					// NULL keys never match in joins
					skipRow = true
					break
				}
				keyParts[i] = hashJoinValue(val)
			}

			hasMatch := false
			hashKey := ""

			if !skipRow {
				hashKey = strings.Join(keyParts, "|")
				// Probe hash table
				if entries, found := hashTable[hashKey]; found {
					hasMatch = true
					for _, entry := range entries {
						// Create new chunk if needed
						if currentChunk == nil || currentSize >= chunkSize {
							if currentChunk != nil {
								_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
								resultChunks = append(resultChunks, currentChunk)
							}
							currentChunk = storage.NewDataChunk(outputSchema, chunkSize)
							currentSize = 0
						}

						// Copy left columns
						for col := 0; col < leftChunk.ColumnCount(); col++ {
							val, err := leftChunk.GetValue(col, leftRow)
							if err != nil {
								return nil, err
							}
							if err := currentChunk.SetValue(col, currentSize, val); err != nil {
								return nil, err
							}
						}

						// Copy right columns
						for col := 0; col < entry.chunk.ColumnCount(); col++ {
							val, err := entry.chunk.GetValue(col, entry.row)
							if err != nil {
								return nil, err
							}
							if err := currentChunk.SetValue(leftChunk.ColumnCount()+col, currentSize, val); err != nil {
								return nil, err
							}
						}

						currentSize++
					}
				}
			}

			// For LEFT JOIN, if no match found, output left row with NULL right columns
			if plan.JoinType == LeftJoin && !hasMatch {
				// Create new chunk if needed
				if currentChunk == nil || currentSize >= chunkSize {
					if currentChunk != nil {
						_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
						resultChunks = append(resultChunks, currentChunk)
					}
					currentChunk = storage.NewDataChunk(outputSchema, chunkSize)
					currentSize = 0
				}

				// Copy left columns
				for col := 0; col < leftChunk.ColumnCount(); col++ {
					val, err := leftChunk.GetValue(col, leftRow)
					if err != nil {
						return nil, err
					}
					if err := currentChunk.SetValue(col, currentSize, val); err != nil {
						return nil, err
					}
				}

				// Set right columns to NULL
				rightColumnCount := len(outputColumns) - leftChunk.ColumnCount()
				for col := 0; col < rightColumnCount; col++ {
					if err := currentChunk.SetValue(leftChunk.ColumnCount()+col, currentSize, nil); err != nil {
						return nil, err
					}
				}

				currentSize++
			}
		}
	}

	// Add final chunk if it has data
	if currentChunk != nil && currentSize > 0 {
		_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
		resultChunks = append(resultChunks, currentChunk)
	}

	return &QueryResult{
		chunks:  resultChunks,
		columns: outputColumns,
		current: -1,
	}, nil
}

// executeNestedLoopJoin performs the original nested loop join
func (e *Executor) executeNestedLoopJoin(ctx context.Context, plan *JoinPlan, leftResult, rightResult *QueryResult, outputColumns []Column) (*QueryResult, error) {
	// Collect all data from both sides
	leftChunks, err := collectAllChunks(leftResult)
	if err != nil {
		return nil, fmt.Errorf("collecting left chunks: %w", err)
	}

	rightChunks, err := collectAllChunks(rightResult)
	if err != nil {
		return nil, fmt.Errorf("collecting right chunks: %w", err)
	}

	// Build output schema
	outputSchema := make([]storage.LogicalType, len(outputColumns))
	for i, col := range outputColumns {
		outputSchema[i] = col.Type
	}

	leftColumns := leftResult.columns
	rightColumns := rightResult.columns

	// Perform nested loop join
	var resultChunks []*storage.DataChunk
	const chunkSize = 1000

	// Create current output chunk
	var currentChunk *storage.DataChunk
	currentSize := 0

	// For each left row
	for _, leftChunk := range leftChunks {
		for leftRow := 0; leftRow < leftChunk.Size(); leftRow++ {
			// For each right row
			for _, rightChunk := range rightChunks {
				for rightRow := 0; rightRow < rightChunk.Size(); rightRow++ {
					// Evaluate join condition
					matches, err := e.evaluateJoinCondition(plan.Condition, leftChunk, leftRow, rightChunk, rightRow, leftColumns, rightColumns)
					if err != nil {
						return nil, fmt.Errorf("join condition evaluation: %w", err)
					}

					// If condition matches (or no condition for cross join)
					if matches || plan.Condition == nil {
						// Create new chunk if needed
						if currentChunk == nil || currentSize >= chunkSize {
							if currentChunk != nil {
								_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
								resultChunks = append(resultChunks, currentChunk)
							}
							currentChunk = storage.NewDataChunk(outputSchema, chunkSize)
							currentSize = 0
						}

						// Copy left columns
						for col := 0; col < leftChunk.ColumnCount(); col++ {
							val, err := leftChunk.GetValue(col, leftRow)
							if err != nil {
								return nil, err
							}
							if err := currentChunk.SetValue(col, currentSize, val); err != nil {
								return nil, err
							}
						}

						// Copy right columns
						for col := 0; col < rightChunk.ColumnCount(); col++ {
							val, err := rightChunk.GetValue(col, rightRow)
							if err != nil {
								return nil, err
							}
							if err := currentChunk.SetValue(leftChunk.ColumnCount()+col, currentSize, val); err != nil {
								return nil, err
							}
						}

						currentSize++
					}
				}
			}

			// For LEFT JOIN, if no matches found, add row with NULLs for right side
			if plan.JoinType == LeftJoin {
				hasMatch := false
				for _, rightChunk := range rightChunks {
					for rightRow := 0; rightRow < rightChunk.Size(); rightRow++ {
						matches, err := e.evaluateJoinCondition(plan.Condition, leftChunk, leftRow, rightChunk, rightRow, leftColumns, rightColumns)
						if err != nil {
							return nil, err
						}
						if matches {
							hasMatch = true
							break
						}
					}
					if hasMatch {
						break
					}
				}

				if !hasMatch {
					// Add left row with NULL right columns
					if currentChunk == nil || currentSize >= chunkSize {
						if currentChunk != nil {
							_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
							resultChunks = append(resultChunks, currentChunk)
						}
						currentChunk = storage.NewDataChunk(outputSchema, chunkSize)
						currentSize = 0
					}

					// Copy left columns
					for col := 0; col < leftChunk.ColumnCount(); col++ {
						val, err := leftChunk.GetValue(col, leftRow)
						if err != nil {
							return nil, err
						}
						if err := currentChunk.SetValue(col, currentSize, val); err != nil {
							return nil, err
						}
					}

					// Set right columns to NULL
					for col := 0; col < len(rightColumns); col++ {
						if err := currentChunk.SetValue(leftChunk.ColumnCount()+col, currentSize, nil); err != nil {
							return nil, err
						}
					}

					currentSize++
				}
			}
		}
	}

	// Add final chunk if it has data
	if currentChunk != nil && currentSize > 0 {
		_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
		resultChunks = append(resultChunks, currentChunk)
	}

	return &QueryResult{
		chunks:  resultChunks,
		columns: outputColumns,
		current: -1,
	}, nil
}

// executeUnion executes a UNION/UNION ALL operation
func (e *Executor) executeUnion(ctx context.Context, plan *UnionPlan) (*QueryResult, error) {
	// Execute left side
	leftResult, err := e.Execute(ctx, plan.Left)
	if err != nil {
		return nil, fmt.Errorf("union left side failed: %w", err)
	}
	defer func() { _ = leftResult.Close() }() // Closing errors not critical in defer

	// Execute right side
	rightResult, err := e.Execute(ctx, plan.Right)
	if err != nil {
		return nil, fmt.Errorf("union right side failed: %w", err)
	}
	defer func() { _ = rightResult.Close() }() // Closing errors not critical in defer

	// Validate schemas match
	if len(leftResult.columns) != len(rightResult.columns) {
		return nil, fmt.Errorf("UNION requires equal number of columns: left has %d, right has %d",
			len(leftResult.columns), len(rightResult.columns))
	}

	// For now, we'll use the left side's column metadata
	// In a full implementation, we'd validate type compatibility
	outputColumns := leftResult.columns

	// Create a unified schema for output
	outputSchema := make([]storage.LogicalType, len(outputColumns))
	for i, col := range outputColumns {
		outputSchema[i] = col.Type
	}

	// Collect all data and normalize to the output schema
	var allChunks []*storage.DataChunk
	const chunkSize = 1000
	var currentChunk *storage.DataChunk
	currentSize := 0

	// Collect left chunks
	leftResult.current = -1
	for leftResult.Next() {
		sourceChunk := leftResult.GetChunk()
		if sourceChunk != nil && sourceChunk.Size() > 0 {
			// Copy data from source chunk to normalized chunks
			for row := 0; row < sourceChunk.Size(); row++ {
				// Create new chunk if needed
				if currentChunk == nil || currentSize >= chunkSize {
					if currentChunk != nil {
						_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
						allChunks = append(allChunks, currentChunk)
					}
					currentChunk = storage.NewDataChunk(outputSchema, chunkSize)
					currentSize = 0
				}

				// Copy row data with type conversion if needed
				for col := 0; col < sourceChunk.ColumnCount() && col < len(outputSchema); col++ {
					val, err := sourceChunk.GetValue(col, row)
					if err != nil {
						return nil, fmt.Errorf("error reading left chunk: %w", err)
					}
					if err := currentChunk.SetValue(col, currentSize, val); err != nil {
						return nil, fmt.Errorf("error setting left chunk value: %w", err)
					}
				}
				currentSize++
			}
		}
	}

	// Collect right chunks
	rightResult.current = -1
	for rightResult.Next() {
		sourceChunk := rightResult.GetChunk()
		if sourceChunk != nil && sourceChunk.Size() > 0 {
			// Copy data from source chunk to normalized chunks
			for row := 0; row < sourceChunk.Size(); row++ {
				// Create new chunk if needed
				if currentChunk == nil || currentSize >= chunkSize {
					if currentChunk != nil {
						_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
						allChunks = append(allChunks, currentChunk)
					}
					currentChunk = storage.NewDataChunk(outputSchema, chunkSize)
					currentSize = 0
				}

				// Copy row data with type conversion if needed
				for col := 0; col < sourceChunk.ColumnCount() && col < len(outputSchema); col++ {
					val, err := sourceChunk.GetValue(col, row)
					if err != nil {
						return nil, fmt.Errorf("error reading right chunk: %w", err)
					}
					if err := currentChunk.SetValue(col, currentSize, val); err != nil {
						return nil, fmt.Errorf("error setting right chunk value: %w", err)
					}
				}
				currentSize++
			}
		}
	}

	// Add final chunk if it has data
	if currentChunk != nil && currentSize > 0 {
		_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
		allChunks = append(allChunks, currentChunk)
	}

	// For UNION (not UNION ALL), we need to remove duplicates
	if !plan.UnionAll {
		allChunks = e.removeDuplicateRows(allChunks, outputColumns)
	}

	// Transfer ownership of chunks to prevent cleanup
	leftResult.chunks = nil
	rightResult.chunks = nil

	return &QueryResult{
		chunks:  allChunks,
		columns: outputColumns,
		current: -1,
	}, nil
}

// removeDuplicateRows removes duplicate rows from chunks (for UNION without ALL)
func (e *Executor) removeDuplicateRows(chunks []*storage.DataChunk, columns []Column) []*storage.DataChunk {
	if len(chunks) == 0 {
		return chunks
	}

	// For simplicity, we'll use a hash set approach
	// In production, this would need to handle large datasets efficiently
	seen := make(map[string]bool)

	// Create schema for new chunks
	schema := make([]storage.LogicalType, len(columns))
	for i, col := range columns {
		schema[i] = col.Type
	}

	var resultChunks []*storage.DataChunk
	const chunkSize = 1000
	var currentChunk *storage.DataChunk
	currentSize := 0

	for _, chunk := range chunks {
		for row := 0; row < chunk.Size(); row++ {
			// Create row hash
			rowKey := ""
			for col := 0; col < chunk.ColumnCount(); col++ {
				val, err := chunk.GetValue(col, row)
				if err != nil {
					continue
				}
				if val == nil {
					rowKey += "<NULL>"
				} else {
					rowKey += fmt.Sprintf("%v", val)
				}
				rowKey += "|" // Column separator
			}

			// Check if we've seen this row
			if !seen[rowKey] {
				seen[rowKey] = true

				// Create new chunk if needed
				if currentChunk == nil || currentSize >= chunkSize {
					if currentChunk != nil {
						_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
						resultChunks = append(resultChunks, currentChunk)
					}
					currentChunk = storage.NewDataChunk(schema, chunkSize)
					currentSize = 0
				}

				// Copy row to result
				for col := 0; col < chunk.ColumnCount(); col++ {
					val, err := chunk.GetValue(col, row)
					if err != nil {
						continue
					}
					_ = currentChunk.SetValue(col, currentSize, val) // Value setting errors are not critical here
				}
				currentSize++
			}
		}
	}

	// Add final chunk if it has data
	if currentChunk != nil && currentSize > 0 {
		_ = currentChunk.SetSize(currentSize) // Size setting errors are not critical here
		resultChunks = append(resultChunks, currentChunk)
	}

	return resultChunks
}

// executeSubquery executes a subquery
func (e *Executor) executeSubquery(ctx context.Context, plan *SubqueryPlan) (*QueryResult, error) {
	// Execute the subquery plan
	result, err := e.Execute(ctx, plan.Plan)
	if err != nil {
		return nil, fmt.Errorf("subquery execution failed: %w", err)
	}

	return result, nil
}

// evaluateConstantValue recursively evaluates function calls within constant values like struct literals
func evaluateConstantValue(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case map[string]interface{}:
		// Recursively evaluate values in the map (struct fields)
		result := make(map[string]interface{})
		for key, val := range v {
			if strVal, ok := val.(string); ok {
				// Check if this string looks like a function call
				if strings.Contains(strVal, "(") && strings.Contains(strVal, ")") {
					// Try to parse and evaluate as a function expression
					if funcExpr := parseFunctionExpression(strVal); funcExpr != nil {
						evaluatedVal, err := evaluateExpressionWithContext(funcExpr, nil, 0, &EvaluationContext{})
						if err != nil {
							// If evaluation fails, keep the original string value
							result[key] = strVal
						} else {
							// The evaluated value should be properly typed (e.g., *types.MapAny)
							result[key] = evaluatedVal
						}
					} else {
						result[key] = strVal
					}
				} else {
					result[key] = strVal
				}
			} else {
				// Recursively evaluate non-string values
				evaluatedVal, err := evaluateConstantValue(val)
				if err != nil {
					return nil, err
				}
				result[key] = evaluatedVal
			}
		}
		return result, nil
	case []interface{}:
		// Recursively evaluate array elements
		result := make([]interface{}, len(v))
		for i, val := range v {
			evaluatedVal, err := evaluateConstantValue(val)
			if err != nil {
				return nil, err
			}
			result[i] = evaluatedVal
		}
		return result, nil
	default:
		// For non-composite types, return as is
		return value, nil
	}
}
