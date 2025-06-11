package engine

import (
	"context"
	"fmt"

	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

// executeAggregate executes an aggregate operation
func (e *Executor) executeAggregate(ctx context.Context, plan *AggregatePlan) (*QueryResult, error) {
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
	
	// Create column names from input schema
	columnNames := make([]string, len(childResult.columns))
	for i, col := range childResult.columns {
		columnNames[i] = col.Name
	}
	
	// Create aggregate operator with context
	aggOp := NewAggregateOperatorWithContext(plan.GroupBy, plan.Aggregates, inputSchema, columnNames)
	
	
	// For aggregates, we need to process all data
	// Collect all chunks
	var allChunks []*storage.DataChunk
	childResult.current = -1
	for childResult.Next() {
		allChunks = append(allChunks, childResult.GetChunk())
	}
	
	if len(allChunks) == 0 {
		// Empty input - create result with NULL aggregates
		emptyChunk := storage.NewDataChunk(inputSchema, 0)
		result, err := aggOp.Execute(ctx, emptyChunk)
		if err != nil {
			return nil, err
		}
		
		// Create column metadata
		columns := e.createAggregateColumns(plan, aggOp.GetOutputSchema())
		
		return &QueryResult{
			chunks:  []*storage.DataChunk{result},
			columns: columns,
			current: -1,
		}, nil
	}
	
	// Process chunks sequentially (aggregate operator handles accumulation)
	var resultChunk *storage.DataChunk
	for _, chunk := range allChunks {
		if resultChunk == nil {
			resultChunk, err = aggOp.Execute(ctx, chunk)
			if err != nil {
				return nil, err
			}
		} else {
			// For multi-chunk aggregation, we'd need to merge results
			// For now, we'll merge all chunks first
			break
		}
	}
	
	// If we have multiple chunks, merge them first
	if len(allChunks) > 1 {
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
		
		// Execute aggregate on merged chunk
		resultChunk, err = aggOp.Execute(ctx, mergedChunk)
		if err != nil {
			return nil, err
		}
	}
	
	// Create column metadata
	columns := e.createAggregateColumns(plan, aggOp.GetOutputSchema())
	
	
	return &QueryResult{
		chunks:  []*storage.DataChunk{resultChunk},
		columns: columns,
		current: -1,
	}, nil
}

// createAggregateColumns creates column metadata for aggregate results
func (e *Executor) createAggregateColumns(plan *AggregatePlan, schema []storage.LogicalType) []Column {
	columns := make([]Column, len(schema))
	col := 0
	
	// Group by columns - use actual column names
	for _, groupByExpr := range plan.GroupBy {
		if colExpr, ok := groupByExpr.(*ColumnExpr); ok {
			columns[col] = Column{
				Name: colExpr.Column,
				Type: schema[col],
			}
		} else {
			columns[col] = Column{
				Name: fmt.Sprintf("group_%d", col),
				Type: schema[col],
			}
		}
		col++
	}
	
	// Aggregate columns - use aliases if available
	for _, agg := range plan.Aggregates {
		name := agg.Alias
		if name == "" {
			// Generate a simple name
			name = fmt.Sprintf("agg_%d", col)
		}
		columns[col] = Column{
			Name: name,
			Type: schema[col],
		}
		col++
	}
	
	return columns
}