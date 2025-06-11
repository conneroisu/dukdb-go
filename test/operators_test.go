package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/connerohnesorge/dukdb-go/internal/engine"
	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

func TestVectorizedOperators(t *testing.T) {
	ctx := context.Background()

	t.Run("FilterOperator", func(t *testing.T) {
		// Create test data
		schema := []storage.LogicalType{
			{ID: storage.TypeInteger},
			{ID: storage.TypeVarchar},
			{ID: storage.TypeDouble},
		}
		
		chunk := storage.NewDataChunk(schema, 5)
		testData := [][]interface{}{
			{int32(1), "Alice", 95.5},
			{int32(2), "Bob", 87.3},
			{int32(3), "Charlie", 92.1},
			{int32(4), "David", 88.9},
			{int32(5), "Eve", 94.2},
		}
		
		for row, data := range testData {
			for col, val := range data {
				chunk.SetValue(col, row, val)
			}
		}
		chunk.SetSize(5)
		
		// Test filter: score > 90
		predicate := &engine.BinaryExpr{
			Left:  &engine.ColumnExpr{Column: "score"}, // Column 2
			Op:    engine.OpGt,
			Right: &engine.ConstantExpr{Value: 90.0},
		}
		
		filterOp := engine.NewFilterOperator(predicate, schema)
		result, err := filterOp.Execute(ctx, chunk)
		if err != nil {
			t.Fatalf("Filter execution failed: %v", err)
		}
		
		// Should have 3 rows (Alice, Charlie, Eve)
		if result.Size() != 3 {
			t.Errorf("Expected 3 rows after filter, got %d", result.Size())
		}
		
		// Verify filtered data
		expectedNames := []string{"Alice", "Charlie", "Eve"}
		for i := 0; i < result.Size(); i++ {
			name, _ := result.GetValue(1, i)
			if name != expectedNames[i] {
				t.Errorf("Row %d: expected %s, got %v", i, expectedNames[i], name)
			}
		}
	})

	t.Run("ProjectOperator", func(t *testing.T) {
		// Create test data
		schema := []storage.LogicalType{
			{ID: storage.TypeInteger},
			{ID: storage.TypeVarchar},
			{ID: storage.TypeDouble},
		}
		
		chunk := storage.NewDataChunk(schema, 3)
		testData := [][]interface{}{
			{int32(1), "Alice", 95.5},
			{int32(2), "Bob", 87.3},
			{int32(3), "Charlie", 92.1},
		}
		
		for row, data := range testData {
			for col, val := range data {
				chunk.SetValue(col, row, val)
			}
		}
		chunk.SetSize(3)
		
		// Project columns 1 and 2 (name and score)
		expressions := []engine.Expression{
			&engine.ColumnExpr{Column: "name"},
			&engine.ColumnExpr{Column: "score"},
		}
		
		projOp := engine.NewProjectOperator(expressions, schema)
		result, err := projOp.Execute(ctx, chunk)
		if err != nil {
			t.Fatalf("Projection execution failed: %v", err)
		}
		
		// Should have 2 columns
		if result.ColumnCount() != 2 {
			t.Errorf("Expected 2 columns after projection, got %d", result.ColumnCount())
		}
		
		// Verify data
		for i := 0; i < result.Size(); i++ {
			name, _ := result.GetValue(0, i)
			score, _ := result.GetValue(1, i)
			
			if name != testData[i][1] {
				t.Errorf("Row %d: name mismatch", i)
			}
			if score != testData[i][2] {
				t.Errorf("Row %d: score mismatch", i)
			}
		}
	})

	t.Run("SortOperator", func(t *testing.T) {
		// Create test data
		schema := []storage.LogicalType{
			{ID: storage.TypeInteger},
			{ID: storage.TypeVarchar},
			{ID: storage.TypeDouble},
		}
		
		chunk := storage.NewDataChunk(schema, 4)
		testData := [][]interface{}{
			{int32(3), "Charlie", 92.1},
			{int32(1), "Alice", 95.5},
			{int32(4), "David", 88.9},
			{int32(2), "Bob", 87.3},
		}
		
		for row, data := range testData {
			for col, val := range data {
				chunk.SetValue(col, row, val)
			}
		}
		chunk.SetSize(4)
		
		// Sort by score descending
		orderBy := []engine.OrderByExpr{
			{
				Expr: &engine.ColumnExpr{Column: "score"},
				Desc: true,
			},
		}
		
		sortOp := engine.NewSortOperator(orderBy, schema)
		result, err := sortOp.Execute(ctx, chunk)
		if err != nil {
			t.Fatalf("Sort execution failed: %v", err)
		}
		
		// Verify order
		expectedOrder := []string{"Alice", "Charlie", "David", "Bob"}
		for i := 0; i < result.Size(); i++ {
			name, _ := result.GetValue(1, i)
			if name != expectedOrder[i] {
				t.Errorf("Row %d: expected %s, got %v", i, expectedOrder[i], name)
			}
		}
	})

	t.Run("LimitOperator", func(t *testing.T) {
		// Create test data
		schema := []storage.LogicalType{
			{ID: storage.TypeInteger},
			{ID: storage.TypeVarchar},
		}
		
		chunk := storage.NewDataChunk(schema, 10)
		for i := 0; i < 10; i++ {
			chunk.SetValue(0, i, int32(i))
			chunk.SetValue(1, i, fmt.Sprintf("Row%d", i))
		}
		chunk.SetSize(10)
		
		// Test LIMIT 3 OFFSET 2
		limitOp := engine.NewLimitOperator(3, 2, schema)
		result, err := limitOp.Execute(ctx, chunk)
		if err != nil {
			t.Fatalf("Limit execution failed: %v", err)
		}
		
		// Should have 3 rows
		if result.Size() != 3 {
			t.Errorf("Expected 3 rows after limit, got %d", result.Size())
		}
		
		// Verify we got rows 2, 3, 4
		for i := 0; i < result.Size(); i++ {
			id, _ := result.GetValue(0, i)
			expectedID := int32(i + 2)
			if id != expectedID {
				t.Errorf("Row %d: expected ID %d, got %v", i, expectedID, id)
			}
		}
	})
}

func TestAggregateOperators(t *testing.T) {
	ctx := context.Background()

	t.Run("SimpleAggregates", func(t *testing.T) {
		// Create test data
		schema := []storage.LogicalType{
			{ID: storage.TypeInteger},
			{ID: storage.TypeDouble},
		}
		
		chunk := storage.NewDataChunk(schema, 5)
		values := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
		
		for i, val := range values {
			chunk.SetValue(0, i, int32(i+1))
			chunk.SetValue(1, i, val)
		}
		chunk.SetSize(5)
		
		// Test COUNT, SUM, AVG, MIN, MAX
		aggregates := []engine.AggregateExpr{
			{
				Function: &engine.CountAggregate{},
				Input:    &engine.ColumnExpr{Column: "id"},
				Alias:    "count",
			},
			{
				Function: engine.NewSumAggregate(schema[1]),
				Input:    &engine.ColumnExpr{Column: "value"},
				Alias:    "sum",
			},
			{
				Function: &engine.AvgAggregate{},
				Input:    &engine.ColumnExpr{Column: "value"},
				Alias:    "avg",
			},
			{
				Function: engine.NewMinAggregate(schema[1]),
				Input:    &engine.ColumnExpr{Column: "value"},
				Alias:    "min",
			},
			{
				Function: engine.NewMaxAggregate(schema[1]),
				Input:    &engine.ColumnExpr{Column: "value"},
				Alias:    "max",
			},
		}
		
		aggOp := engine.NewAggregateOperator(nil, aggregates, schema)
		result, err := aggOp.Execute(ctx, chunk)
		if err != nil {
			t.Fatalf("Aggregate execution failed: %v", err)
		}
		
		// Verify results
		if result.Size() != 1 {
			t.Fatalf("Expected 1 row, got %d", result.Size())
		}
		
		count, _ := result.GetValue(0, 0)
		if count.(int64) != 5 {
			t.Errorf("COUNT: expected 5, got %v", count)
		}
		
		sum, _ := result.GetValue(1, 0)
		if sum.(float64) != 150.0 {
			t.Errorf("SUM: expected 150.0, got %v", sum)
		}
		
		avg, _ := result.GetValue(2, 0)
		if avg.(float64) != 30.0 {
			t.Errorf("AVG: expected 30.0, got %v", avg)
		}
		
		min, _ := result.GetValue(3, 0)
		if min.(float64) != 10.0 {
			t.Errorf("MIN: expected 10.0, got %v", min)
		}
		
		max, _ := result.GetValue(4, 0)
		if max.(float64) != 50.0 {
			t.Errorf("MAX: expected 50.0, got %v", max)
		}
	})

	t.Run("GroupByAggregates", func(t *testing.T) {
		// Create test data with groups
		schema := []storage.LogicalType{
			{ID: storage.TypeVarchar}, // category
			{ID: storage.TypeInteger}, // value
		}
		
		chunk := storage.NewDataChunk(schema, 6)
		testData := [][]interface{}{
			{"A", int32(10)},
			{"B", int32(20)},
			{"A", int32(30)},
			{"B", int32(40)},
			{"A", int32(50)},
			{"C", int32(60)},
		}
		
		for row, data := range testData {
			for col, val := range data {
				chunk.SetValue(col, row, val)
			}
		}
		chunk.SetSize(6)
		
		// GROUP BY category, SUM(value)
		groupBy := []engine.Expression{
			&engine.ColumnExpr{Column: "category"},
		}
		
		aggregates := []engine.AggregateExpr{
			{
				Function: engine.NewSumAggregate(schema[1]),
				Input:    &engine.ColumnExpr{Column: "value"},
				Alias:    "sum_value",
			},
		}
		
		aggOp := engine.NewAggregateOperator(groupBy, aggregates, schema)
		result, err := aggOp.Execute(ctx, chunk)
		if err != nil {
			t.Fatalf("Group by aggregate execution failed: %v", err)
		}
		
		// Should have 3 groups
		if result.Size() != 3 {
			t.Fatalf("Expected 3 groups, got %d", result.Size())
		}
		
		// Verify results (order may vary)
		expectedSums := map[string]int32{
			"A": 90,  // 10 + 30 + 50
			"B": 60,  // 20 + 40
			"C": 60,  // 60
		}
		
		for i := 0; i < result.Size(); i++ {
			category, _ := result.GetValue(0, i)
			sum, _ := result.GetValue(1, i)
			
			expectedSum, ok := expectedSums[category.(string)]
			if !ok {
				t.Errorf("Unexpected category: %v", category)
			}
			
			if sum.(int32) != expectedSum {
				t.Errorf("Category %s: expected sum %d, got %v", 
					category, expectedSum, sum)
			}
		}
	})

	t.Run("StandardDeviation", func(t *testing.T) {
		// Create test data
		schema := []storage.LogicalType{
			{ID: storage.TypeDouble},
		}
		
		chunk := storage.NewDataChunk(schema, 5)
		values := []float64{2.0, 4.0, 4.0, 4.0, 5.0}
		
		for i, val := range values {
			chunk.SetValue(0, i, val)
		}
		chunk.SetSize(5)
		
		// Test STDDEV (sample)
		aggregates := []engine.AggregateExpr{
			{
				Function: engine.NewStdDevAggregate(true),
				Input:    &engine.ColumnExpr{Column: "value"},
				Alias:    "stddev",
			},
		}
		
		aggOp := engine.NewAggregateOperator(nil, aggregates, schema)
		result, err := aggOp.Execute(ctx, chunk)
		if err != nil {
			t.Fatalf("StdDev execution failed: %v", err)
		}
		
		stddev, _ := result.GetValue(0, 0)
		// Expected stddev ≈ 1.095
		expected := 1.095
		actual := stddev.(float64)
		
		if abs(actual-expected) > 0.01 {
			t.Errorf("STDDEV: expected ~%f, got %f", expected, actual)
		}
	})
}

func TestComplexQueries(t *testing.T) {
	ctx := context.Background()

	t.Run("FilterProjectSort", func(t *testing.T) {
		// Create test data
		schema := []storage.LogicalType{
			{ID: storage.TypeInteger},
			{ID: storage.TypeVarchar},
			{ID: storage.TypeDouble},
		}
		
		chunk := storage.NewDataChunk(schema, 5)
		testData := [][]interface{}{
			{int32(1), "Alice", 95.5},
			{int32(2), "Bob", 87.3},
			{int32(3), "Charlie", 92.1},
			{int32(4), "David", 88.9},
			{int32(5), "Eve", 94.2},
		}
		
		for row, data := range testData {
			for col, val := range data {
				chunk.SetValue(col, row, val)
			}
		}
		chunk.SetSize(5)
		
		// Filter: score > 90
		filterPred := &engine.BinaryExpr{
			Left:  &engine.ColumnExpr{Column: "score"},
			Op:    engine.OpGt,
			Right: &engine.ConstantExpr{Value: 90.0},
		}
		filterOp := engine.NewFilterOperator(filterPred, schema)
		
		// Project: name and score
		projExprs := []engine.Expression{
			&engine.ColumnExpr{Column: "name"},
			&engine.ColumnExpr{Column: "score"},
		}
		projOp := engine.NewProjectOperator(projExprs, schema)
		
		// Sort by score descending
		orderBy := []engine.OrderByExpr{
			{
				Expr: &engine.ColumnExpr{Column: "score"},
				Desc: true,
			},
		}
		projSchema := projOp.GetOutputSchema()
		sortOp := engine.NewSortOperator(orderBy, projSchema)
		
		// Execute pipeline
		filtered, err := filterOp.Execute(ctx, chunk)
		if err != nil {
			t.Fatalf("Filter failed: %v", err)
		}
		
		projected, err := projOp.Execute(ctx, filtered)
		if err != nil {
			t.Fatalf("Project failed: %v", err)
		}
		
		sorted, err := sortOp.Execute(ctx, projected)
		if err != nil {
			t.Fatalf("Sort failed: %v", err)
		}
		
		// Verify final result
		if sorted.Size() != 3 {
			t.Errorf("Expected 3 rows, got %d", sorted.Size())
		}
		
		expectedOrder := []string{"Alice", "Eve", "Charlie"}
		for i := 0; i < sorted.Size(); i++ {
			name, _ := sorted.GetValue(0, i)
			if name != expectedOrder[i] {
				t.Errorf("Row %d: expected %s, got %v", i, expectedOrder[i], name)
			}
		}
	})
}

// Helper function for absolute value
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}