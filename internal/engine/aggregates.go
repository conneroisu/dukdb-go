package engine

import (
	"context"
	"fmt"
	"math"

	"github.com/connerohnesorge/dukdb-go/internal/storage"
)

// AggregateFunction represents an aggregate function
type AggregateFunction interface {
	// Initialize prepares the aggregate state
	Initialize() AggregateState
	// Update adds a value to the aggregate
	Update(state AggregateState, value interface{}) error
	// Finalize computes the final result
	Finalize(state AggregateState) (interface{}, error)
	// GetResultType returns the result type
	GetResultType() storage.LogicalType
}

// AggregateState holds the state for an aggregate computation
type AggregateState interface {
	Reset()
}

// AggregateOperator performs aggregation operations
type AggregateOperator struct {
	groupBy     []Expression
	aggregates  []AggregateExpr
	schema      []storage.LogicalType
	columnNames []string // Column names for context
}

// AggregateExpr represents an aggregate expression
type AggregateExpr struct {
	Function AggregateFunction
	Input    Expression
	Alias    string
}

// NewAggregateOperator creates a new aggregate operator
func NewAggregateOperator(groupBy []Expression, aggregates []AggregateExpr, inputSchema []storage.LogicalType) *AggregateOperator {
	return NewAggregateOperatorWithContext(groupBy, aggregates, inputSchema, nil)
}

// NewAggregateOperatorWithContext creates a new aggregate operator with column names
func NewAggregateOperatorWithContext(groupBy []Expression, aggregates []AggregateExpr, inputSchema []storage.LogicalType, columnNames []string) *AggregateOperator {
	// Build output schema
	schema := make([]storage.LogicalType, 0, len(groupBy)+len(aggregates))
	
	// Add group by columns
	for _, expr := range groupBy {
		schema = append(schema, inferExpressionType(expr, inputSchema))
	}
	
	// Add aggregate columns
	for _, agg := range aggregates {
		schema = append(schema, agg.Function.GetResultType())
	}
	
	return &AggregateOperator{
		groupBy:     groupBy,
		aggregates:  aggregates,
		schema:      schema,
		columnNames: columnNames,
	}
}

// Execute performs the aggregation
func (a *AggregateOperator) Execute(ctx context.Context, input *storage.DataChunk) (*storage.DataChunk, error) {
	if len(a.groupBy) > 0 {
		return a.executeGroupedAggregate(ctx, input)
	}
	return a.executeSimpleAggregate(ctx, input)
}

// executeSimpleAggregate performs aggregation without GROUP BY
func (a *AggregateOperator) executeSimpleAggregate(ctx context.Context, input *storage.DataChunk) (*storage.DataChunk, error) {
	// Initialize aggregate states
	states := make([]AggregateState, len(a.aggregates))
	for i, agg := range a.aggregates {
		states[i] = agg.Function.Initialize()
	}
	
	// Update aggregates for all rows
	for row := 0; row < input.Size(); row++ {
		for i, agg := range a.aggregates {
			// Special handling for COUNT(*) - don't evaluate the * expression
			if isCountStar(agg) {
				// For COUNT(*), always pass 1 (any non-null value)
				if err := agg.Function.Update(states[i], 1); err != nil {
					return nil, err
				}
			} else {
				// Create evaluation context if we have column names
				var evalCtx *EvaluationContext
				if a.columnNames != nil {
					evalCtx = &EvaluationContext{
						columnNames: a.columnNames,
					}
				}
				
				var val interface{}
				var err error
				if evalCtx != nil {
					val, err = evaluateExpressionWithContext(agg.Input, input, row, evalCtx)
				} else {
					val, err = evaluateExpression(agg.Input, input, row)
				}
				if err != nil {
					return nil, err
				}
				
				// Skip NULL values for most aggregates
				if val != nil {
					if err := agg.Function.Update(states[i], val); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	
	// Create output chunk with single row
	output := storage.NewDataChunk(a.schema, 1)
	
	// Finalize aggregates
	col := 0
	for i, agg := range a.aggregates {
		result, err := agg.Function.Finalize(states[i])
		if err != nil {
			return nil, err
		}
		
		if err := output.SetValue(col, 0, result); err != nil {
			return nil, err
		}
		col++
	}
	
	output.SetSize(1)
	return output, nil
}

// executeGroupedAggregate performs aggregation with GROUP BY
func (a *AggregateOperator) executeGroupedAggregate(ctx context.Context, input *storage.DataChunk) (*storage.DataChunk, error) {
	// This should be called with all data already merged
	// In a real implementation, we'd support streaming aggregation
	
	// Create hash table for groups
	groups := make(map[string]*groupState)
	
	// Process each row
	for row := 0; row < input.Size(); row++ {
		// Compute group key
		key, groupValues, err := a.computeGroupKey(input, row)
		if err != nil {
			return nil, err
		}
		
		// Get or create group state
		group, exists := groups[key]
		if !exists {
			group = &groupState{
				key:    key,
				values: groupValues,
				states: make([]AggregateState, len(a.aggregates)),
			}
			for i, agg := range a.aggregates {
				group.states[i] = agg.Function.Initialize()
			}
			groups[key] = group
		}
		
		// Update aggregates for this group
		for i, agg := range a.aggregates {
			// Special handling for COUNT(*) - don't evaluate the * expression
			if isCountStar(agg) {
				// For COUNT(*), always pass 1 (any non-null value)
				if err := agg.Function.Update(group.states[i], 1); err != nil {
					return nil, err
				}
			} else {
				// Create evaluation context if we have column names
				var evalCtx *EvaluationContext
				if a.columnNames != nil {
					evalCtx = &EvaluationContext{
						columnNames: a.columnNames,
					}
				}
				
				var val interface{}
				var err error
				if evalCtx != nil {
					val, err = evaluateExpressionWithContext(agg.Input, input, row, evalCtx)
				} else {
					val, err = evaluateExpression(agg.Input, input, row)
				}
				if err != nil {
					return nil, err
				}
				
				if val != nil {
					if err := agg.Function.Update(group.states[i], val); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	
	// Create output chunk
	output := storage.NewDataChunk(a.schema, len(groups))
	outRow := 0
	
	// Process each group
	for _, group := range groups {
		col := 0
		
		// Add group by values
		for i := range a.groupBy {
			if err := output.SetValue(col, outRow, group.values[i]); err != nil {
				return nil, err
			}
			col++
		}
		
		// Add aggregate results
		for i, agg := range a.aggregates {
			result, err := agg.Function.Finalize(group.states[i])
			if err != nil {
				return nil, err
			}
			
			if err := output.SetValue(col, outRow, result); err != nil {
				return nil, err
			}
			col++
		}
		
		outRow++
	}
	
	output.SetSize(len(groups))
	return output, nil
}

// computeGroupKey computes the group key for a row
func (a *AggregateOperator) computeGroupKey(chunk *storage.DataChunk, row int) (string, []interface{}, error) {
	values := make([]interface{}, len(a.groupBy))
	key := ""
	
	for i, expr := range a.groupBy {
		val, err := evaluateExpression(expr, chunk, row)
		if err != nil {
			return "", nil, err
		}
		
		values[i] = val
		key += fmt.Sprintf("%v|", val)
	}
	
	return key, values, nil
}

// GetOutputSchema returns the output schema
func (a *AggregateOperator) GetOutputSchema() []storage.LogicalType {
	return a.schema
}

// groupState holds the state for a group
type groupState struct {
	key    string
	values []interface{}
	states []AggregateState
}

// Aggregate function implementations

// CountAggregate implements COUNT(*)
type CountAggregate struct{}

type countState struct {
	count int64
}

func (c *CountAggregate) Initialize() AggregateState {
	return &countState{}
}

func (c *CountAggregate) Update(state AggregateState, value interface{}) error {
	s := state.(*countState)
	s.count++
	return nil
}

func (c *CountAggregate) Finalize(state AggregateState) (interface{}, error) {
	s := state.(*countState)
	return int32(s.count), nil
}

func (c *CountAggregate) GetResultType() storage.LogicalType {
	return storage.LogicalType{ID: storage.TypeInteger}
}

func (s *countState) Reset() {
	s.count = 0
}

// SumAggregate implements SUM()
type SumAggregate struct {
	inputType storage.LogicalType
}

type sumState struct {
	sum      float64
	hasValue bool
}

func NewSumAggregate(inputType storage.LogicalType) *SumAggregate {
	return &SumAggregate{inputType: inputType}
}

func (s *SumAggregate) Initialize() AggregateState {
	return &sumState{}
}

func (s *SumAggregate) Update(state AggregateState, value interface{}) error {
	st := state.(*sumState)
	
	var numVal float64
	switch v := value.(type) {
	case int32:
		numVal = float64(v)
	case int64:
		numVal = float64(v)
	case float32:
		numVal = float64(v)
	case float64:
		numVal = v
	case int:
		numVal = float64(v)
	default:
		return fmt.Errorf("SUM: cannot sum %T", value)
	}
	
	st.sum += numVal
	st.hasValue = true
	return nil
}

func (s *SumAggregate) Finalize(state AggregateState) (interface{}, error) {
	st := state.(*sumState)
	if !st.hasValue {
		return nil, nil // Return NULL for empty set
	}
	
	// Return appropriate type based on input
	switch s.inputType.ID {
	case storage.TypeInteger:
		return int32(st.sum), nil
	case storage.TypeBigInt:
		return int64(st.sum), nil
	default:
		return st.sum, nil
	}
}

func (s *SumAggregate) GetResultType() storage.LogicalType {
	// Return the same type as input for integers
	switch s.inputType.ID {
	case storage.TypeTinyInt, storage.TypeSmallInt:
		return storage.LogicalType{ID: storage.TypeInteger}
	case storage.TypeInteger:
		return s.inputType // Keep as integer
	case storage.TypeBigInt:
		return s.inputType
	default:
		return storage.LogicalType{ID: storage.TypeDouble}
	}
}

func (s *sumState) Reset() {
	s.sum = 0
	s.hasValue = false
}

// AvgAggregate implements AVG()
type AvgAggregate struct{}

type avgState struct {
	sum   float64
	count int64
}

func (a *AvgAggregate) Initialize() AggregateState {
	return &avgState{}
}

func (a *AvgAggregate) Update(state AggregateState, value interface{}) error {
	s := state.(*avgState)
	
	var numVal float64
	switch v := value.(type) {
	case int32:
		numVal = float64(v)
	case int64:
		numVal = float64(v)
	case float32:
		numVal = float64(v)
	case float64:
		numVal = v
	default:
		return fmt.Errorf("AVG: cannot average %T", value)
	}
	
	s.sum += numVal
	s.count++
	return nil
}

func (a *AvgAggregate) Finalize(state AggregateState) (interface{}, error) {
	s := state.(*avgState)
	if s.count == 0 {
		return nil, nil // Return NULL for empty set
	}
	return s.sum / float64(s.count), nil
}

func (a *AvgAggregate) GetResultType() storage.LogicalType {
	return storage.LogicalType{ID: storage.TypeDouble}
}

func (s *avgState) Reset() {
	s.sum = 0
	s.count = 0
}

// MinAggregate implements MIN()
type MinAggregate struct {
	inputType storage.LogicalType
}

type minState struct {
	min      interface{}
	hasValue bool
}

func NewMinAggregate(inputType storage.LogicalType) *MinAggregate {
	return &MinAggregate{inputType: inputType}
}

func (m *MinAggregate) Initialize() AggregateState {
	return &minState{}
}

func (m *MinAggregate) Update(state AggregateState, value interface{}) error {
	s := state.(*minState)
	
	if !s.hasValue {
		s.min = value
		s.hasValue = true
		return nil
	}
	
	if compareValues(value, s.min) < 0 {
		s.min = value
	}
	
	return nil
}

func (m *MinAggregate) Finalize(state AggregateState) (interface{}, error) {
	s := state.(*minState)
	if !s.hasValue {
		return nil, nil
	}
	return s.min, nil
}

func (m *MinAggregate) GetResultType() storage.LogicalType {
	return m.inputType
}

func (s *minState) Reset() {
	s.min = nil
	s.hasValue = false
}

// MaxAggregate implements MAX()
type MaxAggregate struct {
	inputType storage.LogicalType
}

type maxState struct {
	max      interface{}
	hasValue bool
}

func NewMaxAggregate(inputType storage.LogicalType) *MaxAggregate {
	return &MaxAggregate{inputType: inputType}
}

func (m *MaxAggregate) Initialize() AggregateState {
	return &maxState{}
}

func (m *MaxAggregate) Update(state AggregateState, value interface{}) error {
	s := state.(*maxState)
	
	if !s.hasValue {
		s.max = value
		s.hasValue = true
		return nil
	}
	
	if compareValues(value, s.max) > 0 {
		s.max = value
	}
	
	return nil
}

func (m *MaxAggregate) Finalize(state AggregateState) (interface{}, error) {
	s := state.(*maxState)
	if !s.hasValue {
		return nil, nil
	}
	return s.max, nil
}

func (m *MaxAggregate) GetResultType() storage.LogicalType {
	return m.inputType
}

func (s *maxState) Reset() {
	s.max = nil
	s.hasValue = false
}

// StdDevAggregate implements STDDEV()
type StdDevAggregate struct {
	sample bool // true for sample stddev, false for population
}

type stddevState struct {
	count int64
	mean  float64
	m2    float64 // Sum of squares of differences from mean
}

func NewStdDevAggregate(sample bool) *StdDevAggregate {
	return &StdDevAggregate{sample: sample}
}

func (s *StdDevAggregate) Initialize() AggregateState {
	return &stddevState{}
}

func (s *StdDevAggregate) Update(state AggregateState, value interface{}) error {
	st := state.(*stddevState)
	
	var numVal float64
	switch v := value.(type) {
	case int32:
		numVal = float64(v)
	case int64:
		numVal = float64(v)
	case float32:
		numVal = float64(v)
	case float64:
		numVal = v
	default:
		return fmt.Errorf("STDDEV: cannot compute standard deviation of %T", value)
	}
	
	// Welford's online algorithm
	st.count++
	delta := numVal - st.mean
	st.mean += delta / float64(st.count)
	delta2 := numVal - st.mean
	st.m2 += delta * delta2
	
	return nil
}

func (s *StdDevAggregate) Finalize(state AggregateState) (interface{}, error) {
	st := state.(*stddevState)
	
	if st.count == 0 {
		return nil, nil
	}
	
	if st.count == 1 {
		return 0.0, nil
	}
	
	var variance float64
	if s.sample {
		variance = st.m2 / float64(st.count-1)
	} else {
		variance = st.m2 / float64(st.count)
	}
	
	return math.Sqrt(variance), nil
}

func (s *StdDevAggregate) GetResultType() storage.LogicalType {
	return storage.LogicalType{ID: storage.TypeDouble}
}

func (s *stddevState) Reset() {
	s.count = 0
	s.mean = 0
	s.m2 = 0
}

// Helper function to create aggregate functions
func CreateAggregateFunction(name string, inputType storage.LogicalType) (AggregateFunction, error) {
	switch name {
	case "COUNT":
		return &CountAggregate{}, nil
	case "SUM":
		return NewSumAggregate(inputType), nil
	case "AVG", "AVERAGE":
		return &AvgAggregate{}, nil
	case "MIN":
		return NewMinAggregate(inputType), nil
	case "MAX":
		return NewMaxAggregate(inputType), nil
	case "STDDEV", "STDDEV_SAMP":
		return NewStdDevAggregate(true), nil
	case "STDDEV_POP":
		return NewStdDevAggregate(false), nil
	default:
		return nil, fmt.Errorf("unknown aggregate function: %s", name)
	}
}

// isCountStar checks if an aggregate is COUNT(*)
func isCountStar(agg AggregateExpr) bool {
	// Check if it's a COUNT function
	if _, ok := agg.Function.(*CountAggregate); !ok {
		return false
	}
	
	// Check if the input is a * column reference
	if colExpr, ok := agg.Input.(*ColumnExpr); ok && colExpr.Column == "*" {
		return true
	}
	
	return false
}
