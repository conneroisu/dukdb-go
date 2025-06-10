# Query Optimization and Planning Architecture for DukDB-Go

## Executive Summary

This document outlines comprehensive query optimization strategies for DukDB-Go, a pure-Go implementation of DuckDB. The design incorporates DuckDB's proven optimization techniques while addressing Go-specific runtime considerations like goroutine coordination, garbage collection pressure, and memory allocation patterns.

## 1. DuckDB Optimizer Analysis

### 1.1 Core Architecture Insights

DuckDB's optimizer follows a multi-stage pipeline:

1. **Parser** → SQL string to parse tree
1. **Binder** → Resolve tables/columns using catalog
1. **Logical Planner** → Create LogicalOperator tree
1. **Optimizer** → Apply optimization rules
1. **Physical Planner** → Convert to PhysicalOperator tree
1. **Execution** → Push-based vectorized execution

### 1.2 Key Optimization Rules

**Rule-Based Optimizations:**

- Filter Pushdown: Move filters closer to data sources
- Common Sub-Expression Elimination: Extract duplicate computations
- IN Clause Rewriter: Convert large IN clauses to joins
- Expression Rewriter: Simplify and constant-fold expressions
- TopN Optimization: Replace sort+limit with efficient TopN

**Cost-Based Optimizations:**

- Join Order Optimization: Uses DPccp algorithm
- Statistics-driven cardinality estimation
- Adaptive query processing based on runtime statistics

### 1.3 Performance Impact

DuckDB's optimizer provides:

- 10x+ performance improvements in typical cases
- Automatic adaptation to changing data characteristics
- Elimination of manual query tuning requirements

## 2. Go-Specific Optimization Considerations

### 2.1 Runtime Characteristics

**Goroutine Coordination Costs:**

- Context switching overhead between goroutines
- Channel communication latency
- Synchronization primitives impact

**Memory Allocation Patterns:**

- Stack vs heap allocation decisions
- Slice/map growth patterns
- Escape analysis implications

**GC Pressure Factors:**

- Allocation frequency and object lifetime
- Stop-the-world pause minimization
- Memory layout for cache efficiency

### 2.2 Optimization Opportunities

**Concurrent Query Processing:**

- Parallel operator execution via goroutines
- Pipeline parallelism between operators
- Intra-operator parallelism for large datasets

**Memory Management:**

- Object pooling for frequently allocated structures
- Pre-allocation of known-size collections
- Columnar data layout for cache efficiency

## 3. Three Optimization Approaches

### 3.1 Approach 1: Rule-Based with Heuristics

**Architecture:**

```go
type RuleBasedOptimizer struct {
    rules []OptimizationRule
    config *OptimizerConfig
}

type OptimizationRule interface {
    CanApply(plan *LogicalPlan) bool
    Apply(plan *LogicalPlan) (*LogicalPlan, bool)
    Priority() int
}
```

**Core Rules:**

1. **FilterPushdownRule**: Push predicates through joins and projections
1. **ProjectionEliminationRule**: Remove unused columns early
1. **JoinReorderingRule**: Apply join associativity/commutativity
1. **SubqueryDecorrelationRule**: Convert correlated to uncorrelated subqueries
1. **WindowFunctionOptimizationRule**: Optimize window function placement

**Implementation Strategy:**

- Apply rules in priority order until fixed point
- Track rule application to prevent infinite loops
- Use pattern matching for efficient rule selection

**Go-Specific Enhancements:**

- Goroutine-aware cost estimation for parallel operations
- Memory allocation pattern consideration in rule selection
- GC-friendly data structure transformations

### 3.2 Approach 2: Cost-Based with Statistics Collection

**Architecture:**

```go
type CostBasedOptimizer struct {
    statisticsManager *StatisticsManager
    costModel *CostModel
    cardinality *CardinalityEstimator
}

type TableStatistics struct {
    RowCount int64
    ColumnStats map[string]*ColumnStatistics
    Histograms map[string]*Histogram
}

type ColumnStatistics struct {
    NullCount int64
    DistinctCount int64
    MinValue interface{}
    MaxValue interface{}
    MostCommonValues []interface{}
}
```

**Statistics Collection:**

- **Table-level**: Row count, size, modification frequency
- **Column-level**: Cardinality, null percentage, value distribution
- **Index-level**: Selectivity, clustering factor
- **Join-level**: Correlation statistics, foreign key relationships

**Cost Model Components:**

1. **CPU Cost**: Based on operation complexity and data volume
1. **I/O Cost**: Disk/memory access patterns
1. **Memory Cost**: Allocation and GC pressure
1. **Network Cost**: For distributed operations
1. **Goroutine Cost**: Context switching and synchronization overhead

**Cardinality Estimation:**

- Histogram-based estimation for range predicates
- Bloom filters for existence checks
- Hyperloglog for distinct count estimation
- Machine learning models for complex predicates

### 3.3 Approach 3: Hybrid Adaptive Optimization

**Architecture:**

```go
type AdaptiveOptimizer struct {
    ruleOptimizer *RuleBasedOptimizer
    costOptimizer *CostBasedOptimizer
    executionHistory *ExecutionHistory
    adaptationEngine *AdaptationEngine
}

type ExecutionHistory struct {
    queryPlans map[string]*PlanExecution
    mutex sync.RWMutex
}

type PlanExecution struct {
    LogicalPlan *LogicalPlan
    PhysicalPlan *PhysicalPlan
    ActualCost float64
    EstimatedCost float64
    ExecutionTime time.Duration
    ResourceUsage *ResourceUsage
}
```

**Adaptive Strategy:**

1. **Initial Optimization**: Apply rule-based optimization for fast planning
1. **Cost Refinement**: Use statistics for expensive query plans
1. **Runtime Feedback**: Collect actual vs estimated metrics
1. **Plan Adaptation**: Adjust future optimizations based on feedback
1. **Dynamic Re-optimization**: Re-plan long-running queries with updated statistics

**Machine Learning Integration:**

- Plan cost prediction models
- Cardinality estimation neural networks
- Resource usage forecasting
- Anomaly detection for plan regression

## 4. Optimization Rules Design

### 4.1 Filter Pushdown and Predicate Optimization

**FilterPushdownRule Implementation:**

```go
type FilterPushdownRule struct{}

func (r *FilterPushdownRule) Apply(plan *LogicalPlan) (*LogicalPlan, bool) {
    return r.pushFiltersDown(plan, make([]*FilterExpression, 0))
}

func (r *FilterPushdownRule) pushFiltersDown(node *LogicalPlan, filters []*FilterExpression) (*LogicalPlan, bool) {
    switch node.Type {
    case LogicalJoin:
        return r.pushThroughJoin(node, filters)
    case LogicalProjection:
        return r.pushThroughProjection(node, filters)
    case LogicalTableScan:
        return r.applyToTableScan(node, filters)
    }
}
```

**Optimizations:**

- Push filters to both sides of equi-joins
- Duplicate filters for correlated columns
- Convert filters to index lookups when possible
- Combine multiple filters using logical operators

### 4.2 Join Reordering and Selection

**JoinOrderOptimizer Implementation:**

```go
type JoinOrderOptimizer struct {
    statisticsManager *StatisticsManager
    dpTable map[string]*JoinNode
}

func (opt *JoinOrderOptimizer) OptimizeJoinOrder(joins []*JoinNode) *JoinNode {
    // Use dynamic programming with bitsets for efficient enumeration
    return opt.dpOptimize(joins)
}

func (opt *JoinOrderOptimizer) dpOptimize(relations []*JoinNode) *JoinNode {
    n := len(relations)
    dp := make(map[uint64]*JoinNode)
    
    // Initialize single relations
    for i, rel := range relations {
        bitset := uint64(1) << i
        dp[bitset] = rel
    }
    
    // Build join plans bottom-up
    for size := 2; size <= n; size++ {
        opt.enumerateSubsets(dp, relations, size)
    }
    
    return dp[(1<<n)-1]
}
```

**Join Selection Strategies:**

1. **Hash Join**: For equi-joins with good hash distribution
1. **Nested Loop Join**: For small outer relations
1. **Sort-Merge Join**: For sorted inputs or range predicates
1. **Index Nested Loop**: When inner relation has suitable index

### 4.3 Aggregation Pushdown and Optimization

**AggregationPushdownRule:**

```go
type AggregationPushdownRule struct{}

func (r *AggregationPushdownRule) Apply(plan *LogicalPlan) (*LogicalPlan, bool) {
    if agg, ok := plan.(*LogicalAggregation); ok {
        return r.pushAggregation(agg)
    }
    return plan, false
}

func (r *AggregationPushdownRule) pushAggregation(agg *LogicalAggregation) (*LogicalPlan, bool) {
    switch child := agg.Input.(type) {
    case *LogicalJoin:
        return r.pushThroughJoin(agg, child)
    case *LogicalUnion:
        return r.pushThroughUnion(agg, child)
    }
    return agg, false
}
```

**Optimization Techniques:**

- Push COUNT/SUM through UNION operations
- Pre-aggregate before expensive joins
- Use GROUP BY elimination for functional dependencies
- Optimize DISTINCT operations using hash sets

### 4.4 Projection Elimination

**ProjectionEliminationRule:**

```go
type ProjectionEliminationRule struct{}

func (r *ProjectionEliminationRule) Apply(plan *LogicalPlan) (*LogicalPlan, bool) {
    requiredColumns := r.computeRequiredColumns(plan)
    return r.eliminateUnusedProjections(plan, requiredColumns)
}

func (r *ProjectionEliminationRule) computeRequiredColumns(plan *LogicalPlan) *ColumnSet {
    // Traverse plan tree bottom-up to determine required columns
    return r.traverseRequiredColumns(plan, NewColumnSet())
}
```

### 4.5 Subquery Optimization and Decorrelation

**SubqueryDecorrelationRule:**

```go
type SubqueryDecorrelationRule struct{}

func (r *SubqueryDecorrelationRule) Apply(plan *LogicalPlan) (*LogicalPlan, bool) {
    return r.decorrelateSubqueries(plan)
}

func (r *SubqueryDecorrelationRule) decorrelateSubqueries(plan *LogicalPlan) (*LogicalPlan, bool) {
    switch node := plan.(type) {
    case *LogicalFilter:
        return r.decorrelateFilterSubqueries(node)
    case *LogicalProjection:
        return r.decorrelateProjectionSubqueries(node)
    }
    return plan, false
}
```

**Decorrelation Techniques:**

- Convert EXISTS to SEMI-JOIN
- Convert NOT EXISTS to ANTI-JOIN
- Transform scalar subqueries to LEFT OUTER JOIN
- Use window functions for correlated aggregates

### 4.6 Window Function Optimization

**WindowFunctionOptimizationRule:**

```go
type WindowFunctionOptimizationRule struct{}

func (r *WindowFunctionOptimizationRule) Apply(plan *LogicalPlan) (*LogicalPlan, bool) {
    return r.optimizeWindowFunctions(plan)
}

func (r *WindowFunctionOptimizationRule) optimizeWindowFunctions(plan *LogicalPlan) (*LogicalPlan, bool) {
    // Combine window functions with same PARTITION BY/ORDER BY
    // Push window functions closer to data source
    // Use specialized algorithms for common patterns
    return r.combineCompatibleWindows(plan)
}
```

## 5. Statistics Collection and Maintenance

### 5.1 Statistics Architecture

```go
type StatisticsManager struct {
    tableStats map[string]*TableStatistics
    indexStats map[string]*IndexStatistics
    systemStats *SystemStatistics
    updateQueue chan *StatisticsUpdate
    mutex sync.RWMutex
}

type StatisticsCollector struct {
    samplingRate float64
    asyncUpdate bool
    updateInterval time.Duration
}
```

### 5.2 Column Statistics Collection

**Basic Statistics:**

```go
type ColumnStatistics struct {
    NullCount int64
    DistinctCount int64
    MinValue interface{}
    MaxValue interface{}
    AvgLength float64
    MostCommonValues []ValueFrequency
    Histogram *Histogram
}

func (cs *ColumnStatistics) UpdateWithSample(values []interface{}) {
    // Use HyperLogLog for distinct count estimation
    // Maintain reservoir sampling for most common values
    // Update histogram buckets incrementally
}
```

### 5.3 Histogram Generation and Maintenance

**Equi-Width and Equi-Depth Histograms:**

```go
type Histogram struct {
    Type HistogramType
    Buckets []HistogramBucket
    TotalCount int64
}

type HistogramBucket struct {
    LowerBound interface{}
    UpperBound interface{}
    Count int64
    DistinctCount int64
}

func NewEquiDepthHistogram(values []interface{}, bucketCount int) *Histogram {
    // Create buckets with equal number of values
    // Maintain bucket boundaries for range queries
    sort.Slice(values, func(i, j int) bool {
        return compareValues(values[i], values[j]) < 0
    })
    
    bucketSize := len(values) / bucketCount
    buckets := make([]HistogramBucket, bucketCount)
    
    for i := 0; i < bucketCount; i++ {
        start := i * bucketSize
        end := start + bucketSize
        if i == bucketCount-1 {
            end = len(values)
        }
        
        buckets[i] = HistogramBucket{
            LowerBound: values[start],
            UpperBound: values[end-1],
            Count: int64(end - start),
            DistinctCount: countDistinct(values[start:end]),
        }
    }
    
    return &Histogram{Type: EquiDepth, Buckets: buckets}
}
```

### 5.4 Join Selectivity Estimation

**Correlation Statistics:**

```go
type JoinStatistics struct {
    JoinColumns []string
    Selectivity float64
    CorrelationCoeff float64
    ForeignKeyRatio float64
}

func EstimateJoinSelectivity(leftStats, rightStats *TableStatistics, joinKeys []string) float64 {
    // Use containment assumption for foreign key joins
    // Apply independence assumption for uncorrelated columns
    // Adjust for multi-column joins using correlation statistics
    
    selectivity := 1.0
    for _, key := range joinKeys {
        leftCard := leftStats.ColumnStats[key].DistinctCount
        rightCard := rightStats.ColumnStats[key].DistinctCount
        
        // Containment assumption
        keySelectivity := 1.0 / math.Max(float64(leftCard), float64(rightCard))
        selectivity *= keySelectivity
    }
    
    return selectivity
}
```

### 5.5 Cost Model for Go-Specific Operations

**Resource Cost Modeling:**

```go
type CostModel struct {
    CPUCostPerTuple float64
    IOCostPerPage float64
    MemoryCostPerByte float64
    GoroutineCostPerSpawn float64
    ChannelCostPerSend float64
    GCCostPerAlloc float64
}

func (cm *CostModel) EstimateOperatorCost(op *PhysicalOperator, inputCard int64) float64 {
    baseCost := cm.CPUCostPerTuple * float64(inputCard)
    
    switch op.Type {
    case PhysicalHashJoin:
        // Account for hash table build cost + GC pressure
        hashBuildCost := cm.MemoryCostPerByte * float64(op.HashTableSize)
        gcCost := cm.GCCostPerAlloc * float64(inputCard/1000) // Estimate allocations
        return baseCost + hashBuildCost + gcCost
        
    case PhysicalSort:
        // Sort complexity with Go-specific slice growth patterns
        sortCost := baseCost * math.Log2(float64(inputCard))
        sliceGrowthCost := cm.MemoryCostPerByte * float64(inputCard) * 0.5 // Average growth overhead
        return sortCost + sliceGrowthCost
        
    case PhysicalParallelScan:
        // Account for goroutine coordination overhead
        goroutineCount := float64(runtime.NumCPU())
        coordinationCost := cm.GoroutineCostPerSpawn * goroutineCount
        channelCost := cm.ChannelCostPerSend * float64(inputCard/1000) // Batched sends
        return baseCost/goroutineCount + coordinationCost + channelCost
    }
    
    return baseCost
}
```

## 6. Go-Specific Optimization Considerations

### 6.1 Goroutine Coordination Optimization

**Worker Pool Pattern for Query Execution:**

```go
type WorkerPool struct {
    workers []chan WorkItem
    results chan Result
    wg sync.WaitGroup
}

type QueryExecutor struct {
    workerPool *WorkerPool
    maxConcurrency int
}

func (qe *QueryExecutor) ExecuteParallelOperator(op *PhysicalOperator) <-chan *DataChunk {
    resultChan := make(chan *DataChunk, qe.maxConcurrency)
    
    // Estimate optimal goroutine count based on operation type and data size
    goroutineCount := qe.estimateOptimalConcurrency(op)
    
    for i := 0; i < goroutineCount; i++ {
        go qe.executeWorker(op, resultChan)
    }
    
    return resultChan
}

func (qe *QueryExecutor) estimateOptimalConcurrency(op *PhysicalOperator) int {
    // Consider CPU-bound vs I/O-bound operations
    // Account for goroutine creation overhead
    // Balance against GC pressure from increased allocation
    
    baseConcurrency := runtime.NumCPU()
    
    switch op.Type {
    case PhysicalHashJoin:
        // Hash join benefits from parallelism but creates GC pressure
        return int(math.Min(float64(baseConcurrency), float64(op.EstimatedCardinality/10000)))
    case PhysicalTableScan:
        // I/O bound operations can handle more goroutines
        return baseConcurrency * 2
    case PhysicalSort:
        // CPU-bound operations should match CPU count
        return baseConcurrency
    }
    
    return baseConcurrency
}
```

### 6.2 Memory Allocation Pattern Optimization

**Pre-allocation Strategy:**

```go
type DataChunk struct {
    columns []Column
    rowCount int
    capacity int
}

func NewDataChunk(columnCount, estimatedRows int) *DataChunk {
    // Pre-allocate slices to avoid growth overhead
    columns := make([]Column, columnCount)
    for i := range columns {
        columns[i] = NewColumn(estimatedRows)
    }
    
    return &DataChunk{
        columns: columns,
        rowCount: 0,
        capacity: estimatedRows,
    }
}

type Column struct {
    data interface{} // []int64, []string, etc.
    nullMask []bool
    length int
    capacity int
}

func NewColumn(capacity int) Column {
    return Column{
        data: make([]interface{}, 0, capacity),
        nullMask: make([]bool, 0, capacity),
        length: 0,
        capacity: capacity,
    }
}
```

**Object Pooling for Frequent Allocations:**

```go
var (
    dataChunkPool = sync.Pool{
        New: func() interface{} {
            return NewDataChunk(10, 1000) // Default size
        },
    }
    
    columnPool = sync.Pool{
        New: func() interface{} {
            return NewColumn(1000)
        },
    }
)

func GetDataChunk() *DataChunk {
    chunk := dataChunkPool.Get().(*DataChunk)
    chunk.Reset()
    return chunk
}

func PutDataChunk(chunk *DataChunk) {
    if chunk.capacity < 10000 { // Don't pool very large chunks
        dataChunkPool.Put(chunk)
    }
}
```

### 6.3 GC Pressure Minimization

**Stack Allocation Optimization:**

```go
// Prefer stack allocation for small, short-lived objects
func (op *HashJoinOperator) processRow(row *Row) bool {
    // Use value types instead of pointers when possible
    var keyBuffer [8]byte // Stack allocated
    key := op.extractKey(row, keyBuffer[:])
    
    // Avoid escape to heap by not storing pointers to local variables
    return op.hashTable.Contains(key)
}

// Minimize allocations in hot paths
func (op *FilterOperator) evaluatePredicate(row *Row) bool {
    // Reuse evaluation context to avoid allocations
    op.evalContext.Reset()
    return op.predicate.Evaluate(row, &op.evalContext)
}
```

**Batch Processing to Reduce GC Frequency:**

```go
type BatchProcessor struct {
    batchSize int
    buffer []*DataChunk
    bufferIndex int
}

func (bp *BatchProcessor) ProcessChunk(chunk *DataChunk) {
    bp.buffer[bp.bufferIndex] = chunk
    bp.bufferIndex++
    
    if bp.bufferIndex >= bp.batchSize {
        bp.flushBatch()
        bp.bufferIndex = 0
    }
}

func (bp *BatchProcessor) flushBatch() {
    // Process entire batch at once to amortize overhead
    for i := 0; i < bp.bufferIndex; i++ {
        bp.processChunkInternal(bp.buffer[i])
        PutDataChunk(bp.buffer[i]) // Return to pool
        bp.buffer[i] = nil // Clear reference
    }
}
```

## 7. Integration Architecture

### 7.1 SQL Parser Integration

**Parser Output to Optimizer Input:**

```go
type QueryOptimizer interface {
    Optimize(statement *SQLStatement, catalog *Catalog) (*PhysicalPlan, error)
}

type OptimizerImpl struct {
    ruleOptimizer *RuleBasedOptimizer
    costOptimizer *CostBasedOptimizer
    statisticsManager *StatisticsManager
}

func (opt *OptimizerImpl) Optimize(stmt *SQLStatement, catalog *Catalog) (*PhysicalPlan, error) {
    // 1. Create initial logical plan from parsed statement
    logicalPlan, err := opt.createLogicalPlan(stmt, catalog)
    if err != nil {
        return nil, err
    }
    
    // 2. Apply rule-based optimizations
    optimizedLogical, err := opt.ruleOptimizer.Optimize(logicalPlan)
    if err != nil {
        return nil, err
    }
    
    // 3. Apply cost-based optimizations for complex queries
    if opt.shouldUseCostBasedOptimization(optimizedLogical) {
        optimizedLogical, err = opt.costOptimizer.Optimize(optimizedLogical)
        if err != nil {
            return nil, err
        }
    }
    
    // 4. Generate physical plan
    physicalPlan, err := opt.generatePhysicalPlan(optimizedLogical)
    if err != nil {
        return nil, err
    }
    
    return physicalPlan, nil
}
```

### 7.2 Physical Execution Engine Integration

**Operator Interface:**

```go
type PhysicalOperator interface {
    Open() error
    Next() (*DataChunk, error)
    Close() error
    GetEstimatedCost() float64
    GetActualCost() float64
}

type PipelineOperator interface {
    PhysicalOperator
    AddSink(sink PhysicalOperator)
    SetSource(source PhysicalOperator)
}
```

**Pipeline Execution:**

```go
type PipelineExecutor struct {
    operators []PhysicalOperator
    parallelism int
    bufferSize int
}

func (pe *PipelineExecutor) Execute(ctx context.Context) <-chan *DataChunk {
    resultChan := make(chan *DataChunk, pe.bufferSize)
    
    go func() {
        defer close(resultChan)
        
        // Create pipeline stages
        stages := pe.createPipelineStages()
        
        // Execute pipeline with proper back-pressure handling
        pe.executePipeline(ctx, stages, resultChan)
    }()
    
    return resultChan
}
```

### 7.3 Statistics Storage and Updates

**Statistics Persistence:**

```go
type StatisticsPersistence interface {
    SaveStatistics(tableName string, stats *TableStatistics) error
    LoadStatistics(tableName string) (*TableStatistics, error)
    UpdateStatistics(tableName string, updates *StatisticsUpdate) error
}

type FileBasedStatistics struct {
    basePath string
    cache map[string]*TableStatistics
    mutex sync.RWMutex
}

func (fbs *FileBasedStatistics) SaveStatistics(tableName string, stats *TableStatistics) error {
    fbs.mutex.Lock()
    defer fbs.mutex.Unlock()
    
    // Serialize statistics to JSON/binary format
    data, err := json.Marshal(stats)
    if err != nil {
        return err
    }
    
    filename := filepath.Join(fbs.basePath, tableName+".stats")
    err = os.WriteFile(filename, data, 0644)
    if err != nil {
        return err
    }
    
    // Update cache
    fbs.cache[tableName] = stats
    return nil
}
```

**Automatic Statistics Updates:**

```go
type StatisticsUpdater struct {
    persistence StatisticsPersistence
    updateQueue chan *StatisticsUpdate
    batchSize int
    flushInterval time.Duration
}

func (su *StatisticsUpdater) Start(ctx context.Context) {
    ticker := time.NewTicker(su.flushInterval)
    defer ticker.Stop()
    
    batch := make([]*StatisticsUpdate, 0, su.batchSize)
    
    for {
        select {
        case update := <-su.updateQueue:
            batch = append(batch, update)
            if len(batch) >= su.batchSize {
                su.processBatch(batch)
                batch = batch[:0]
            }
            
        case <-ticker.C:
            if len(batch) > 0 {
                su.processBatch(batch)
                batch = batch[:0]
            }
            
        case <-ctx.Done():
            // Flush remaining updates
            if len(batch) > 0 {
                su.processBatch(batch)
            }
            return
        }
    }
}
```

## 8. Performance Monitoring and Tuning

### 8.1 Query Plan Metrics

**Execution Metrics Collection:**

```go
type ExecutionMetrics struct {
    PlanningTime time.Duration
    ExecutionTime time.Duration
    RowsProcessed int64
    BytesProcessed int64
    GoroutinesUsed int
    MemoryAllocated int64
    GCPauses int
    CacheHitRatio float64
}

type MetricsCollector struct {
    metrics map[string]*ExecutionMetrics
    mutex sync.RWMutex
}

func (mc *MetricsCollector) RecordExecution(queryID string, metrics *ExecutionMetrics) {
    mc.mutex.Lock()
    defer mc.mutex.Unlock()
    
    mc.metrics[queryID] = metrics
    
    // Update optimizer feedback
    mc.updateOptimizerFeedback(queryID, metrics)
}
```

### 8.2 Adaptive Optimization Feedback

**Plan Performance Tracking:**

```go
type PlanPerformanceTracker struct {
    planMetrics map[string]*PlanMetrics
    feedbackQueue chan *PlanFeedback
    adaptationRules []AdaptationRule
}

type PlanFeedback struct {
    PlanHash string
    ActualCost float64
    EstimatedCost float64
    ActualCardinality int64
    EstimatedCardinality int64
    ResourceUsage *ResourceUsage
}

func (ppt *PlanPerformanceTracker) UpdatePlanMetrics(feedback *PlanFeedback) {
    // Update cost model parameters based on actual vs estimated costs
    costError := math.Abs(feedback.ActualCost - feedback.EstimatedCost) / feedback.EstimatedCost
    
    if costError > 0.5 { // Significant estimation error
        ppt.adjustCostModel(feedback)
    }
    
    // Update cardinality estimation models
    cardError := math.Abs(float64(feedback.ActualCardinality - feedback.EstimatedCardinality)) / float64(feedback.EstimatedCardinality)
    
    if cardError > 0.3 { // Significant cardinality error
        ppt.adjustCardinalityModel(feedback)
    }
}
```

## 9. Implementation Roadmap

### Phase 1: Core Infrastructure (Months 1-2)

- Implement basic logical and physical operator interfaces
- Create rule-based optimizer framework
- Develop statistics collection foundation
- Build query plan representation structures

### Phase 2: Rule-Based Optimization (Months 3-4)

- Implement core optimization rules (filter pushdown, projection elimination)
- Add join reordering with simple heuristics
- Create subquery decorrelation framework
- Implement basic cost estimation

### Phase 3: Cost-Based Optimization (Months 5-6)

- Build comprehensive statistics collection system
- Implement histogram-based cardinality estimation
- Create Go-specific cost model
- Add dynamic programming join optimization

### Phase 4: Go-Specific Optimizations (Months 7-8)

- Implement goroutine-aware query execution
- Add memory allocation optimization patterns
- Create GC pressure minimization strategies
- Build object pooling and reuse mechanisms

### Phase 5: Adaptive Optimization (Months 9-10)

- Implement execution feedback collection
- Add plan performance tracking
- Create adaptive optimization rules
- Build machine learning integration framework

### Phase 6: Integration and Testing (Months 11-12)

- Integrate with SQL parser and execution engine
- Comprehensive performance testing and benchmarking
- Documentation and optimization tuning guides
- Production readiness assessment

## 10. Success Metrics

### Performance Targets

- **Query Planning Time**: < 10ms for simple queries, < 100ms for complex queries
- **Optimization Effectiveness**: 5x+ performance improvement over unoptimized plans
- **Memory Efficiency**: < 2x memory overhead compared to minimal execution
- **GC Impact**: < 5% of total execution time spent in GC pauses
- **Scalability**: Linear performance scaling with available CPU cores

### Quality Metrics

- **Plan Quality**: 90%+ of generated plans within 20% of optimal cost
- **Cardinality Accuracy**: 80%+ of cardinality estimates within 2x of actual
- **Adaptation Speed**: Significant plan improvements within 10 executions
- **Resource Utilization**: 80%+ CPU utilization during query execution

This comprehensive query optimization architecture provides a solid foundation for building a high-performance, Go-native analytical database that can compete with DuckDB's optimization capabilities while leveraging Go's unique strengths in concurrent programming and cross-platform deployment.
