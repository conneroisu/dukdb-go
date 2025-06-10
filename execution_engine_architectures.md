# DukDB-Go Execution Engine Architectures

## Executive Summary

This document analyzes three distinct execution engine architectures for DukDB-Go, a pure-Go implementation of DuckDB. Each architecture leverages different approaches to query execution while optimizing for analytical workloads and Go's concurrency primitives.

Based on DuckDB's proven vectorized execution model and morsel-driven parallelism, we propose three architectures that balance performance, complexity, and Go-idiomatic design.

## DuckDB Execution Engine Analysis

### Core Principles from DuckDB

1. **Vectorized Processing**: Operations process batches (vectors) of 2048 tuples rather than individual rows
1. **Columnar Storage**: Data stored column-wise for cache efficiency and SIMD operations
1. **Pipeline-Based Execution**: Operators connected in pipelines with minimal materialization
1. **Morsel-Driven Parallelism**: Work distributed in small fragments (morsels) to worker threads
1. **Multiple Vector Formats**: Flat, constant, dictionary, and sequence vectors for compression

### DuckDB's Physical Operators

- **Source Operators**: Table scans, index scans, constant values
- **Transform Operators**: Filter, projection, expression evaluation
- **Aggregate Operators**: Hash-based grouping, window functions
- **Join Operators**: Hash joins, nested loop joins, merge joins
- **Sink Operators**: Result materialization, sorting, top-N

### Performance Characteristics

- Default vector size: 2048 tuples
- Pipeline breakers: Aggregations, sorts, joins (build phase)
- Radix partitioning for parallel aggregations
- Work-stealing task scheduler
- Cache-conscious data structures

______________________________________________________________________

## Architecture 1: Volcano-Style Iterator Model

### Overview

A pull-based execution model where each operator implements a `Next()` method that returns the next batch of results. This model is simple to implement and reason about, making it ideal for initial development phases.

### Design Structure

```go
type PhysicalOperator interface {
    Next(ctx context.Context) (*DataChunk, error)
    Children() []PhysicalOperator
    Schema() *Schema
    Reset() error
    Close() error
}

type DataChunk struct {
    Vectors  []*Vector
    Size     int
    Schema   *Schema
}

type Vector struct {
    Type      LogicalType
    Data      interface{} // Actual data storage
    Validity  *ValidityMask
    Format    VectorFormat
}
```

### Execution Pipeline

```go
type ExecutionContext struct {
    TaskScheduler *TaskScheduler
    BufferManager *BufferManager
    Statistics    *QueryStats
}

type VolcanoExecutor struct {
    root     PhysicalOperator
    context  *ExecutionContext
    chunks   chan *DataChunk
}

func (ve *VolcanoExecutor) Execute() error {
    for {
        chunk, err := ve.root.Next(ve.context)
        if err != nil {
            return err
        }
        if chunk == nil {
            break // End of data
        }
        
        select {
        case ve.chunks <- chunk:
        case <-ve.context.Done():
            return ve.context.Err()
        }
    }
    return nil
}
```

### Physical Operators Implementation

```go
// Table scan operator
type SeqScanOperator struct {
    table       *Table
    projection  []int
    filter      Expression
    current     int64
    chunkSize   int
}

func (scan *SeqScanOperator) Next(ctx context.Context) (*DataChunk, error) {
    if scan.current >= scan.table.RowCount() {
        return nil, nil
    }
    
    chunk := scan.table.GetChunk(scan.current, scan.chunkSize)
    scan.current += int64(scan.chunkSize)
    
    // Apply projection and filters
    if scan.filter != nil {
        chunk = ApplyFilter(chunk, scan.filter)
    }
    if scan.projection != nil {
        chunk = ApplyProjection(chunk, scan.projection)
    }
    
    return chunk, nil
}

// Hash aggregation operator
type HashAggregateOperator struct {
    child       PhysicalOperator
    groups      []Expression
    aggregates  []AggregateFunction
    hashTable   *AggregateHashTable
    finalized   bool
    iterator    *HashTableIterator
}

func (agg *HashAggregateOperator) Next(ctx context.Context) (*DataChunk, error) {
    // Build phase
    if !agg.finalized {
        for {
            chunk, err := agg.child.Next(ctx)
            if err != nil {
                return nil, err
            }
            if chunk == nil {
                break
            }
            agg.hashTable.Aggregate(chunk, agg.groups, agg.aggregates)
        }
        agg.finalized = true
        agg.iterator = agg.hashTable.Iterator()
    }
    
    // Probe phase
    return agg.iterator.Next()
}
```

### Memory Management

```go
type ChunkAllocator struct {
    pools     map[int]*sync.Pool // Size-based pools
    maxSize   int
    allocated int64
}

func (ca *ChunkAllocator) AllocateChunk(size int) *DataChunk {
    pool := ca.pools[size]
    if pool == nil {
        pool = &sync.Pool{
            New: func() interface{} {
                return &DataChunk{
                    Vectors: make([]*Vector, 0),
                    Size:    0,
                }
            },
        }
        ca.pools[size] = pool
    }
    
    chunk := pool.Get().(*DataChunk)
    atomic.AddInt64(&ca.allocated, int64(size))
    return chunk
}
```

### Performance Analysis

**Strengths:**

- Simple to implement and debug
- Natural backpressure mechanism
- Low memory overhead
- Easy to add new operators

**Weaknesses:**

- Sequential execution limits parallelism
- Function call overhead per chunk
- Limited CPU cache utilization
- Difficult to pipeline efficiently

**Memory Efficiency:** ⭐⭐⭐⭐ (4/5)

- Minimal intermediate materialization
- Controlled memory usage through pull-based execution

**CPU Cache Optimization:** ⭐⭐ (2/5)

- Limited data locality due to function calls
- No prefetching opportunities

**Parallelization:** ⭐⭐ (2/5)

- Requires exchange operators for parallelism
- Complex to implement parallel aggregations

**Go Integration:** ⭐⭐⭐⭐⭐ (5/5)

- Natural fit with Go's interfaces
- Easy error handling with standard patterns

______________________________________________________________________

## Architecture 2: Vectorized Columnar Execution (DuckDB-Style)

### Overview

A push-based execution model with vectorized operations that process data in columnar batches. This architecture closely mirrors DuckDB's approach while adapting to Go's type system and memory management.

### Design Structure

```go
type VectorizedOperator interface {
    Execute(input *DataChunk, output *DataChunk) error
    GetCardinality(input *DataChunk) int
    RequiresFullChunk() bool
    Schema() *Schema
}

type Pipeline struct {
    source     SourceOperator
    operators  []VectorizedOperator
    sink       SinkOperator
    buffers    []*DataChunk
}

type VectorFormat int
const (
    FlatVector VectorFormat = iota
    ConstantVector
    DictionaryVector
    SequenceVector
)

type Vector struct {
    logicalType LogicalType
    format      VectorFormat
    size        int
    
    // Data storage - varies by format
    data        unsafe.Pointer
    auxiliary   unsafe.Pointer  // For dictionary indices, string heap, etc.
    validity    *ValidityMask
}
```

### Vectorized Operations Engine

```go
type VectorizedEngine struct {
    pipelines    []*Pipeline
    scheduler    *TaskScheduler
    vectorSize   int
    allocator    *VectorAllocator
}

func (ve *VectorizedEngine) Execute(plan *QueryPlan) (*ResultSet, error) {
    pipelines := ve.buildPipelines(plan)
    
    var wg sync.WaitGroup
    errors := make(chan error, len(pipelines))
    
    for _, pipeline := range pipelines {
        wg.Add(1)
        go func(p *Pipeline) {
            defer wg.Done()
            if err := ve.executePipeline(p); err != nil {
                errors <- err
            }
        }(pipeline)
    }
    
    wg.Wait()
    close(errors)
    
    if err := <-errors; err != nil {
        return nil, err
    }
    
    return ve.collectResults(pipelines)
}

func (ve *VectorizedEngine) executePipeline(pipeline *Pipeline) error {
    inputChunk := ve.allocator.AllocateChunk(ve.vectorSize)
    outputChunk := ve.allocator.AllocateChunk(ve.vectorSize)
    
    defer func() {
        ve.allocator.ReleaseChunk(inputChunk)
        ve.allocator.ReleaseChunk(outputChunk)
    }()
    
    for {
        // Get data from source
        err := pipeline.source.GetChunk(inputChunk)
        if err != nil {
            return err
        }
        if inputChunk.Size == 0 {
            break // No more data
        }
        
        // Process through pipeline
        currentInput := inputChunk
        for _, op := range pipeline.operators {
            outputChunk.Reset()
            if err := op.Execute(currentInput, outputChunk); err != nil {
                return err
            }
            currentInput, outputChunk = outputChunk, currentInput
        }
        
        // Send to sink
        if err := pipeline.sink.Consume(currentInput); err != nil {
            return err
        }
    }
    
    return nil
}
```

### Vectorized Operators

```go
// Vectorized filter operation
type FilterOperator struct {
    predicate    Expression
    selectVector []int32
}

func (f *FilterOperator) Execute(input, output *DataChunk) error {
    // Evaluate predicate to get selection vector
    selectionCount := f.evaluatePredicate(input, f.selectVector)
    
    // Apply selection to all vectors
    for i, vector := range input.Vectors {
        output.Vectors[i] = f.applySelection(vector, f.selectVector, selectionCount)
    }
    
    output.Size = selectionCount
    return nil
}

func (f *FilterOperator) evaluatePredicate(chunk *DataChunk, selection []int32) int {
    // Vectorized predicate evaluation
    count := 0
    switch pred := f.predicate.(type) {
    case *ComparisonExpression:
        count = f.evaluateComparison(pred, chunk, selection)
    case *ConjunctionExpression:
        count = f.evaluateConjunction(pred, chunk, selection)
    }
    return count
}

// Vectorized aggregation with SIMD-style operations
type VectorizedAggregator struct {
    hashTable    *VectorizedHashTable
    groupCols    []int
    aggCols      []int
    aggFunctions []AggregateFunction
}

func (va *VectorizedAggregator) Execute(input, output *DataChunk) error {
    // Compute hash values for all groups at once
    hashes := va.computeHashes(input)
    
    // Find or create groups in hash table
    addresses := va.hashTable.FindOrCreateGroups(input, hashes, va.groupCols)
    
    // Apply aggregates to all matching groups
    for i, aggFunc := range va.aggFunctions {
        aggFunc.Update(addresses, input.Vectors[va.aggCols[i]])
    }
    
    return nil
}
```

### SIMD-Inspired Vector Operations

```go
// Vector arithmetic operations
func AddVectors(left, right, result *Vector) {
    switch left.logicalType {
    case INTEGER:
        addInt32Vectors(
            (*[maxArraySize]int32)(left.data),
            (*[maxArraySize]int32)(right.data),
            (*[maxArraySize]int32)(result.data),
            left.size,
        )
    case BIGINT:
        addInt64Vectors(
            (*[maxArraySize]int64)(left.data),
            (*[maxArraySize]int64)(right.data),
            (*[maxArraySize]int64)(result.data),
            left.size,
        )
    }
}

func addInt32Vectors(left, right, result *[maxArraySize]int32, size int) {
    // Unrolled loop for better performance
    i := 0
    for i < size-7 {
        result[i] = left[i] + right[i]
        result[i+1] = left[i+1] + right[i+1]
        result[i+2] = left[i+2] + right[i+2]
        result[i+3] = left[i+3] + right[i+3]
        result[i+4] = left[i+4] + right[i+4]
        result[i+5] = left[i+5] + right[i+5]
        result[i+6] = left[i+6] + right[i+6]
        result[i+7] = left[i+7] + right[i+7]
        i += 8
    }
    for i < size {
        result[i] = left[i] + right[i]
        i++
    }
}
```

### Performance Analysis

**Strengths:**

- Excellent CPU cache utilization
- High throughput for analytical queries
- Efficient memory usage with columnar layout
- Good SIMD potential

**Weaknesses:**

- Complex implementation
- Higher memory allocation overhead
- Difficult to handle variable-width data
- Limited flexibility for complex operators

**Memory Efficiency:** ⭐⭐⭐ (3/5)

- Columnar layout reduces memory bandwidth
- Higher allocation overhead for vectors

**CPU Cache Optimization:** ⭐⭐⭐⭐⭐ (5/5)

- Excellent data locality
- Prefetching opportunities
- SIMD-friendly operations

**Parallelization:** ⭐⭐⭐ (3/5)

- Good for independent pipelines
- Complex parallel aggregations

**Go Integration:** ⭐⭐⭐ (3/5)

- Requires unsafe operations
- Memory management complexity

______________________________________________________________________

## Architecture 3: Morsel-Driven Parallelism with Work-Stealing

### Overview

A hybrid approach combining Go's goroutine work-stealing scheduler with morsel-driven parallelism. This architecture divides work into small, independent morsels that can be processed by any available goroutine.

### Design Structure

```go
type Morsel struct {
    ID       int64
    StartRow int64
    EndRow   int64
    TableID  int
    Data     *DataChunk
}

type MorselTask struct {
    Morsel    *Morsel
    Pipeline  *Pipeline
    Operators []MorselOperator
    Output    chan<- *DataChunk
}

type MorselOperator interface {
    ProcessMorsel(morsel *Morsel, output chan<- *DataChunk) error
    CanProcessInParallel() bool
    RequiresGlobalState() bool
}

type WorkStealingExecutor struct {
    workers      int
    morsels      chan *MorselTask
    results      chan *DataChunk
    globalState  *GlobalExecutionState
    scheduler    *MorselScheduler
}
```

### Morsel Scheduler

```go
type MorselScheduler struct {
    queues      []*MorselQueue  // Per-worker queues
    globalQueue *MorselQueue    // Global work queue
    workers     []*MorselWorker
    stealing    sync.Map        // Track work stealing attempts
}

type MorselQueue struct {
    morsels []MorselTask
    mutex   sync.RWMutex
    head    int
    tail    int
}

type MorselWorker struct {
    id       int
    executor *WorkStealingExecutor
    queue    *MorselQueue
    stats    *WorkerStats
}

func (mw *MorselWorker) Run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        default:
            task := mw.getWork()
            if task == nil {
                // Try to steal work
                task = mw.stealWork()
            }
            if task != nil {
                mw.processTask(task)
            } else {
                // Brief pause to avoid busy waiting
                time.Sleep(time.Microsecond * 10)
            }
        }
    }
}

func (mw *MorselWorker) stealWork() *MorselTask {
    // Try to steal from other workers (random order)
    workers := rand.Perm(len(mw.executor.workers))
    
    for _, workerID := range workers {
        if workerID == mw.id {
            continue
        }
        
        victim := mw.executor.workers[workerID]
        if task := victim.queue.StealHalf(); task != nil {
            atomic.AddInt64(&mw.stats.StolenTasks, 1)
            return task
        }
    }
    
    // Try global queue
    return mw.executor.scheduler.globalQueue.Pop()
}
```

### Pipeline with Morsel Support

```go
type MorselPipeline struct {
    stages        []MorselStage
    morselsSize   int64
    parallelism   int
    bufferManager *MorselBufferManager
}

type MorselStage struct {
    operator      MorselOperator
    inputBuffer   chan *Morsel
    outputBuffer  chan *Morsel
    state         *StageState
}

func (mp *MorselPipeline) Execute(table *Table) error {
    // Generate morsels from input
    morsels := mp.generateMorsels(table)
    
    var wg sync.WaitGroup
    
    // Start workers for each stage
    for i, stage := range mp.stages {
        for j := 0; j < mp.parallelism; j++ {
            wg.Add(1)
            go func(stg *MorselStage, workerID int) {
                defer wg.Done()
                mp.runStageWorker(stg, workerID)
            }(stage, j)
        }
    }
    
    // Feed morsels to first stage
    go func() {
        for _, morsel := range morsels {
            mp.stages[0].inputBuffer <- morsel
        }
        close(mp.stages[0].inputBuffer)
    }()
    
    wg.Wait()
    return nil
}

func (mp *MorselPipeline) generateMorsels(table *Table) []*Morsel {
    rowCount := table.RowCount()
    morsels := make([]*Morsel, 0, (rowCount/mp.morselsSize)+1)
    
    for start := int64(0); start < rowCount; start += mp.morselsSize {
        end := start + mp.morselsSize
        if end > rowCount {
            end = rowCount
        }
        
        morsels = append(morsels, &Morsel{
            ID:       int64(len(morsels)),
            StartRow: start,
            EndRow:   end,
            TableID:  table.ID,
        })
    }
    
    return morsels
}
```

### Parallel Aggregation with Morsels

```go
type MorselAggregator struct {
    localTables   map[int]*LocalHashTable  // Per-worker hash tables
    finalTable    *GlobalHashTable
    partitioner   *RadixPartitioner
    groupColumns  []int
    aggregates    []AggregateFunction
}

func (ma *MorselAggregator) ProcessMorsel(morsel *Morsel, output chan<- *DataChunk) error {
    workerID := getWorkerID()
    localTable := ma.getLocalTable(workerID)
    
    // Process morsel into local hash table
    chunk := morsel.Data
    for row := 0; row < chunk.Size; row++ {
        // Extract group key
        groupKey := ma.extractGroupKey(chunk, row)
        
        // Get or create aggregate state
        aggState := localTable.GetOrCreate(groupKey)
        
        // Update aggregates
        for i, agg := range ma.aggregates {
            value := chunk.GetValue(ma.aggregates[i].Column, row)
            agg.Update(aggState, value)
        }
    }
    
    return nil
}

func (ma *MorselAggregator) Finalize() *DataChunk {
    // Merge all local hash tables
    mergedTable := ma.finalTable
    
    for _, localTable := range ma.localTables {
        ma.mergeIntoFinal(localTable, mergedTable)
    }
    
    return mergedTable.ToDataChunk()
}

func (ma *MorselAggregator) mergeIntoFinal(local *LocalHashTable, final *GlobalHashTable) {
    local.ForEach(func(key GroupKey, state *AggregateState) {
        finalState := final.GetOrCreate(key)
        ma.combineStates(finalState, state)
    })
}
```

### Integration with Go's Runtime

```go
type GoSchedulerIntegration struct {
    gomaxprocs    int
    workers       []*MorselWorker
    workQueues    []chan *MorselTask
    globalWork    chan *MorselTask
    statistics    *SchedulerStats
}

func NewGoIntegratedExecutor() *GoSchedulerIntegration {
    numCPU := runtime.GOMAXPROCS(0)
    
    gsi := &GoSchedulerIntegration{
        gomaxprocs: numCPU,
        workQueues: make([]chan *MorselTask, numCPU),
        globalWork: make(chan *MorselTask, numCPU*16), // Buffered global queue
        statistics: &SchedulerStats{},
    }
    
    // Create per-P queues aligned with Go's scheduler
    for i := 0; i < numCPU; i++ {
        gsi.workQueues[i] = make(chan *MorselTask, 256)
        gsi.workers[i] = &MorselWorker{
            id:     i,
            queue:  gsi.workQueues[i],
            stats:  &WorkerStats{},
        }
    }
    
    return gsi
}

func (gsi *GoSchedulerIntegration) ScheduleQuery(query *QueryPlan) {
    morsels := gsi.generateMorsels(query)
    
    // Distribute morsels across local queues
    for i, morsel := range morsels {
        workerID := i % gsi.gomaxprocs
        
        select {
        case gsi.workQueues[workerID] <- morsel:
        default:
            // Local queue full, send to global
            gsi.globalWork <- morsel
        }
    }
    
    // Start workers
    var wg sync.WaitGroup
    for i := 0; i < gsi.gomaxprocs; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            gsi.runWorker(workerID)
        }(i)
    }
    
    wg.Wait()
}

func (gsi *GoSchedulerIntegration) runWorker(workerID int) {
    localQueue := gsi.workQueues[workerID]
    
    for {
        select {
        case task := <-localQueue:
            if task != nil {
                gsi.processTask(task, workerID)
            }
            
        case task := <-gsi.globalWork:
            if task != nil {
                gsi.processTask(task, workerID)
            }
            
        default:
            // Try to steal work from other queues
            stolen := gsi.stealWork(workerID)
            if stolen == nil {
                return // No more work available
            }
            gsi.processTask(stolen, workerID)
        }
    }
}
```

### Performance Analysis

**Strengths:**

- Excellent parallelization across all CPU cores
- Dynamic load balancing through work stealing
- Natural integration with Go's runtime
- Fault tolerance through isolated morsels

**Weaknesses:**

- Complex coordination between workers
- Potential for load imbalance with variable morsel sizes
- Overhead from work stealing mechanism
- Synchronization costs for global state

**Memory Efficiency:** ⭐⭐⭐ (3/5)

- Good locality within morsels
- Some overhead from multiple worker states

**CPU Cache Optimization:** ⭐⭐⭐⭐ (4/5)

- Good cache utilization within morsels
- Some cache misses from work stealing

**Parallelization:** ⭐⭐⭐⭐⭐ (5/5)

- Excellent scalability across cores
- Dynamic load balancing
- Efficient work distribution

**Go Integration:** ⭐⭐⭐⭐⭐ (5/5)

- Natural fit with goroutine model
- Leverages Go's work-stealing scheduler
- Excellent error handling and cancellation

______________________________________________________________________

## Physical Operator Tree Structure

### Common Operator Interface

```go
type PhysicalOperator interface {
    // Execution
    Execute(ctx context.Context, input ExecutionInput) (ExecutionOutput, error)
    
    // Schema and planning
    Schema() *Schema
    Children() []PhysicalOperator
    SetChildren(children []PhysicalOperator)
    
    // Resource management  
    EstimateMemory() int64
    EstimateCost() float64
    
    // Parallelization support
    SupportsParallelism() bool
    GetDOP() int // Degree of parallelism
    
    // State management
    Reset() error
    Close() error
}

type ExecutionInput struct {
    Chunks   []*DataChunk
    Context  *ExecutionContext
    WorkerID int
}

type ExecutionOutput struct {
    Chunks []*DataChunk
    Stats  *OperatorStats
}
```

### Operator Categories

```go
// Source operators - generate data
type SourceOperator interface {
    PhysicalOperator
    GetCardinality() int64
    SupportsProjection() bool
    SupportsPredicate() bool
}

// Transform operators - modify data without changing cardinality significantly  
type TransformOperator interface {
    PhysicalOperator
    IsStreaming() bool // Can process without buffering
}

// Blocking operators - require all input before producing output
type BlockingOperator interface {
    PhysicalOperator
    RequiresFullInput() bool
    GetMemoryRequirement() int64
}

// Sink operators - consume final results
type SinkOperator interface {
    PhysicalOperator
    Consume(chunk *DataChunk) error
    GetResults() *ResultSet
}
```

### Specific Operator Implementations

```go
// Table scan with predicate pushdown
type TableScanOperator struct {
    table       *Table
    projection  []int
    filters     []Expression
    statistics  *TableStatistics
    chunkSize   int
    parallel    bool
}

// Hash join operator
type HashJoinOperator struct {
    left        PhysicalOperator
    right       PhysicalOperator
    joinType    JoinType
    conditions  []JoinCondition
    hashTable   *JoinHashTable
    
    // Parallelization support
    partitions  int
    buildDone   chan struct{}
}

// Sort operator with external sorting
type SortOperator struct {
    child       PhysicalOperator
    sortKeys    []SortKey
    
    // External sorting support
    maxMemory   int64
    tempFiles   []*TempFile
    merger      *ExternalMerger
}

// Window function operator
type WindowOperator struct {
    child         PhysicalOperator
    partitionCols []int
    orderCols     []SortKey
    functions     []WindowFunction
    
    // Streaming window support
    buffer        *WindowBuffer
    currentFrame  *WindowFrame
}
```

______________________________________________________________________

## Execution Pipeline and Data Flow

### Pipeline Construction

```go
type PipelineBuilder struct {
    operators []PhysicalOperator
    breaks    []PipelineBreak
}

type PipelineBreak struct {
    Operator PhysicalOperator
    Reason   BreakReason
}

type BreakReason int
const (
    MaterializationBreak BreakReason = iota
    ParallelismBreak
    OrderingBreak
    MemoryBreak
)

func (pb *PipelineBuilder) Build(root PhysicalOperator) []*Pipeline {
    pipelines := make([]*Pipeline, 0)
    currentPipeline := &Pipeline{}
    
    pb.traverseOperators(root, func(op PhysicalOperator) {
        if pb.isPipelineBreaker(op) {
            // Finish current pipeline
            pipelines = append(pipelines, currentPipeline)
            currentPipeline = &Pipeline{}
        }
        currentPipeline.AddOperator(op)
    })
    
    if len(currentPipeline.operators) > 0 {
        pipelines = append(pipelines, currentPipeline)
    }
    
    return pipelines
}

func (pb *PipelineBuilder) isPipelineBreaker(op PhysicalOperator) bool {
    switch op.(type) {
    case *HashJoinOperator:
        return true // Build phase needs materialization
    case *SortOperator:
        return true // Sorting requires all input
    case *HashAggregateOperator:
        return true // Aggregation is blocking
    case *WindowOperator:
        return op.(*WindowOperator).requiresOrdering()
    default:
        return false
    }
}
```

### Data Flow Control

```go
type DataFlowController struct {
    pipelines     []*Pipeline
    dependencies  map[*Pipeline][]*Pipeline
    scheduler     *PipelineScheduler
    backpressure  *BackpressureController
}

type BackpressureController struct {
    bufferSizes   map[*Pipeline]int
    maxBuffer     int
    spillToDisk   bool
    pressureLevel float64
}

func (dfc *DataFlowController) Execute(ctx context.Context) error {
    // Topological sort of pipelines based on dependencies
    ordered := dfc.topologicalSort()
    
    var wg sync.WaitGroup
    errors := make(chan error, len(ordered))
    
    for _, pipeline := range ordered {
        // Wait for dependencies
        for _, dep := range dfc.dependencies[pipeline] {
            dep.Wait()
        }
        
        wg.Add(1)
        go func(p *Pipeline) {
            defer wg.Done()
            defer p.Signal() // Signal completion to dependents
            
            if err := dfc.executePipeline(ctx, p); err != nil {
                errors <- err
                return
            }
        }(pipeline)
    }
    
    wg.Wait()
    close(errors)
    
    return <-errors
}

func (dfc *DataFlowController) executePipeline(ctx context.Context, pipeline *Pipeline) error {
    // Apply backpressure control
    if dfc.backpressure.shouldThrottle(pipeline) {
        dfc.backpressure.throttle(pipeline)
    }
    
    // Execute based on chosen architecture
    switch dfc.scheduler.executionModel {
    case VolcanoModel:
        return dfc.executeVolcano(ctx, pipeline)
    case VectorizedModel:
        return dfc.executeVectorized(ctx, pipeline)
    case MorselDrivenModel:
        return dfc.executeMorselDriven(ctx, pipeline)
    }
    
    return nil
}
```

### Memory and Resource Management

```go
type ResourceManager struct {
    memoryLimit   int64
    memoryUsed    int64
    spillManager  *SpillManager
    bufferPool    *BufferPool
    
    // Per-operator resource tracking
    operatorLimits map[PhysicalOperator]int64
    operatorUsage  map[PhysicalOperator]int64
}

type SpillManager struct {
    tempDir       string
    maxMemoryPct  float64
    spillThreshold int64
    spilledData   map[string]*SpilledPartition
}

func (rm *ResourceManager) AllocateMemory(op PhysicalOperator, size int64) error {
    rm.mutex.Lock()
    defer rm.mutex.Unlock()
    
    if rm.memoryUsed + size > rm.memoryLimit {
        // Try to spill some data
        if err := rm.spillManager.SpillLeastUsed(size); err != nil {
            return fmt.Errorf("out of memory and cannot spill: %w", err)
        }
    }
    
    rm.memoryUsed += size
    rm.operatorUsage[op] += size
    return nil
}

func (sm *SpillManager) SpillLeastUsed(requiredSize int64) error {
    // Find operators that can spill data
    candidates := sm.getSpillCandidates()
    
    sort.Slice(candidates, func(i, j int) bool {
        return candidates[i].lastAccessed.Before(candidates[j].lastAccessed)
    })
    
    spilledSize := int64(0)
    for _, candidate := range candidates {
        if spilledSize >= requiredSize {
            break
        }
        
        size, err := candidate.Spill()
        if err != nil {
            continue
        }
        spilledSize += size
    }
    
    if spilledSize < requiredSize {
        return fmt.Errorf("insufficient spillable memory")
    }
    
    return nil
}
```

______________________________________________________________________

## Recommendation and Implementation Roadmap

### Recommended Approach: Hybrid Architecture

Based on the analysis, I recommend implementing a **hybrid approach** that combines elements from all three architectures:

#### Phase 1: Volcano Foundation (Months 1-3)

- Implement the basic Volcano-style iterator model
- Establish operator interfaces and basic execution framework
- Focus on correctness and completeness of SQL operations
- Build comprehensive test suite

#### Phase 2: Vectorized Operations (Months 4-6)

- Add vectorized processing within operators
- Implement columnar data structures and vector operations
- Optimize critical operators (scans, aggregations, joins)
- Maintain Volcano interfaces for compatibility

#### Phase 3: Morsel-Driven Parallelism (Months 7-9)

- Implement morsel generation and work-stealing scheduler
- Add parallel execution for independent operations
- Integrate with Go's runtime scheduler
- Optimize for multi-core performance

#### Phase 4: Advanced Optimizations (Months 10-12)

- External sorting and spilling mechanisms
- Advanced join algorithms (radix hash join)
- Query plan optimization and cost-based decisions
- Performance tuning and benchmarking

### Key Implementation Principles

1. **Start Simple**: Begin with Volcano model for rapid development
1. **Incremental Optimization**: Add vectorization and parallelism gradually
1. **Go-Idiomatic Design**: Leverage Go's strengths (goroutines, channels, interfaces)
1. **Memory Safety**: Minimize unsafe operations, prefer type safety
1. **Extensive Testing**: Build comprehensive test suite from day one
1. **Performance Monitoring**: Instrument for profiling and optimization

### Success Metrics

- **Correctness**: Pass TPC-H and TPC-DS query compatibility tests
- **Performance**: Achieve 50-70% of CGO DuckDB performance on analytical workloads
- **Scalability**: Linear scalability up to available CPU cores
- **Memory Efficiency**: Handle datasets larger than available RAM
- **Deployment**: Single binary deployment with no external dependencies

This hybrid approach balances implementation complexity with performance goals while leveraging Go's unique strengths in concurrent programming and deployment simplicity.
