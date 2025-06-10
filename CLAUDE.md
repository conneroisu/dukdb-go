# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a **pure-Go implementation of DuckDB** project aimed at creating a DuckDB-compatible analytical database without CGO dependencies. The project has completed comprehensive research and architecture design, with the goal of providing a static-binary deployable alternative to the existing CGO-based DuckDB driver.

The existing DuckDB Go driver (https://github.com/marcboeker/go-duckdb) is CGO-based but shares many of the same output interfaces as this project will implement.

## Current Status

The repository has completed the **research and design phase** containing:
- Comprehensive architecture design documents for all major components
- Detailed analysis of Go SQL database driver implementations (see `prompt.md`)
- Implementation recommendations for each subsystem
- GPL v3.0 licensing
- Ready for implementation phase

### Architecture Design Documents

1. **API Architecture** (`API_ARCHITECTURE_DESIGN.md`)
   - Three approaches analyzed: Standard database/sql only, Extended interface, Dual-mode API
   - Recommends: Dual-mode API for maximum compatibility and functionality
   - Detailed compatibility matrix with Go's database/sql interface

2. **SQL Parser** (`sql_parser_architecture_analysis.md`)
   - Approaches: Hand-written recursive descent, Generated parser (Goyacc/ANTLR), Hybrid
   - Recommends: Generated parser using Goyacc for DuckDB SQL dialect compatibility
   - Includes handling for DuckDB-specific extensions

3. **Query Optimization** (`query_optimization_design.md`)
   - Approaches: Rule-based, Cost-based, Hybrid adaptive
   - Recommends: Hybrid approach starting with rule-based, adding cost-based optimization
   - Go-specific optimizations for goroutines and memory management

4. **Execution Engine** (`execution_engine_architectures.md`)
   - Approaches: Volcano-style iterator, Vectorized columnar, Morsel-driven parallelism
   - Recommends: Start with Volcano model, progressively add vectorization
   - Based on DuckDB's vectorized execution model

5. **Storage Architecture** (`storage-architecture-design.md`)
   - Approaches: Native columnar format, Multi-format abstraction, Memory-mapped files
   - Recommends: Hybrid approach combining all three
   - Leverages existing Go libraries for Parquet, CSV, JSON formats

## Common Commands

Since this is a greenfield Go project, standard Go development commands will apply once implementation begins:

### Project Setup (when ready)
```bash
# Initialize Go module (not yet done)
go mod init github.com/username/dukdb-go

# Install dependencies
go mod tidy

# Run the application
go run main.go
```

### Development
```bash
# Run tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Build the project
go build ./...

# Format code
go fmt ./...

# Run linter (requires golangci-lint)
golangci-lint run
```

## Architecture Implementation Plan

Based on the completed research and design documents, the implementation will follow this architecture:

### Core Components (designed and ready for implementation)

1. **API Layer** (`API_ARCHITECTURE_DESIGN.md`)
   - Dual-mode API: Standard `database/sql` interface + extended DuckDB-specific features
   - Type-safe query builders and result helpers
   - Backward compatibility with existing Go database code

2. **SQL Parser** (`sql_parser_architecture_analysis.md`)
   - Goyacc-based parser generator for DuckDB SQL dialect
   - Support for DuckDB extensions: LIST types, STRUCT types, advanced analytics
   - Extensible grammar for future DuckDB compatibility

3. **Query Optimizer** (`query_optimization_design.md`)
   - Hybrid optimizer: rule-based transformations + cost-based decisions
   - Go-specific optimizations for goroutine scheduling and memory allocation
   - Statistics collection and adaptive query optimization

4. **Execution Engine** (`execution_engine_architectures.md`)
   - Start with Volcano-style iterator model for simplicity
   - Progressive enhancement with vectorized operations
   - Parallel execution using Go's concurrency primitives

5. **Storage Layer** (`storage-architecture-design.md`)
   - Multi-format support: Parquet (via parquet-go), CSV, JSON
   - Native columnar format for intermediate results
   - Memory-mapped file support for large datasets

### Design Principles
- **Pure-Go Implementation** - No CGO dependencies for deployment simplicity
- **DuckDB Compatibility** - Compatible SQL dialect and core functionality
- **Progressive Enhancement** - Start simple, add optimizations incrementally
- **Go-Native Design** - Leverage Go's strengths (goroutines, channels, interfaces)
- **Cross-Platform** - Leveraging Go's cross-compilation capabilities

## Research Insights

The `prompt.md` contains comprehensive analysis showing:
- Most modern Go SQL drivers are pure-Go (PostgreSQL, MySQL, SQL Server, etc.)
- DuckDB currently only has a CGO-based driver (`marcboeker/go-duckdb`)
- Pure-Go drivers offer significant deployment advantages (static binaries, no runtime dependencies)
- PostgreSQL wire protocol success demonstrates feasibility of pure-Go database implementations

## Implementation Roadmap

Based on the completed architecture designs, here's the recommended implementation order:

### Phase 1: Foundation
1. **Core Data Types** - Implement DuckDB-compatible types (LIST, STRUCT, etc.)
2. **Basic Storage** - In-memory columnar storage using the hybrid approach from `storage-architecture-design.md`
3. **SQL Parser** - Generate parser using Goyacc as recommended in `sql_parser_architecture_analysis.md`

### Phase 2: Query Processing
1. **Logical Plan** - AST to logical plan transformation
2. **Physical Plan** - Implement Volcano-style operators as per `execution_engine_architectures.md`
3. **Basic Optimizer** - Rule-based optimizations from `query_optimization_design.md`

### Phase 3: Integration
1. **database/sql Driver** - Implement standard interface as per `API_ARCHITECTURE_DESIGN.md`
2. **File Format Support** - Integrate Parquet-go, CSV reader/writer
3. **Basic Analytics** - Window functions, aggregations

### Phase 4: Optimization
1. **Vectorization** - Add vectorized operations to execution engine
2. **Cost-Based Optimizer** - Implement statistics and cost models
3. **Parallel Execution** - Leverage Go concurrency for query parallelism

### Phase 5: DuckDB Compatibility
1. **Extended API** - DuckDB-specific features beyond database/sql
2. **Advanced Analytics** - Full window function support, complex aggregations
3. **Performance Tuning** - Benchmark against DuckDB and optimize

## Key Technical Decisions (from Architecture Documents)

### From Research Phase
- **Parser**: Goyacc over ANTLR for better Go integration and smaller runtime
- **Execution**: Start with iterator model, add vectorization incrementally
- **Storage**: Hybrid approach using existing Go libraries + custom columnar format
- **API**: Dual-mode supporting both standard database/sql and DuckDB extensions
- **Optimization**: Rule-based first, cost-based as enhancement

### Go-Specific Considerations
- Leverage goroutines for parallel query execution
- Use channels for operator communication in execution engine
- Consider GC pressure in vectorized operations
- Memory pooling for columnar data buffers
- Interface-based design for extensibility

## Testing Strategy

The project has a comprehensive testing plan documented in:
- **TESTING_STRATEGY.md** - Overall testing philosophy and approach
- **TEST_IMPLEMENTATION_GUIDE.md** - Detailed code examples and patterns
- **COMPATIBILITY_VALIDATION_FRAMEWORK.md** - CGO vs pure-Go behavioral validation
- **PERFORMANCE_BENCHMARKING_FRAMEWORK.md** - Performance testing and regression detection

### Key Testing Components

1. **SQL Logic Tests** - Adopt DuckDB's SQL Logic Test format for compatibility
2. **Unit Tests** - Table-driven tests for all components (80% coverage minimum)
3. **Compatibility Tests** - Side-by-side comparison with CGO DuckDB
4. **Performance Benchmarks** - TPC-H, TPC-DS, and micro-benchmarks
5. **Fuzzing** - Go native fuzzing for parser and type system robustness

### Testing Commands
```bash
# Run all tests
go test -v ./...

# Run with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Run benchmarks
go test -bench=. -benchmem ./benchmark/...

# Run SQL logic tests
go test -v ./test/sqllogictest/...

# Run compatibility tests (requires CGO DuckDB)
go test -v ./test/compatibility/...
```

## License Considerations

Project uses GPL v3.0, which requires:
- Derivative works to be GPL-licensed
- Source code availability for distributed binaries
- Careful consideration when integrating with non-GPL code

This licensing choice suggests the project aims to remain open-source and may influence library selection during implementation.

## Purego Integration Strategy

The project will use **purego** (github.com/ebitengine/purego) to enable CGO-free calling of C functions, particularly for interfacing with existing DuckDB C libraries during the transition phase. This allows:

### Key Benefits of Purego
- **Cross-compilation** without C toolchains (e.g., `GOOS=windows go build`)
- **Simplified builds** - no CGO complexity or C dependencies
- **Dynamic library loading** - interface with system libraries at runtime
- **Production proven** - used by Ebitengine game engine

### Purego Usage Patterns
```go
// Load DuckDB C library dynamically
lib, err := purego.Dlopen("libduckdb.so", purego.RTLD_NOW|purego.RTLD_GLOBAL)
defer purego.Dlclose(lib)

// Register C functions
var duckdb_open func(path string, db *uintptr) int
purego.RegisterLibFunc(&duckdb_open, lib, "duckdb_open")

// Call C functions from pure Go
var db uintptr
result := duckdb_open(":memory:", &db)
```

### Purego Limitations to Consider
- **Float support** only on 64-bit platforms (amd64, arm64)
- **Struct alignment** must be manually managed
- **Callback limitations** on Linux platforms
- **Maximum 2000 callbacks** per process (never freed)

### Implementation Approach
1. Use purego for initial compatibility layer with DuckDB C API
2. Progressively replace C function calls with pure-Go implementations
3. Maintain purego wrapper for testing against reference implementation
4. Eventually remove purego dependency once fully implemented in Go

## Contributing Guidelines

When working on this project:
1. Review the relevant architecture document before implementing a component
2. Follow the recommended approach from the design documents
3. Start simple and iterate - avoid premature optimization
4. Maintain compatibility with Go's database/sql interface
5. Write comprehensive tests for all new functionality
6. Document any deviations from the architecture with justification
7. Consider purego for any C library integration needs
8. Ensure all code works without CGO (`CGO_ENABLED=0`)
