# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a **pure-Go implementation of DuckDB** project aimed at creating a DuckDB-compatible analytical database without CGO dependencies. The project is currently in the research and planning phase, with the goal of providing a static-binary deployable alternative to the existing CGO-based DuckDB driver.
https://github.com/marcboeker/go-duckdb is CGO-based, but shares many of the same output interfaces as this project.
## Current Status

The repository is in **initial development phase** containing:
- Research documentation analyzing Go SQL database driver implementations
- Project planning and architecture considerations
- GPL v3.0 licensing
- No implementation code yet

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

## Architecture Goals

Based on the research in `prompt.md`, this project aims to implement:

### Core Components (planned)
1. **SQL Parser** - DuckDB-compatible SQL dialect parser
2. **Query Planner** - Query optimization and execution planning
3. **Execution Engine** - Vectorized query processing engine
4. **Storage Layer** - File format handling (Parquet, CSV, JSON compatibility)
5. **Memory Management** - Columnar data structures optimized for analytics
6. **API Layer** - Go `database/sql` interface compatibility

### Design Principles
- **Pure-Go Implementation** - No CGO dependencies for deployment simplicity
- **DuckDB Compatibility** - Compatible SQL dialect and functionality
- **Analytical Workload Focus** - Optimized for OLAP queries and data analytics
- **Cross-Platform** - Leveraging Go's cross-compilation capabilities

## Research Insights

The `prompt.md` contains comprehensive analysis showing:
- Most modern Go SQL drivers are pure-Go (PostgreSQL, MySQL, SQL Server, etc.)
- DuckDB currently only has a CGO-based driver (`marcboeker/go-duckdb`)
- Pure-Go drivers offer significant deployment advantages (static binaries, no runtime dependencies)
- PostgreSQL wire protocol success demonstrates feasibility of pure-Go database implementations

## Development Approach

When implementation begins, the project should:

1. **Study DuckDB Architecture** - Understand core algorithms and data structures
2. **Implement Core Primitives** - Start with basic data types and storage formats
3. **Build SQL Parser** - Focus on DuckDB-specific SQL extensions
4. **Develop Query Engine** - Implement vectorized execution
5. **Ensure Standard Compatibility** - Implement `database/sql` interface
6. **Add File Format Support** - Parquet, CSV, JSON readers/writers
7. **Optimize for Analytics** - Column-oriented processing and memory management

## License Considerations

Project uses GPL v3.0, which requires:
- Derivative works to be GPL-licensed
- Source code availability for distributed binaries
- Careful consideration when integrating with non-GPL code

This licensing choice suggests the project aims to remain open-source and may influence library selection during implementation.
