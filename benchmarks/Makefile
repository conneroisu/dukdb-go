# DuckDB Go Benchmarks Makefile

.PHONY: all basic analytical dataload clean help install-cgo

# Default target
all: basic analytical dataload

# Install CGO DuckDB driver
install-cgo:
	@echo "Installing CGO DuckDB driver..."
	go get github.com/marcboeker/go-duckdb

# Run basic query benchmarks
basic:
	@echo "Running basic query benchmarks..."
	go test -bench=. -benchmem -run=^$$ ./basic

# Run analytical query benchmarks
analytical:
	@echo "Running analytical query benchmarks..."
	go test -bench=. -benchmem -run=^$$ ./analytical

# Run data loading benchmarks
dataload:
	@echo "Running data loading benchmarks..."
	go test -bench=. -benchmem -run=^$$ ./dataload

# Run all benchmarks with comparison
compare: install-cgo
	@echo "Running full benchmark comparison..."
	go run run_benchmarks.go

# Run benchmarks with pure-Go only
purego:
	@echo "Running pure-Go benchmarks only..."
	CGO_ENABLED=0 go run run_benchmarks.go -skip-cgo

# Run quick smoke test
quick:
	@echo "Running quick benchmark test..."
	go test -bench=BenchmarkSimpleSelect -benchtime=1s -run=^$$ ./basic

# Generate test data files
testdata:
	@echo "Generating test data files..."
	@mkdir -p testdata
	@go run ../scripts/generate_test_data.go

# Clean results
clean:
	@echo "Cleaning benchmark results..."
	@rm -rf results/

# Show help
help:
	@echo "DuckDB Go Benchmarks"
	@echo ""
	@echo "Usage:"
	@echo "  make all          - Run all benchmark categories"
	@echo "  make basic        - Run basic query benchmarks"
	@echo "  make analytical   - Run analytical query benchmarks"
	@echo "  make dataload     - Run data loading benchmarks"
	@echo "  make compare      - Run full comparison with CGO"
	@echo "  make purego       - Run pure-Go benchmarks only"
	@echo "  make quick        - Run quick smoke test"
	@echo "  make install-cgo  - Install CGO DuckDB driver"
	@echo "  make clean        - Clean benchmark results"
	@echo "  make help         - Show this help"
	@echo ""
	@echo "Examples:"
	@echo "  make basic                    # Run basic benchmarks"
	@echo "  make compare                  # Full comparison"
	@echo "  CGO_ENABLED=0 make basic      # Pure-Go only"