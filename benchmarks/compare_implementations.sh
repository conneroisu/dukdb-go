#!/bin/bash
# Script to compare Pure-Go vs CGO DuckDB implementations

echo "DuckDB Implementation Comparison Tool"
echo "===================================="
echo

# Check if CGO driver is available
if ! go list -m github.com/marcboeker/go-duckdb &>/dev/null; then
    echo "❌ CGO DuckDB driver not found"
    echo "To install: go get github.com/marcboeker/go-duckdb"
    echo
    echo "Running Pure-Go benchmarks only..."
    echo
else
    echo "✅ CGO DuckDB driver found"
    echo
fi

# Create results directory
mkdir -p results/comparison
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "Running benchmarks..."
echo

# Run basic benchmarks
echo "1. Basic Query Benchmarks"
echo "------------------------"
go test -bench='^(BenchmarkSimpleSelect|BenchmarkSingleInsert|BenchmarkPreparedInsert)' \
    -benchmem -benchtime=5s -run=^$ ./basic \
    > results/comparison/basic_${TIMESTAMP}.txt 2>&1

# Extract and display results
echo "Results:"
grep "BenchmarkSimpleSelect" results/comparison/basic_${TIMESTAMP}.txt || echo "  SimpleSelect: Failed"
grep "BenchmarkSingleInsert" results/comparison/basic_${TIMESTAMP}.txt || echo "  SingleInsert: Failed"
grep "BenchmarkPreparedInsert" results/comparison/basic_${TIMESTAMP}.txt || echo "  PreparedInsert: Failed"
echo

# Check if both implementations ran
if grep -q "PureGo" results/comparison/basic_${TIMESTAMP}.txt && grep -q "CGO" results/comparison/basic_${TIMESTAMP}.txt; then
    echo "📊 Comparison Summary:"
    echo "-------------------"
    
    # Parse and compare results
    python3 - << 'EOF'
import re
import sys

def parse_benchmark_line(line):
    # Parse benchmark output line
    # Format: BenchmarkName/Impl-16    iterations    ns/op    B/op    allocs/op
    parts = line.split()
    if len(parts) >= 5:
        name = parts[0]
        time_ns = int(parts[2])
        memory_b = int(parts[4]) if len(parts) > 4 else 0
        return name, time_ns, memory_b
    return None, 0, 0

try:
    with open('results/comparison/basic_${TIMESTAMP}.txt', 'r') as f:
        lines = f.readlines()
    
    benchmarks = {}
    for line in lines:
        if 'Benchmark' in line and 'ns/op' in line:
            name, time_ns, memory_b = parse_benchmark_line(line)
            if name:
                bench_name = name.split('/')[0]
                impl = 'PureGo' if 'PureGo' in name else 'CGO'
                if bench_name not in benchmarks:
                    benchmarks[bench_name] = {}
                benchmarks[bench_name][impl] = {'time': time_ns, 'memory': memory_b}
    
    # Calculate and display comparisons
    for bench, impls in benchmarks.items():
        if 'PureGo' in impls and 'CGO' in impls:
            pg_time = impls['PureGo']['time']
            cgo_time = impls['CGO']['time']
            speedup = (cgo_time - pg_time) / cgo_time * 100
            
            print(f"\n{bench}:")
            print(f"  Pure-Go: {pg_time:,} ns/op")
            print(f"  CGO:     {cgo_time:,} ns/op")
            if speedup > 0:
                print(f"  Result:  Pure-Go is {speedup:.1f}% faster ✅")
            else:
                print(f"  Result:  Pure-Go is {-speedup:.1f}% slower ⚠️")
            
except Exception as e:
    print(f"Error parsing results: {e}")
EOF
else
    echo "⚠️  Only Pure-Go implementation results available"
    echo "   Install CGO driver for full comparison"
fi

echo
echo "Full results saved to: results/comparison/basic_${TIMESTAMP}.txt"
echo

# Generate final report
echo "Generating comparison report..."
cat > results/comparison/report_${TIMESTAMP}.md << EOF
# DuckDB Implementation Comparison Report
Generated: $(date)

## Test Environment
- OS: $(uname -s) $(uname -r)
- CPU: $(grep "model name" /proc/cpuinfo | head -1 | cut -d: -f2 | xargs)
- Go Version: $(go version)
- CGO_ENABLED: ${CGO_ENABLED:-1}

## Benchmark Results

\`\`\`
$(cat results/comparison/basic_${TIMESTAMP}.txt | grep -E "Benchmark|PASS|FAIL")
\`\`\`

## Summary

EOF

if grep -q "CGO" results/comparison/basic_${TIMESTAMP}.txt; then
    echo "Both Pure-Go and CGO implementations were tested successfully." >> results/comparison/report_${TIMESTAMP}.md
else
    echo "Only Pure-Go implementation was tested. CGO driver not available." >> results/comparison/report_${TIMESTAMP}.md
    echo "" >> results/comparison/report_${TIMESTAMP}.md
    echo "To enable CGO comparison:" >> results/comparison/report_${TIMESTAMP}.md
    echo "1. Install CGO driver: \`go get github.com/marcboeker/go-duckdb\`" >> results/comparison/report_${TIMESTAMP}.md
    echo "2. Ensure CGO is enabled: \`export CGO_ENABLED=1\`" >> results/comparison/report_${TIMESTAMP}.md
    echo "3. Run this script again" >> results/comparison/report_${TIMESTAMP}.md
fi

echo "Report saved to: results/comparison/report_${TIMESTAMP}.md"