#!/bin/bash
# Script to run analytical benchmarks and generate summary

echo "=== Analytical Benchmark Summary ==="
echo "Date: $(date)"
echo "=================================="
echo

# List of benchmarks to test
benchmarks=(
    "SimpleAggregation"
    "GroupByAggregation" 
    "HavingClause"
    "SimpleJoin"
    "ComplexJoinAggregation"
    "MultiTableJoin"
    "Union"
    "Subqueries"
)

# Run each benchmark
for bench in "${benchmarks[@]}"; do
    echo "Testing $bench..."
    
    # Run with timeout to catch hanging benchmarks
    output=$(timeout 30s go test -C /home/connerohnesorge/Documents/001Repos/dukdb-go/benchmarks -bench="Benchmark$bench/PureGo" ./analytical/ -benchtime=1x 2>&1)
    exit_code=$?
    
    # Check results
    if [ $exit_code -eq 124 ]; then
        echo "⏱️ $bench: TIMEOUT (>30s)"
    elif echo "$output" | grep -q "FAIL:"; then
        echo "❌ $bench: FAILED"
        echo "$output" | grep -E "(FAIL:|error:)" | head -3
    elif echo "$output" | grep -q "ns/op"; then
        echo "✅ $bench: PASSED"
        echo "$output" | grep "ns/op" | head -1
    else
        echo "❓ $bench: UNKNOWN"
    fi
    echo
done

echo "=================================="
echo "Summary generated successfully"