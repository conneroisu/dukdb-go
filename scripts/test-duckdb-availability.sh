#!/usr/bin/env bash
set -euo pipefail

echo "🦆 Testing DuckDB availability..."
echo ""

# Check if we're in Nix environment
if [ -n "${IN_NIX_SHELL:-}" ]; then
    echo "✅ Running in Nix shell"
else
    echo "⚠️  Not running in Nix shell"
fi

# Check DuckDB CLI
if command -v duckdb &> /dev/null; then
    echo "✅ DuckDB CLI found: $(which duckdb)"
    echo "   Version: $(duckdb --version | head -n1)"
else
    echo "❌ DuckDB CLI not found"
fi

# Check library paths
echo ""
echo "Library search paths:"
echo "  LD_LIBRARY_PATH: ${LD_LIBRARY_PATH:-<not set>}"
echo "  DYLD_LIBRARY_PATH: ${DYLD_LIBRARY_PATH:-<not set>}"
echo "  DUCKDB_LIB_DIR: ${DUCKDB_LIB_DIR:-<not set>}"

# Try to find DuckDB library
echo ""
echo "Searching for DuckDB library..."

# Common library names
lib_names=("libduckdb.so" "libduckdb.dylib" "duckdb.dll")

found=false
for lib in "${lib_names[@]}"; do
    # Check in DUCKDB_LIB_DIR
    if [ -n "${DUCKDB_LIB_DIR:-}" ] && [ -f "$DUCKDB_LIB_DIR/$lib" ]; then
        echo "✅ Found $lib in DUCKDB_LIB_DIR: $DUCKDB_LIB_DIR/$lib"
        ls -la "$DUCKDB_LIB_DIR/$lib"
        found=true
    fi
    
    # Check in LD_LIBRARY_PATH
    if [ -n "${LD_LIBRARY_PATH:-}" ]; then
        IFS=':' read -ra PATHS <<< "$LD_LIBRARY_PATH"
        for path in "${PATHS[@]}"; do
            if [ -f "$path/$lib" ]; then
                echo "✅ Found $lib in LD_LIBRARY_PATH: $path/$lib"
                ls -la "$path/$lib"
                found=true
            fi
        done
    fi
    
    # Check system locations
    for path in /usr/lib /usr/local/lib /opt/homebrew/lib; do
        if [ -f "$path/$lib" ]; then
            echo "✅ Found $lib in system path: $path/$lib"
            ls -la "$path/$lib"
            found=true
        fi
    done
done

if ! $found; then
    echo "❌ DuckDB library not found in any standard location"
    echo ""
    echo "To fix this:"
    echo "1. Run 'nix develop' to enter the development shell"
    echo "2. Or install DuckDB system-wide"
    exit 1
fi

echo ""
echo "✅ DuckDB is properly configured for use with dukdb-go!"