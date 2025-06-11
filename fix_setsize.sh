#!/bin/bash

# Script to fix unchecked SetSize calls

files=(
    "internal/engine/catalog.go"
    "internal/engine/connection.go"
    "internal/engine/executor.go"
    "internal/engine/executor_aggregates.go"
)

for file in "${files[@]}"; do
    echo "Fixing SetSize calls in $file..."
    
    # Find lines with SetSize that don't have error checking
    grep -n "\.SetSize(" "$file" | grep -v "err :=" | while IFS=: read line_num content; do
        echo "  Line $line_num: $content"
    done
done