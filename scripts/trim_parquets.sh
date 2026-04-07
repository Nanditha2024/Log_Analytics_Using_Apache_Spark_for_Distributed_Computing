#!/bin/bash
# Trim parquet files to 100,000 records (1 lakh)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Trimming Parquet Files to 1 Lakh Records"
echo "=========================================="
echo ""

cd "$PROJECT_ROOT"

echo "Running spark-submit with trim_parquet_files.py..."
spark-submit "$SCRIPT_DIR/trim_parquet_files.py"

echo ""
echo "Done!"
