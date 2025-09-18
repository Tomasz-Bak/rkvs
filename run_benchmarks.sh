#!/bin/bash

# RKVS Benchmark Runner
# This script runs comprehensive benchmarks for the RKVS library

set -e

echo "🚀 Running RKVS Performance Benchmarks"
echo "======================================"

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    echo "❌ Error: Please run this script from the project root directory"
    exit 1
fi

# Build the project first
echo "📦 Building project..."
cargo build --release

# Run the benchmarks
echo "🏃 Running benchmarks..."
echo ""

# Run all benchmarks
cargo bench

echo ""
echo "✅ Benchmarks completed!"
echo ""
echo "📊 Results are saved in target/criterion/"
echo "📈 HTML reports available in target/criterion/rkvs_benchmarks/"
echo ""
echo "💡 To run specific benchmark groups:"
echo "   cargo bench --bench rkvs_benchmarks -- namespace_operations"
echo "   cargo bench --bench rkvs_benchmarks -- key_value_operations"
echo "   cargo bench --bench rkvs_benchmarks -- bulk_operations"
echo "   cargo bench --bench rkvs_benchmarks -- concurrent_operations"
echo "   cargo bench --bench rkvs_benchmarks -- persistence_operations"
echo "   cargo bench --bench rkvs_benchmarks -- config_operations"
