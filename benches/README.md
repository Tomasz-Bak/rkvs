# RKVS Benchmark Suite

This directory contains a comprehensive benchmark suite for the RKVS (Rust Key-Value Store) project.

## Files

- `rkvs_benchmarks.rs` - Main benchmark suite using Criterion (standard benchmarking)
- `realistic_benchmark.rs` - Standalone realistic benchmark with detailed analysis
- `benchmark_helpers.rs` - Helper functions for realistic benchmarking

## Running Benchmarks

### Run Criterion benchmarks (standard)
```bash
cargo bench
```

### Run specific Criterion benchmark groups
```bash
cargo bench single_operations
cargo bench bulk_operations
cargo bench concurrent_operations
cargo bench data_sizes
cargo bench realistic_workloads
```

### Run realistic benchmark with detailed analysis
```bash
# Run all tests with default settings
cargo run --bin realistic_benchmark

# Run with custom test and warmup times
cargo run --bin realistic_benchmark -- --test-time 2000 --warmup-time 500

# Run only specific test
cargo run --bin realistic_benchmark -- --test single

# Run concurrency test with custom thread count
cargo run --bin realistic_benchmark -- --test concurrency --concurrency 20

# Verbose output with configuration details
cargo run --bin realistic_benchmark -- --verbose

# Show help
cargo run --bin realistic_benchmark -- --help

# Bulk operations only
cargo run --bin realistic_benchmark -- --test bulk --verbose

# Bulk operations with custom batch size (10 items per operation)
cargo run --bin realistic_benchmark -- --test bulk --batch-size 10 --verbose
```

### Run with specific options
```bash
# Run with more iterations for better accuracy
cargo bench -- --measurement-time 30

# Run only a specific benchmark
cargo bench -- single_operations::get_operation

# Save results to file
cargo bench -- --save-baseline my_baseline
```

## Benchmark Types

### 1. Criterion Benchmarks (Standard)
Fast, automated benchmarks using the Criterion framework:

#### Single Operations
- `get_operation` - Single key retrieval
- `set_operation` - Single key storage
- `delete_operation` - Single key deletion
- `exists_operation` - Key existence check
- `consume_operation` - Single key consume (get and delete)

#### Bulk Operations
- `bulk_get` - Multiple key retrieval (10, 50, 100 keys)
- `bulk_set` - Multiple key storage (10, 50, 100 keys)

#### Concurrent Operations
- `concurrent_reads` - Concurrent read operations (2, 5, 10, 20 threads)
- `concurrent_writes` - Concurrent write operations (2, 5, 10 threads)

#### Data Sizes
- Tests with different data sizes: 64B, 256B, 1KB, 4KB, 16KB
- Both get and set operations for each size

#### Realistic Workloads
- `mixed_read_write` - Mixed read/write operations
- `read_heavy` - Read-heavy workload
- `write_heavy` - Write-heavy workload

### 2. Realistic Benchmarks (Detailed Analysis)
Comprehensive performance analysis with anti-optimization measures:

#### Single Get Operation Analysis
- Detailed latency percentiles (P50, P95, P99)
- Operations per second with realistic timing
- Performance assessment (EXCELLENT/GOOD/FAIR/POOR)

#### Mixed Read/Write Workload Analysis
- Realistic workload simulation
- Combined read and write operations
- Performance metrics for mixed scenarios

#### High Concurrency Analysis
- Multi-threaded performance testing
- Concurrent operation scaling
- Thread-level performance analysis

#### Data Size Impact Analysis
- Performance across different data sizes (64B to 16KB)
- Latency analysis for various payload sizes
- Throughput scaling with data size

#### Consume Operations Analysis
- Single key consume operations (get and delete in one operation)
- Consume all operations (retrieve and delete all keys)
- Performance comparison between consume and separate get/delete operations

## Command Line Options

The realistic benchmark supports the following command line options:

- `--test-time <MILLISECONDS>` - Test duration in milliseconds (default: 1000)
- `--warmup-time <MILLISECONDS>` - Warmup duration in milliseconds (default: 200)
- `-t, --test <TEST_TYPE>` - Run only specific test: `single`, `mixed`, `concurrency`, `data_sizes`, `bulk`, or `all` (default: all)
- `-c, --concurrency <THREADS>` - Number of concurrent threads for concurrency test (default: 10)
- `--batch-size <ITEMS>` - Number of items per batch operation (get_multiple, set_multiple, delete_multiple, consume_multiple) (default: 5)
- `-v, --verbose` - Show configuration details and verbose output
- `-h, --help` - Show help message
- `-V, --version` - Show version information

## Features

- **Realistic Performance**: Includes anti-optimization measures to prevent unrealistic results
- **Comprehensive Coverage**: Tests single operations, bulk operations, concurrency, and data sizes
- **Data Size Analysis**: Tests performance with different data sizes (64B to 16KB)
- **Realistic Workloads**: Simulates real-world usage patterns
- **Detailed Metrics**: Provides operations per second, latency percentiles, and error rates
- **Configurable**: Command line options for test duration, warmup time, and test selection
- **Flexible Testing**: Run individual tests or all tests with custom parameters

## Understanding Results

The benchmark results show:
- **Operations per second (OPS)**: How many operations can be performed per second
- **Latency percentiles**: P50, P95, P99 latency measurements
- **Throughput**: Data transfer rates in MB/s
- **Error rates**: Percentage of failed operations

## Performance Expectations

Based on typical results:
- **Single operations**: 100K-500K OPS
- **Bulk operations**: 10K-100K OPS (depending on batch size)
- **Concurrent operations**: Scales with thread count
- **Data size impact**: Larger data sizes may reduce OPS but increase throughput
- **Persistence**: Disk operations are significantly slower than memory operations

## Customization

The benchmark suite can be easily customized by:
- Modifying data sizes in `benchmark_data_sizes`
- Adjusting concurrency levels in `benchmark_concurrent_operations`
- Adding new workload patterns in `benchmark_realistic_workloads`
- Changing measurement times and warmup periods
