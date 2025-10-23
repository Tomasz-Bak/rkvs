[![Crates.io](https://img.shields.io/crates/v/rkvs.svg)](https://crates.io/crates/rkvs)
[![Documentation](https://docs.rs/rkvs/badge.svg)](https://docs.rs/rkvs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
# RKVS - Rust Key-Value Storage

RKVS is a high-performance, in-memory, asynchronous key-value storage library for Rust. It is designed for concurrent applications and provides a thread-safe API built on Tokio.

Key features include:

- **Namespaces**: Isolate data into separate key-value stores, each with its own configuration for limits and behavior.
- **Automatic Sharding**: Keys are automatically distributed across internal shards using jump consistent hashing for improved concurrency under load.
- **Concurrent Access**: Optimized for high-throughput scenarios with support for multiple concurrent readers and writers, using `RwLock` for efficient read-heavy workloads.
- **Batch Operations**: Perform atomic `set`, `get`, and `delete` operations on multiple items with "all-or-nothing" or "best-effort" semantics.
- **Optional Persistence**: Save and load snapshots of the entire database or individual namespaces to disk.
- **Rich API**: Includes convenience methods like `consume` (atomic get-and-delete) and `update` (fail if a key does not exist).
- **Configurable Autosave**: Configure automatic background saving for the entire storage manager or for individual namespaces.

## Basic Usage

RKVS is designed to be straightforward to use. Here's a quick overview of its core capabilities, including initialization, namespace management, single-key operations, batch operations, sharding, and persistence.
<details>
<summary>Code Example</summary>

```rust
use rkvs::{
    StorageManager, StorageConfig, NamespaceConfig,
    ManagerAutosaveConfig, NamespaceAutosaveConfig,
    BatchMode, Result,
};
use std::time::Duration;
use std::env::temp_dir; // For temporary persistence path

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Setup StorageManager with Persistence
    //    Using a temporary directory for demonstration.
    let persistence_path = temp_dir().join("rkvs_basic_usage");
    println!("Persistence path: {}", persistence_path.display());

    // Configure manager-level autosave (optional)
    let manager_config = StorageConfig {
        max_namespaces: None, // No limit on namespaces
        manager_autosave: Some(ManagerAutosaveConfig {
            interval: Duration::from_secs(300), // Save every 5 minutes
            filename: "full_db_snapshot.bin".to_string(),
        }),
        namespace_autosave: vec![], // Can be configured here or dynamically
    };

    let storage = StorageManager::builder()
        .with_config(manager_config)
        .with_persistence(persistence_path.clone())
        .build().await?;

    // 2. Initialize the StorageManager
    //    Attempt to load from a snapshot. If not found, starts fresh without error.
    storage.initialize(Some("full_db_snapshot.bin")).await?;
    println!("StorageManager initialized.");

    // 3. Create a Namespace
    let ns_name = "my_application_data";
    let mut ns_config = NamespaceConfig::default();
    ns_config.set_max_keys(10_000); // Limit to 10,000 keys
    ns_config.set_shard_count(4);   // Use 4 shards for this namespace

    storage.create_namespace(ns_name, Some(ns_config.clone())).await?;
    println!("Namespace '{}' created with {} shards.", ns_name, ns_config.shard_count());

    // Get a handle to the namespace
    let namespace = storage.namespace(ns_name).await?;

    // 4. Single Key Operations: Set, Get, Update, Exists, Consume, Delete

    // Set a new key
    let old_value = namespace.set("user:1", b"Alice".to_vec()).await?;
    assert!(old_value.is_none());
    println!("Set 'user:1' to 'Alice'");

    // Get a value
    let value = namespace.get("user:1").await;
    assert_eq!(value.map(|v| *v), Some(b"Alice".to_vec()));
    println!("Got 'user:1': {:?}", value.map(|v| String::from_utf8_lossy(v.as_ref())));

    // Update an existing key (fails if key does not exist)
    let old_value = namespace.update("user:1", b"Alicia".to_vec()).await?;
    assert_eq!(*old_value, b"Alice".to_vec());
    println!("Updated 'user:1' to 'Alicia', old value was 'Alice'");

    // Check if a key exists
    assert!(namespace.exists("user:1").await);
    println!("'user:1' exists.");

    // Consume (atomically get and delete)
    let consumed_value = namespace.consume("user:1").await?;
    assert_eq!(*consumed_value, b"Alicia".to_vec());
    assert!(!namespace.exists("user:1").await);
    println!("Consumed 'user:1', value was 'Alicia'. It no longer exists.");

    // Set keys back for further examples
    namespace.set("user:1", b"Bob".to_vec()).await?;
    namespace.set("user:2", b"Charlie".to_vec()).await?;
    namespace.set("user:3", b"David".to_vec()).await?;
    println!("Set 'user:1', 'user:2', 'user:3' for batch operations.");

    // Delete a key
    let deleted = namespace.delete("user:2").await;
    assert!(deleted);
    assert!(!namespace.exists("user:2").await);
    println!("Deleted 'user:2'.");

    // 5. Batch Operations

    // Batch Set (BestEffort: processes all, reports errors for failed ones)
    let batch_set_items = vec![("user:1".to_string(), b"Bobby".to_vec()), ("user:4".to_string(), b"Eve".to_vec())];
    let set_result = namespace.set_multiple(batch_set_items, BatchMode::BestEffort).await?;
    println!("Batch Set (BestEffort) processed {} items.", set_result.total_processed);

    // Batch Get (AllOrNothing: fails if any key is missing)
    let batch_get_keys_aon = vec!["user:1".to_string(), "non_existent_key".to_string()];
    let get_result_aon = namespace.get_multiple(batch_get_keys_aon, BatchMode::AllOrNothing).await;
    assert!(get_result_aon.data.is_none() && get_result_aon.errors.is_some());
    println!("Batch Get (AllOrNothing) failed as expected for missing key.");

    // Batch Delete (BestEffort)
    let batch_delete_keys = vec!["user:3".to_string(), "non_existent_key_2".to_string()];
    let delete_result = namespace.delete_multiple(batch_delete_keys, BatchMode::BestEffort).await?;
    assert!(delete_result.errors.is_some()); // non_existent_key_2 was not found
    println!("Batch Delete (BestEffort) deleted 1 item, 1 error reported.");

    // 6. Resizing Shards (only supports increasing shard count)
    let current_shard_count = namespace.get_config().await.shard_count();
    namespace.resize_shards(current_shard_count * 2).await?;
    println!("Namespace '{}' resized from {} to {} shards.", ns_name, current_shard_count, namespace.get_config().await.shard_count());

    // 7. Manual Persistence (Save/Load)
    storage.save_all("manual_full_snapshot.bin").await?; // Saves all namespaces
    storage.save_namespace(ns_name, "my_app_snapshot.bin").await?; // Saves a single namespace
    println!("Manually saved full StorageManager and namespace '{}' snapshots.", ns_name);

    // 8. Dynamic Namespace Autosave (can also be configured at StorageManager creation)
    let ns_autosave_config = NamespaceAutosaveConfig {
        namespace_name: ns_name.to_string(),
        interval: Duration::from_secs(60), // Save every minute
        filename_pattern: "ns_{ns}_snapshot_{ts}.bin".to_string(), // {ns} and {ts} are placeholders
    };
    storage.add_namespace_autosave_task(ns_autosave_config).await?;
    println!("Added dynamic autosave task for namespace '{}'.", ns_name);

    // 9. Clean up (optional, for demonstration purposes)
    storage.delete_namespace(ns_name).await?;
    println!("Namespace '{}' deleted.", ns_name);

    // Clean up persistence files
    if persistence_path.exists() {
        std::fs::remove_dir_all(&persistence_path)?;
        println!("Cleaned up persistence directory: {}", persistence_path.display());
    }

    Ok(())
}
```
</details>

### Performance Overview

RKVS is designed for high-performance, in-memory key-value storage. Our benchmarks aim to illustrate its capabilities across various workloads and configurations, latest test results are available [here](https://github.com/Tomasz-Bak/rkvs/tree/master/assets/benchmarks). While exact numbers will vary based on hardware and specific test conditions, the general trends observed are:

*   **Sequential Operations (e.g., `get`, `set`, `delete`)**:
    *   Individual operations exhibit very low latency, typically in the single-digit to low double-digit microsecond range.
    *   Latency scales gracefully with increasing namespace size (from 1k to 1M keys), showing that the underlying data structures maintain efficiency even with large datasets.
    *   `exists` operations are generally the fastest, followed by `get`, `set` (update), `update`, `consume`, and `set` (insert) and `delete`.

*   **Sharding Overhead**:
    *   Sharding effectively distributes load, leading to improved overall throughput and often reduced average latency for individual operations as the number of shards increases, up to an optimal point.
    *   The `jump_consistent_hash` algorithm ensures a relatively even distribution of keys across shards, minimizing hot spots and maximizing the benefits of concurrency. The deviation from perfect distribution remains low across various shard counts.

*   **Concurrent Workloads (Mixed Read/Write)**:
    *   RKVS demonstrates strong performance under concurrent access, leveraging `RwLock` for efficient read-heavy scenarios and effective sharding for write-heavy or balanced workloads.
    *   Throughput (operations per second) increases significantly with higher concurrency levels and appropriate shard counts.
    *   Average latency per operation remains stable or decreases for read-heavy workloads, and scales predictably for write-heavy workloads as concurrency and sharding are optimized.

*   **Batch Operations (e.g., `set_multiple`, `get_multiple`)**:
    *   Batch operations provide a substantial performance improvement by amortizing overhead across multiple key-value pairs.
    *   The average latency *per item* in a batch is significantly lower than performing individual operations, making batching highly recommended for bulk data manipulation.
    *   `BestEffort` mode typically offers slightly lower latency than `AllOrNothing` due to reduced validation and rollback overhead, but `AllOrNothing` provides stronger transactional guarantees.

*   **Concurrent Batch Operations**:
    *   Combining batching with concurrency yields very high throughput for bulk data operations under load.
    *   Latency per item remains low, even as multiple concurrent tasks perform batch operations, showcasing the efficiency of RKVS's concurrent design for large-scale data processing.

## Benchmarks

RKVS includes a comprehensive suite of benchmarks to measure performance across various workloads. The results are saved as JSON files, and a Python script is provided to generate plots from these results.

### Running the Benchmarks

The benchmarks are located in the `benches/` directory and can be run using `cargo bench`. Each benchmark focuses on a different aspect of the system. The `-- --nocapture` flag is recommended to see live progress and results in the console.

-   **Sequential Operations**: Measures latency for individual `get`, `set`, `delete`, etc., operations on namespaces of different sizes.
    ```sh
    cargo bench --bench operations_bench -- --nocapture
    ```

-   **Concurrent Workloads**: Measures latency and throughput for mixed read/write workloads at different concurrency levels and shard counts.
    ```sh
    cargo bench --bench concurrent_bench -- --nocapture
    ```

-   **Batch Operations**: Measures latency for batch `set_multiple`, `get_multiple`, and `delete_multiple` operations.
    ```sh
    cargo bench --bench batch_operations_bench -- --nocapture
    ```

-   **Concurrent Batch Operations**: Measures latency for concurrent batch operations.
    ```sh
    cargo bench --bench batch_concurrent_bench -- --nocapture
    ```

-   **Sharding Overhead**: Measures the latency overhead of sharding for `get` and `set` operations as the number of shards increases.
    ```sh
    cargo bench --bench sharding_overhead_bench -- --nocapture
    ```

Running a benchmark will produce a `.json` result file in the `assets/benchmarks/` directory.

## Migration Guide

### Upgrading from v0.1.0 to v0.2.0

The main breaking change is the switch from hash-based namespace identifiers to string-based namespace IDs:

**Before (v0.1.0):**
```rust
let ns_hash = storage.create_namespace("my_app", Some(config)).await?;
let namespace = storage.namespace(ns_hash).await?;  // ns_hash was [u8; 32]
```

**After (v0.2.0):**
```rust
storage.create_namespace("my_app", Some(config)).await?;
storage.namespace("my_app").await?;  // namespace_id is String
```
**Key Changes:**
- `create_namespace()` now returns Ok(()) or Err() instead of `[u8; 32]`
- `namespace()` method now takes `&str` instead of `[u8; 32]`
- All other methods (`delete_namespace`, `get_namespace_stats`, etc.) now use `&str` for namespace identification
- No more manual hash conversion needed 

## License

This project is licensed under the MIT License - see the [LICENSE](https://opensource.org/licenses/MIT) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Changelog

### v0.3.1 (Latest)
- **Fix**: Fixed persistance to save the config along the manager

### v0.3.0
- **Core Feature**: Implemented automatic sharding for namespaces.
- **Persistence**: Implemented automated background persistence for both the entire storage manager and individual namespaces.
- **Batch Operations**: Reworked batch operations for `set`, `get`, `delete`, and `consume` with `AllOrNothing` and `BestEffort` modes.
- **Benchmarking**: Reworked the benchmarking suite to cover new features and provide more detailed performance metrics.
- **Benchmarking**: Added Python scripts for generating benchmark plots from results.
- **API Consistency**: Addressed several API inconsistencies for a more uniform user experience.
- **Serialization**: Improved serialization mechanisms for better performance and reliability during persistence.
- **Data Structure**: Reworked the base data structure for improved efficiency and concurrency.
- **Documentation**: Updated `README.md` with a comprehensive project summary and a detailed "Basic Usage" section.

### v0.2.0
- **Breaking Change**: Updated API to use string-based namespace IDs instead of hash values
- **Performance**: Switched from `Mutex` to `RwLock` for better concurrent read performance, removed pointless hashing and data duplication
- **API Improvements**: Simplified namespace ID handling - no more manual hash conversion needed
- **Documentation**: Updated all examples and documentation to reflect new API
- **Concurrency**: Improved read performance with multiple concurrent readers support

### v0.1.0
- Initial release
- Namespace-based storage
- Async operations
- Batch processing
- File persistence
- Comprehensive benchmarking
