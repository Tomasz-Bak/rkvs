# RKVS - Rust Key-Value Storage

[![Crates.io](https://img.shields.io/crates/v/rkvs.svg)](https://crates.io/crates/rkvs)
[![Documentation](https://docs.rs/rkvs/badge.svg)](https://docs.rs/rkvs)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A high-performance, namespace-based key-value storage system built in Rust. RKVS provides persistent storage with configurable limits, async operations, and atomic batch processing.

## Features

- **🚀 High Performance**: Optimized for speed with async operations and efficient data structures
- **📁 Namespace Support**: Organize data into isolated namespaces with individual configurations
- **⚡ Atomic Operations**: Batch operations with all-or-nothing semantics
- **💾 Persistence**: Optional file-based persistence with automatic serialization
- **🔒 Thread-Safe**: Built on Tokio's async primitives for concurrent access
- **📊 Rich Metadata**: Track key counts, sizes, and namespace statistics
- **🎯 Configurable Limits**: Set per-namespace limits for keys and value sizes
- **🔄 Atomic Consume**: Get and delete operations in a single atomic step

## Quick Start

Add RKVS to your `Cargo.toml`:

```toml
[dependencies]
rkvs = "0.2.0"
tokio = { version = "1.0", features = ["full"] }
```

### Basic Usage

```rust
use rkvs::{StorageManager, NamespaceConfig, Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Create a storage manager
    let storage = StorageManager::builder()
        .with_persistence("/tmp/rkvs_data".into())
        .build();
    
    // Initialize the storage
    storage.initialize().await?;
    
    // Create a namespace with configuration
    let config = NamespaceConfig {
        max_keys: Some(1000),
        max_value_size: Some(1024 * 1024), // 1MB
    };
    storage.create_namespace("my_app", Some(config)).await?;
    
    // Get the namespace handle
    let namespace = storage.namespace("my_app").await?;
    
    // Store data
    namespace.set("user:123".to_string(), b"John Doe".to_vec()).await?;
    
    // Retrieve data
    if let Some(data) = namespace.get("user:123").await {
        println!("User: {}", String::from_utf8_lossy(&data));
    }
    
    // Atomic consume (get and delete)
    if let Some(data) = namespace.consume("user:123").await {
        println!("Consumed: {}", String::from_utf8_lossy(&data));
        // Key is now deleted
    }
    
    Ok(())
}
```

### Batch Operations

RKVS supports efficient batch operations for processing multiple key-value pairs:

```rust
use rkvs::{Namespace, BatchResult};

async fn batch_example(namespace: &Namespace) -> Result<()> {
    // Batch set multiple items
    let items = vec![
        ("key1".to_string(), b"value1".to_vec()),
        ("key2".to_string(), b"value2".to_vec()),
        ("key3".to_string(), b"value3".to_vec()),
    ];
    
    let result = namespace.set_multiple(items).await;
    if result.is_success() {
        println!("Set {} items successfully", result.total_processed);
    } else {
        println!("Errors: {:?}", result.errors);
    }
    
    // Batch get multiple items
    let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
    let result = namespace.get_multiple(keys).await;
    
    if let Some(data) = result.data {
        for (key, value) in data {
            println!("{}: {}", key, String::from_utf8_lossy(&value));
        }
    }
    
    Ok(())
}
```

### Configuration

Configure storage and namespace limits:

```rust
use rkvs::{StorageConfig, NamespaceConfig};

// Global storage configuration
let storage_config = StorageConfig {
    max_namespaces: Some(100),
    default_max_keys_per_namespace: Some(10000),
    default_max_value_size: Some(10 * 1024 * 1024), // 10MB
};

// Per-namespace configuration
let namespace_config = NamespaceConfig {
    max_keys: Some(1000),
    max_value_size: Some(1024 * 1024), // 1MB
};

let storage = StorageManager::builder()
    .with_config(storage_config)
    .with_persistence("/data/rkvs".into())
    .build();
```

## Performance

RKVS is designed for high performance with the following characteristics:

- **Get**: ~134ns
- **Set**: ~482ns
- **Delete** - ~160ns
- **Exists** - ~122ns
- **consume** - ~160ns 
- **Batch Operations**: Tests show 30% more efficient Get and negligible Set
- **Concurrent Access**: Optimized for read-heavy workloads with RwLock-based synchronization

See the [benchmark results](benches/) for detailed performance metrics.

## API Reference

### Core Types

- **`StorageManager`**: Main entry point for managing namespaces
- **`Namespace`**: Handle for working with a specific namespace
- **`NamespaceConfig`**: Configuration for namespace limits
- **`StorageConfig`**: Global storage configuration
- **`BatchResult<T>`**: Result of batch operations with metadata

### Key Methods

#### StorageManager
- `create_namespace(name, config)` - Create a new namespace, returns namespace name
- `namespace(name)` - Get a namespace handle using namespace name
- `delete_namespace(name)` - Remove a namespace using namespace name
- `list_namespaces()` - List all namespace names
- `save()` - Save all data to disk (if persistence enabled)
- `initialize()` - Initialize storage and load from disk (if persistence enabled)

#### Namespace
- `set(key, value)` - Store a key-value pair
- `get(key)` - Retrieve a value
- `delete(key)` - Remove a key
- `exists(key)` - Check if key exists
- `consume(key)` - Atomically get and delete
- `set_multiple(items)` - Batch set operation
- `get_multiple(keys)` - Batch get operation
- `delete_multiple(keys)` - Batch delete operation
- `consume_multiple(keys)` - Batch consume operation

## Error Handling

RKVS uses a unified error type `RkvsError` with the following variants:

- **`Storage`**: File I/O and storage-related errors
- **`Serialization`**: Data serialization/deserialization errors
- **`Internal`**: Internal system errors

```rust
use rkvs::{Result, RkvsError};

async fn error_handling_example() -> Result<()> {
    match namespace.set("key".to_string(), b"value".to_vec()).await {
        Ok(()) => println!("Success"),
        Err(RkvsError::Storage(msg)) => println!("Storage error: {}", msg),
        Err(RkvsError::Serialization(msg)) => println!("Serialization error: {}", msg),
        Err(RkvsError::Internal(msg)) => println!("Internal error: {}", msg),
    }
    Ok(())
}
```

## Thread Safety

RKVS is fully thread-safe and designed for concurrent access:

- All operations are async and can be safely called from multiple tasks
- Read operations can run concurrently within a namespace
- Write operations are serialized per namespace
- Batch operations provide atomicity guarantees

## Persistence

RKVS supports optional file-based persistence:

```rust
let storage = StorageManager::builder()
    .with_persistence("/path/to/data".into())
    .build();

// Save all data to disk
storage.save().await?;

// Initialize storage (loads data from disk if persistence is enabled)
storage.initialize().await?;
```

Data is automatically serialized using `bincode` for efficient storage.

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

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Changelog

### v0.2.0 (Latest)
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
