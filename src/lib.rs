//! RKVS - Rust Key-Value Storage
//! 
//! A high-performance, namespace-based key-value storage system built in Rust.
//! Provides persistent storage with configurable limits, async operations, and atomic batch processing.
//!
//! # Features
//!
//! - **High Performance**: Optimized for speed with async operations and efficient data structures.
//! - **Namespace Support**: Organize data into isolated namespaces with individual configurations.
//! - **Automatic Sharding**: Keys are automatically distributed across multiple shards for improved concurrency.
//! - **Optional Auto-Scaling**: Namespaces can automatically increase their shard count as data grows.
//! - **Atomic Batch Operations**: Perform `set`, `get`, and `delete` on multiple items with all-or-nothing semantics.
//! - **Persistence**: Optional file-based persistence with automatic serialization.
//! - **Thread-Safe**: Built on Tokio's async primitives for concurrent access.
//! - **Convenience Methods**: Includes atomic operations like `consume` (get and delete) and `update` (fail if key doesn't exist).
//!
//! # Quick Start
//!
//! ```rust
//! use rkvs::{StorageManager, NamespaceConfig, Result};
//! use std::env::temp_dir;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Use a temporary directory. The persistence layer will create it if it doesn't exist.
//!     let temp_path = temp_dir().join("rkvs_docs_lib_main");
//!
//!     let storage = StorageManager::builder()
//!         .with_persistence(temp_path)
//!         .build().await?;
//!     
//!     // Initialize the storage
//!     storage.initialize(None).await?;
//!     
//!     // Create a namespace with configuration
//!     let mut config = NamespaceConfig::default();
//!     config.set_max_keys(1000);
//!     config.set_max_value_size(1024 * 1024); // 1MB
//!     storage.create_namespace("my_app", Some(config)).await?;
//!     
//!     // Get the namespace handle
//!     let namespace = storage.namespace("my_app").await?;
//!     
//!     // Store data
//!     namespace.set("user:123", b"John Doe".to_vec()).await?;
//!     
//!     // Retrieve data
//!     if let Some(data) = namespace.get("user:123").await {
//!         println!("User: {}", String::from_utf8_lossy(&data));
//!     }
//!     
//!     Ok(())
//! }
//! ```
//!
//! # Batch Operations
//!
//! RKVS supports efficient batch operations for processing multiple key-value pairs:
//! ```rust
//! use rkvs::{StorageManager, Result, BatchMode};
//!
//! # async fn run() -> Result<()> {
//! #   let storage = StorageManager::builder().build().await?;
//! #   storage.initialize(None).await?;
//! #   storage.create_namespace("my_app", None).await?;
//! #   let namespace = storage.namespace("my_app").await?;
//!
//!     // Batch set multiple items
//!     let items = vec![
//!         ("key1".to_string(), b"value1".to_vec()),
//!         ("key2".to_string(), b"value2".to_vec()),
//!         ("key3".to_string(), b"value3".to_vec()),
//!     ];
//!
//!     let result = namespace.set_multiple(items, BatchMode::BestEffort).await;
//!
//!     // Check for errors by inspecting the `errors` field.
//!     if result.errors.is_none() {
//!         println!("Set {} items successfully", result.total_processed);
//!     }
//!     
//!     Ok(())
//! }
//! ```


pub mod data_table;
pub mod error;
pub mod types;
pub mod namespace;
pub mod manager;
pub mod persistence;

// Re-export commonly used types
pub use error::{RkvsError, Result};
pub use types::*;
pub use namespace::*;
pub use manager::*;
pub use persistence::*;
