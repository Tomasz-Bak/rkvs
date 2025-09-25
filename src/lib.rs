//! RKVS - Rust Key-Value Storage
//! 
//! A high-performance, namespace-based key-value storage system built in Rust.
//! Provides persistent storage with configurable limits, async operations, and atomic batch processing.
//!
//! # Features
//!
//! - **High Performance**: Optimized for speed with async operations and efficient data structures
//! - **Namespace Support**: Organize data into isolated namespaces with individual configurations
//! - **Atomic Operations**: Batch operations with all-or-nothing semantics
//! - **Persistence**: Optional file-based persistence with automatic serialization
//! - **Thread-Safe**: Built on Tokio's async primitives for concurrent access
//! - **Rich Metadata**: Track key counts, sizes, and namespace statistics
//! - **Configurable Limits**: Set per-namespace limits for keys and value sizes
//! - **Atomic Consume**: Get and delete operations in a single atomic step
//!
//! # Quick Start
//!
//! ```rust
//! use rkvs::{StorageManager, NamespaceConfig, Result};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create a storage manager
//!     let storage = StorageManager::builder()
//!         .with_persistence("/tmp/rkvs_data".into())
//!         .build();
//!     
//!     // Initialize the storage
//!     storage.initialize().await?;
//!     
//!     // Create a namespace with configuration
//!     let config = NamespaceConfig {
//!         max_keys: Some(1000),
//!         max_value_size: Some(1024 * 1024), // 1MB
//!     };
//!     let namespace_id = storage.create_namespace("my_app", Some(config)).await?;
//!     
//!     // Get the namespace handle
//!     let namespace = storage.namespace(&namespace_id).await?;
//!     
//!     // Store data
//!     namespace.set("user:123".to_string(), b"John Doe".to_vec()).await?;
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
//!
//! ```rust
//! use rkvs::{namespace::Namespace, BatchResult, Result};
//!
//! async fn batch_example(namespace: &Namespace) -> Result<()> {
//!     // Batch set multiple items
//!     let items = vec![
//!         ("key1".to_string(), b"value1".to_vec()),
//!         ("key2".to_string(), b"value2".to_vec()),
//!         ("key3".to_string(), b"value3".to_vec()),
//!     ];
//!     
//!     let result = namespace.set_multiple(items).await;
//!     if result.is_success() {
//!         println!("Set {} items successfully", result.total_processed);
//!     }
//!     
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod types;
pub mod namespace;
pub mod manager;
pub mod persistence;

// Re-export commonly used types
pub use error::{RkvsError, Result};
pub use types::*;
pub use manager::*;
pub use persistence::*;
