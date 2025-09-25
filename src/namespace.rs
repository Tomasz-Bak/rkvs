use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use crate::Result;
use super::types::{NamespaceConfig, BatchResult, BatchError};

/// Namespace metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceMetadata {
    pub name: String,
    pub key_count: usize,
    pub total_size: usize,
}

/// A namespace handle for working with a specific namespace
#[derive(Debug, Clone)]
pub struct Namespace {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    metadata: Arc<RwLock<NamespaceMetadata>>,
    config: Arc<RwLock<NamespaceConfig>>,
}

impl Namespace {
    pub fn new(name: String, config: NamespaceConfig) -> Self {
        let data_table = if config.max_keys.unwrap_or(0) != 0 {
            HashMap::with_capacity(config.max_keys.unwrap())
        } else {
            HashMap::with_capacity(10240)
        };

        Self {
            data: Arc::new(RwLock::new(data_table)),
            metadata: Arc::new(RwLock::new(NamespaceMetadata {
                name,
                key_count: 0,
                total_size: 0,
            })),
            config: Arc::new(RwLock::new(config)),
        }
    }

    /// Get a value by key
    /// 
    /// Retrieves the value associated with the given key from the namespace.
    /// 
    /// # Arguments
    /// 
    /// * `key` - The key to look up
    /// 
    /// # Returns
    /// 
    /// Returns `Some(value)` if the key exists, or `None` if it doesn't.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::{namespace::Namespace, NamespaceConfig};
    /// 
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let namespace = Namespace::new("test".to_string(), NamespaceConfig::default());
    ///     
    ///     // Set a value
    ///     namespace.set("key1".to_string(), b"value1".to_vec()).await?;
    ///     
    ///     // Get the value
    ///     if let Some(data) = namespace.get("key1").await {
    ///         println!("Value: {}", String::from_utf8_lossy(&data));
    ///     }
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let data = self.data.read().await;
        data.get(key).cloned()
    }

    /// Set a key-value pair
    /// 
    /// Stores a key-value pair in the namespace. The operation will fail if:
    /// - The value size exceeds the namespace's `max_value_size` limit
    /// - Adding the key would exceed the namespace's `max_keys` limit
    /// 
    /// # Arguments
    /// 
    /// * `key` - The key to store
    /// * `value` - The value to store
    /// 
    /// # Returns
    /// 
    /// Returns `Ok(())` if the operation succeeds, or an error if it fails.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::{namespace::Namespace, NamespaceConfig};
    /// 
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let config = NamespaceConfig {
    ///         max_keys: Some(1000),
    ///         max_value_size: Some(1024),
    ///     };
    ///     let namespace = Namespace::new("test".to_string(), config);
    ///     
    ///     // Set a value
    ///     namespace.set("user:123".to_string(), b"John Doe".to_vec()).await?;
    ///     
    ///     // This will fail if value is too large
    ///     let large_value = vec![0u8; 2048]; // 2KB
    ///     match namespace.set("large_key".to_string(), large_value).await {
    ///         Ok(()) => println!("Stored successfully"),
    ///         Err(e) => println!("Error: {}", e),
    ///     }
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub async fn set(&self, key: String, value: Vec<u8>) -> Result<()> {
        
        let config = self.config.read().await;
        if let Some(max_value_size) = config.max_value_size {
            if value.len() > max_value_size {
                return Err(crate::RkvsError::Storage(format!(
                    "Value size {} exceeds maximum allowed size {}", value.len(), max_value_size
                )));
            }
        }
        
        let mut data = self.data.write().await;
        let mut metadata = self.metadata.write().await;

        if !data.contains_key(&key) {
            if let Some(max_keys) = config.max_keys {
                if metadata.key_count >= max_keys {
                    return Err(crate::RkvsError::Storage(format!(
                        "Maximum number of keys ({}) reached for this namespace", max_keys
                    )));
                }
            }
            metadata.key_count += 1;
        }

        if let Some(old_value) = data.get(&key) {
            metadata.total_size = metadata.total_size - old_value.len() + value.len();
        } else {
            metadata.total_size += value.len();
        }

        data.insert(key, value);
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> bool {
        let mut data = self.data.write().await;
        let mut metadata = self.metadata.write().await;

        if let Some(value) = data.remove(key) {
            metadata.key_count -= 1;
            metadata.total_size -= value.len();
            true
        } else {
            false
        }
    }

    /// Atomically get and delete a key-value pair
    /// 
    /// This method performs an atomic get-and-delete operation. It retrieves the value
    /// associated with the key and immediately removes it from the namespace in a single
    /// atomic operation. This is useful for implementing message queues, task processing,
    /// or any scenario where you need to process and remove data atomically.
    /// 
    /// # Arguments
    /// 
    /// * `key` - The key to consume
    /// 
    /// # Returns
    /// 
    /// Returns `Some(value)` if the key existed and was consumed, or `None` if the key
    /// didn't exist.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::{namespace::Namespace, NamespaceConfig};
    /// 
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let namespace = Namespace::new("test".to_string(), NamespaceConfig::default());
    ///     
    ///     // Set a value
    ///     namespace.set("task:1".to_string(), b"process_data".to_vec()).await?;
    ///     
    ///     // Consume the task (get and delete atomically)
    ///     if let Some(task_data) = namespace.consume("task:1").await {
    ///         println!("Processing task: {}", String::from_utf8_lossy(&task_data));
    ///         // Key is now deleted, no race conditions possible
    ///     }
    ///     
    ///     // Key no longer exists
    ///     assert!(namespace.get("task:1").await.is_none());
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub async fn consume(&self, key: &str) -> Option<Vec<u8>> {
        let mut data = self.data.write().await;
        let mut metadata = self.metadata.write().await;

        if let Some(value) = data.remove(key) {
            metadata.key_count -= 1;
            metadata.total_size -= value.len();
            Some(value)
        } else {
            None
        }
    }

    pub async fn exists(&self, key: &str) -> bool {
        let data = self.data.read().await;
        data.contains_key(key)
    }

    pub async fn list_keys(&self) -> Vec<String> {
        let data = self.data.read().await;
        data.keys().cloned().collect()
    }

    pub async fn get_metadata(&self) -> NamespaceMetadata {
        self.metadata.read().await.clone()
    }

    pub async fn get_config(&self) -> NamespaceConfig {
        self.config.read().await.clone()
    }

    pub async fn update_config(&self, new_config: NamespaceConfig) -> Result<()> {
        let metadata = self.metadata.read().await;
        
        if let Some(max_keys) = new_config.max_keys {
            if metadata.key_count > max_keys {
                return Err(crate::RkvsError::Storage(format!(
                    "Cannot set max_keys to {} when namespace already has {} keys", 
                    max_keys, metadata.key_count
                )));
            }
        }
        
        let mut config = self.config.write().await;
        *config = new_config;
        Ok(())
    }

    pub async fn get_all_data(&self) -> (HashMap<String, Vec<u8>>, NamespaceMetadata) {
        let data = self.data.read().await;
        let metadata = self.metadata.read().await;
        
        (data.clone(), metadata.clone())
    }

    pub async fn restore_data(&self, data: HashMap<String, Vec<u8>>, metadata: NamespaceMetadata) {
        {
            let mut data_guard = self.data.write().await;
            *data_guard = data;
        }
        
        {
            let mut metadata_guard = self.metadata.write().await;
            *metadata_guard = metadata;
        }
    }

    /// Set multiple key-value pairs in a single batch operation
    /// 
    /// Performs a batch set operation with all-or-nothing semantics. If any item fails
    /// validation (e.g., value too large, key limit exceeded), the entire operation is
    /// aborted and no changes are made.
    /// 
    /// # Arguments
    /// 
    /// * `items` - Vector of (key, value) pairs to store
    /// 
    /// # Returns
    /// 
    /// Returns a `BatchResult<()>` containing:
    /// - `data`: `Some(())` if successful, `None` if failed
    /// - `total_processed`: Number of items processed
    /// - `duration`: Time taken for the operation
    /// - `errors`: Vector of errors if any occurred
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::{namespace::Namespace, NamespaceConfig, BatchResult};
    /// 
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let namespace = Namespace::new("test".to_string(), NamespaceConfig::default());
    ///     
    ///     let items = vec![
    ///         ("key1".to_string(), b"value1".to_vec()),
    ///         ("key2".to_string(), b"value2".to_vec()),
    ///         ("key3".to_string(), b"value3".to_vec()),
    ///     ];
    ///     
    ///     let result = namespace.set_multiple(items).await;
    ///     
    ///     if result.is_success() {
    ///         println!("Successfully set {} items", result.total_processed);
    ///     } else {
    ///         if let Some(errors) = &result.errors {
    ///             println!("Failed with {} errors", errors.len());
    ///             for error in errors {
    ///                 println!("Error for key '{}': {}", error.key, error.error_message);
    ///             }
    ///         }
    ///     }
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub async fn set_multiple(&self, items: Vec<(String, Vec<u8>)>) -> BatchResult<()> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_items = items.len();
        
        let mut data = self.data.write().await;
        let mut metadata = self.metadata.write().await;
        let config = self.config.read().await;
        
        if let Some(max_keys) = config.max_keys {
            if metadata.key_count + total_items > max_keys {
                return BatchResult {
                    data: None,
                    total_processed: total_items,
                    duration: start.elapsed(),
                    errors: Some(vec![BatchError {
                        key: "batch".to_string(),
                        operation: "set".to_string(),
                        error_message: format!(
                            "Key count limit {} would be exceeded by inserting {} items", 
                            max_keys, total_items
                        ),
                        index: 0,
                    }]),
                };
            }
        }
        
        let mut items_to_insert = Vec::with_capacity(total_items);
        let mut total_size_increase = 0;
        let mut new_keys_count = 0;
        
        for (index, (key, value)) in items.into_iter().enumerate() {
            
            if let Some(max_value_size) = config.max_value_size {
                if value.len() > max_value_size {
                    errors.push(BatchError {
                        key: key.clone(),
                        operation: "set".to_string(),
                        error_message: format!(
                            "Value size {} exceeds maximum {}", 
                            value.len(), max_value_size
                        ),
                        index,
                    });
                    continue;
                }
            }
            
            if !data.contains_key(&key) {
                new_keys_count += 1;
                total_size_increase += value.len();
            } else {
                let existing_size = data.get(&key).map(|v| v.len()).unwrap_or(0);
                total_size_increase += value.len() - existing_size;
            }

            items_to_insert.push((key, value));
        }
        
        if !errors.is_empty() {
            return BatchResult {
                data: None,
                total_processed: total_items,
                duration: start.elapsed(),
                errors: Some(errors),
            };
        }
        
        for (key, value) in items_to_insert {
            data.insert(key, value);
        }
        
        metadata.key_count += new_keys_count;
        metadata.total_size += total_size_increase;
        
        BatchResult {
            data: Some(()),
            total_processed: total_items,
            duration: start.elapsed(),
            errors: None,
        }
    }

    /// Get multiple key-value pairs in a single batch operation
    pub async fn get_multiple(&self, keys: Vec<String>) -> BatchResult<Vec<(String, Vec<u8>)>> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_keys = keys.len();
        
        let data = self.data.read().await;
        
        let mut items_to_get = Vec::new();
        
        for (index, key) in keys.into_iter().enumerate() {
            if let Some(value) = data.get(&key) {
                if errors.is_empty() {
                    items_to_get.push((key, value.clone()));
                }
            } else {
                errors.push(BatchError {
                    key,
                    operation: "get".to_string(),
                    error_message: "Key not found".to_string(),
                    index,
                });
            }
        }
        
        if !errors.is_empty() {
            return BatchResult {
                data: None,
                total_processed: total_keys,
                duration: start.elapsed(),
                errors: Some(errors),
            };
        }
        
        BatchResult {
            data: Some(items_to_get),
            total_processed: total_keys,
            duration: start.elapsed(),
            errors: None,
        }
    }

    /// Delete multiple keys in a single batch operation
    pub async fn delete_multiple(&self, keys: Vec<String>) -> BatchResult<()> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_keys = keys.len();
        
        let mut data = self.data.write().await;
        let mut metadata = self.metadata.write().await;
        
        let mut items_to_delete = Vec::new();
        let mut total_size_decrease = 0;
        let mut deleted_keys_count = 0;
        
        for (index, key) in keys.into_iter().enumerate() {
            if let Some(value) = data.get(&key) {
                if errors.is_empty() {
                    items_to_delete.push(key);
                    total_size_decrease += value.len();
                    deleted_keys_count += 1;
                }
            } else {
                errors.push(BatchError {
                    key,
                    operation: "delete".to_string(),
                    error_message: "Key not found".to_string(),
                    index,
                });
            }
        }
        
        if !errors.is_empty() {
            return BatchResult {
                data: None,
                total_processed: total_keys,
                duration: start.elapsed(),
                errors: Some(errors),
            };
        }
        
        for key in items_to_delete {
            data.remove(&key);
        }
        metadata.key_count -= deleted_keys_count;
        metadata.total_size -= total_size_decrease;
        
        BatchResult {
            data: Some(()),
            total_processed: total_keys,
            duration: start.elapsed(),
            errors: None,
        }
    }

    /// Consume multiple key-value pairs in a single batch operation
    pub async fn consume_multiple(&self, keys: Vec<String>) -> BatchResult<Vec<(String, Vec<u8>)>> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_keys = keys.len();
        
        let mut data = self.data.write().await;
        let mut metadata = self.metadata.write().await;
        
        let mut items_to_consume = Vec::new();
        let mut total_size_decrease = 0;
        let mut consumed_keys_count = 0;
        
        for (index, key) in keys.into_iter().enumerate() {
            if let Some(value) = data.get(&key) {
                if errors.is_empty() {
                    items_to_consume.push((key, value.clone()));
                    total_size_decrease += value.len();
                    consumed_keys_count += 1;
                }
            } else {
                errors.push(BatchError {
                    key,
                    operation: "consume".to_string(),
                    error_message: "Key not found".to_string(),
                    index,
                });
            }
        }
        
        if !errors.is_empty() {
            return BatchResult {
                data: None,
                total_processed: total_keys,
                duration: start.elapsed(),
                errors: Some(errors),
            };
        }
        
        let mut results = Vec::new();
        for (key, value) in items_to_consume {
            data.remove(&key);
            results.push((key, value));
        }
        metadata.key_count -= consumed_keys_count;
        metadata.total_size -= total_size_decrease;
        
        BatchResult {
            data: Some(results),
            total_processed: total_keys,
            duration: start.elapsed(),
            errors: None,
        }
    }
}

