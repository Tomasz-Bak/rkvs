use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use crate::Result;
use super::types::{HashUtils, NamespaceConfig, BatchResult, BatchError};

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
    data: Arc<RwLock<HashMap<[u8; 32], Vec<u8>>>>,
    key_mappings: Arc<RwLock<HashMap<[u8; 32], String>>>,
    metadata: Arc<RwLock<NamespaceMetadata>>,
    config: Arc<RwLock<NamespaceConfig>>,
}

impl Namespace {
    pub fn new(name: String, config: NamespaceConfig) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            key_mappings: Arc::new(RwLock::new(HashMap::new())),
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
        let key_hash = HashUtils::hash(key);
        let data = self.data.read().await;
        data.get(&key_hash).cloned()
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
        let key_hash = HashUtils::hash(&key);
        
        let config = self.config.read().await;
        if let Some(max_value_size) = config.max_value_size {
            if value.len() > max_value_size {
                return Err(crate::RkvsError::Storage(format!(
                    "Value size {} exceeds maximum allowed size {}", value.len(), max_value_size
                )));
            }
        }
        
        let mut data = self.data.write().await;
        let mut key_mappings = self.key_mappings.write().await;
        let mut metadata = self.metadata.write().await;

        if !key_mappings.contains_key(&key_hash) {
            if let Some(max_keys) = config.max_keys {
                if metadata.key_count >= max_keys {
                    return Err(crate::RkvsError::Storage(format!(
                        "Maximum number of keys ({}) reached for this namespace", max_keys
                    )));
                }
            }
            key_mappings.insert(key_hash, key.clone());
            metadata.key_count += 1;
        }

        if let Some(old_value) = data.get(&key_hash) {
            metadata.total_size = metadata.total_size - old_value.len() + value.len();
        } else {
            metadata.total_size += value.len();
        }

        data.insert(key_hash, value);
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> bool {
        let key_hash = HashUtils::hash(key);
        
        let mut data = self.data.write().await;
        let mut key_mappings = self.key_mappings.write().await;
        let mut metadata = self.metadata.write().await;

        if let Some(value) = data.remove(&key_hash) {
            key_mappings.remove(&key_hash);
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
        let key_hash = HashUtils::hash(key);
        
        let mut data = self.data.write().await;
        let mut key_mappings = self.key_mappings.write().await;
        let mut metadata = self.metadata.write().await;

        if let Some(value) = data.remove(&key_hash) {
            key_mappings.remove(&key_hash);
            metadata.key_count -= 1;
            metadata.total_size -= value.len();
            Some(value)
        } else {
            None
        }
    }

    pub async fn exists(&self, key: &str) -> bool {
        let key_hash = HashUtils::hash(key);
        let data = self.data.read().await;
        data.contains_key(&key_hash)
    }

    pub async fn list_keys(&self) -> Vec<String> {
        let key_mappings = self.key_mappings.read().await;
        key_mappings.values().cloned().collect()
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

    pub async fn get_all_data(&self) -> (HashMap<[u8; 32], Vec<u8>>, HashMap<[u8; 32], String>, NamespaceMetadata) {
        let data = self.data.read().await;
        let key_mappings = self.key_mappings.read().await;
        let metadata = self.metadata.read().await;
        
        (data.clone(), key_mappings.clone(), metadata.clone())
    }

    pub async fn restore_data(&self, data: HashMap<[u8; 32], Vec<u8>>, key_mappings: HashMap<[u8; 32], String>, metadata: NamespaceMetadata) {
        {
            let mut data_guard = self.data.write().await;
            *data_guard = data;
        }
        
        {
            let mut key_mappings_guard = self.key_mappings.write().await;
            *key_mappings_guard = key_mappings;
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
    ///         println!("Failed with {} errors", result.errors.len());
    ///         for error in &result.errors {
    ///             println!("Error for key '{}': {}", error.key, error.error_message);
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
        let mut key_mappings = self.key_mappings.write().await;
        let mut metadata = self.metadata.write().await;
        let config = self.config.read().await;
        let mut items_to_insert = Vec::new();
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
            if let Some(max_keys) = config.max_keys {
                let key_hash = HashUtils::hash(&key);
                let is_new_key = !data.contains_key(&key_hash);
                if is_new_key && metadata.key_count + new_keys_count >= max_keys {
                    errors.push(BatchError {
                        key: key.clone(),
                        operation: "set".to_string(),
                        error_message: format!(
                            "Key count limit {} would be exceeded", max_keys
                        ),
                        index,
                    });
                    continue;
                }
            }
            
            if !errors.is_empty() {
                continue;
            }
            
            let key_hash = HashUtils::hash(&key);
            if !data.contains_key(&key_hash) {
                new_keys_count += 1;
                total_size_increase += value.len();
            } else {
                let existing_size = data.get(&key_hash).map(|v| v.len()).unwrap_or(0);
                total_size_increase += value.len() - existing_size;
            }
            
            items_to_insert.push((key_hash, key, value));
        }
        
        if !errors.is_empty() {
            return BatchResult {
                data: None,
                total_processed: total_items,
                duration: start.elapsed(),
                errors,
            };
        }
        
        for (key_hash, key, value) in items_to_insert {
            data.insert(key_hash, value);
            key_mappings.insert(key_hash, key);
        }
        
        metadata.key_count += new_keys_count;
        metadata.total_size += total_size_increase;
        
        BatchResult {
            data: Some(()),
            total_processed: total_items,
            duration: start.elapsed(),
            errors: Vec::new(),
        }
    }

    /// Get multiple key-value pairs in a single batch operation
    pub async fn get_multiple(&self, keys: Vec<String>) -> BatchResult<Vec<(String, Vec<u8>)>> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_keys = keys.len();
        
        let data = self.data.read().await;
        let _key_mappings = self.key_mappings.read().await;
        
        let mut items_to_get = Vec::new();
        
        for (index, key) in keys.into_iter().enumerate() {
            let key_hash = HashUtils::hash(&key);
            if let Some(value) = data.get(&key_hash) {
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
                errors,
            };
        }
        
        BatchResult {
            data: Some(items_to_get),
            total_processed: total_keys,
            duration: start.elapsed(),
            errors: Vec::new(),
        }
    }

    /// Delete multiple keys in a single batch operation
    pub async fn delete_multiple(&self, keys: Vec<String>) -> BatchResult<()> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_keys = keys.len();
        
        let mut data = self.data.write().await;
        let mut key_mappings = self.key_mappings.write().await;
        let mut metadata = self.metadata.write().await;
        
        let mut items_to_delete = Vec::new();
        let mut total_size_decrease = 0;
        let mut deleted_keys_count = 0;
        
        for (index, key) in keys.into_iter().enumerate() {
            let key_hash = HashUtils::hash(&key);
            if let Some(value) = data.get(&key_hash) {
                if errors.is_empty() {
                    items_to_delete.push((key_hash, key));
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
                errors,
            };
        }
        
        for (key_hash, _key) in items_to_delete {
            data.remove(&key_hash);
            key_mappings.remove(&key_hash);
        }
        metadata.key_count -= deleted_keys_count;
        metadata.total_size -= total_size_decrease;
        
        BatchResult {
            data: Some(()),
            total_processed: total_keys,
            duration: start.elapsed(),
            errors: Vec::new(),
        }
    }

    /// Consume multiple key-value pairs in a single batch operation
    pub async fn consume_multiple(&self, keys: Vec<String>) -> BatchResult<Vec<(String, Vec<u8>)>> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_keys = keys.len();
        
        let mut data = self.data.write().await;
        let mut key_mappings = self.key_mappings.write().await;
        let mut metadata = self.metadata.write().await;
        
        let mut items_to_consume = Vec::new();
        let mut total_size_decrease = 0;
        let mut consumed_keys_count = 0;
        
        for (index, key) in keys.into_iter().enumerate() {
            let key_hash = HashUtils::hash(&key);
            if let Some(value) = data.get(&key_hash) {
                if errors.is_empty() {
                    items_to_consume.push((key_hash, key, value.clone()));
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
                errors,
            };
        }
        
        let mut results = Vec::new();
        for (key_hash, key, value) in items_to_consume {
            data.remove(&key_hash);
            key_mappings.remove(&key_hash);
            results.push((key, value));
        }
        metadata.key_count -= consumed_keys_count;
        metadata.total_size -= total_size_decrease;
        
        BatchResult {
            data: Some(results),
            total_processed: total_keys,
            duration: start.elapsed(),
            errors: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_namespace_creation() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test_namespace".to_string(), config);
        
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.name, "test_namespace");
        assert_eq!(metadata.key_count, 0);
        assert_eq!(metadata.total_size, 0);
    }

    #[tokio::test]
    async fn test_set_and_get() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        let key = "test_key".to_string();
        let value = b"test_value".to_vec();
        
        namespace.set(key.clone(), value.clone()).await.unwrap();
        
        let retrieved = namespace.get(&key).await;
        assert_eq!(retrieved, Some(value));
    }

    #[tokio::test]
    async fn test_set_and_get_multiple() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        namespace.set("key3".to_string(), b"value3".to_vec()).await.unwrap();
        
        assert_eq!(namespace.get("key1").await, Some(b"value1".to_vec()));
        assert_eq!(namespace.get("key2").await, Some(b"value2".to_vec()));
        assert_eq!(namespace.get("key3").await, Some(b"value3".to_vec()));
        
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 3);
        assert_eq!(metadata.total_size, 18);
    }

    #[tokio::test]
    async fn test_delete() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        assert!(namespace.exists("key1").await);
        
        let deleted = namespace.delete("key1").await;
        assert!(deleted);
        assert!(!namespace.exists("key1").await);
        let not_deleted = namespace.delete("nonexistent").await;
        assert!(!not_deleted);
    }

    #[tokio::test]
    async fn test_exists() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        assert!(!namespace.exists("nonexistent").await);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        assert!(namespace.exists("key1").await);
    }

    #[tokio::test]
    async fn test_list_keys() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        let keys = namespace.list_keys().await;
        assert!(keys.is_empty());
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        namespace.set("key3".to_string(), b"value3".to_vec()).await.unwrap();
        
        let keys = namespace.list_keys().await;
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
        assert!(keys.contains(&"key3".to_string()));
    }

    #[tokio::test]
    async fn test_metadata_updates() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        let initial_metadata = namespace.get_metadata().await;
        assert_eq!(initial_metadata.key_count, 0);
        assert_eq!(initial_metadata.total_size, 0);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 1);
        assert_eq!(metadata.total_size, 6);
        
        namespace.set("key1".to_string(), b"longer_value".to_vec()).await.unwrap();
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 1);
        assert_eq!(metadata.total_size, 12);
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 2);
        assert_eq!(metadata.total_size, 18);
    }

    #[tokio::test]
    async fn test_max_keys_limit() {
        let config = NamespaceConfig {
            max_keys: Some(2),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        let result = namespace.set("key3".to_string(), b"value3".to_vec()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Maximum number of keys"));
    }

    #[tokio::test]
    async fn test_max_value_size_limit() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(10),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"12345".to_vec()).await.unwrap();
        let result = namespace.set("key2".to_string(), b"12345678901".to_vec()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exceeds maximum allowed size"));
    }

    #[tokio::test]
    async fn test_get_config() {
        let config = NamespaceConfig {
            max_keys: Some(5),
            max_value_size: Some(50),
        };
        let namespace = Namespace::new("test".to_string(), config.clone());
        
        let retrieved_config = namespace.get_config().await;
        assert_eq!(retrieved_config.max_keys, Some(5));
        assert_eq!(retrieved_config.max_value_size, Some(50));
    }

    #[tokio::test]
    async fn test_update_config() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        let new_config = NamespaceConfig {
            max_keys: Some(5),
            max_value_size: Some(50),
        };
        namespace.update_config(new_config).await.unwrap();
        
        let retrieved_config = namespace.get_config().await;
        assert_eq!(retrieved_config.max_keys, Some(5));
        assert_eq!(retrieved_config.max_value_size, Some(50));
    }

    #[tokio::test]
    async fn test_update_config_validation() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        let invalid_config = NamespaceConfig {
            max_keys: Some(1), // Only 1 key allowed, but we have 2
            max_value_size: Some(50),
        };
        let result = namespace.update_config(invalid_config).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot set max_keys to 1 when namespace already has 2 keys"));
        
        let valid_config = NamespaceConfig {
            max_keys: Some(5), // More than current key count
            max_value_size: Some(50),
        };
        namespace.update_config(valid_config).await.unwrap();
    }

    #[tokio::test]
    async fn test_get_all_data() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        
        let (data, key_mappings, metadata) = namespace.get_all_data().await;
        
        assert_eq!(data.len(), 2);
        assert_eq!(key_mappings.len(), 2);
        assert_eq!(metadata.key_count, 2);
        assert_eq!(metadata.total_size, 12);
        let key1_hash = HashUtils::hash("key1");
        let key2_hash = HashUtils::hash("key2");
        
        assert!(data.contains_key(&key1_hash));
        assert!(data.contains_key(&key2_hash));
        assert!(key_mappings.contains_key(&key1_hash));
        assert!(key_mappings.contains_key(&key2_hash));
    }

    #[tokio::test]
    async fn test_restore_data() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        let mut data = HashMap::new();
        let mut key_mappings = HashMap::new();
        let metadata = NamespaceMetadata {
            name: "restored".to_string(),
            key_count: 2,
            total_size: 12,
        };
        
        let key1_hash = HashUtils::hash("key1");
        let key2_hash = HashUtils::hash("key2");
        
        data.insert(key1_hash, b"value1".to_vec());
        data.insert(key2_hash, b"value2".to_vec());
        key_mappings.insert(key1_hash, "key1".to_string());
        key_mappings.insert(key2_hash, "key2".to_string());
        
        namespace.restore_data(data, key_mappings, metadata).await;
        
        assert_eq!(namespace.get("key1").await, Some(b"value1".to_vec()));
        assert_eq!(namespace.get("key2").await, Some(b"value2".to_vec()));
        
        let restored_metadata = namespace.get_metadata().await;
        assert_eq!(restored_metadata.name, "restored");
        assert_eq!(restored_metadata.key_count, 2);
        assert_eq!(restored_metadata.total_size, 12);
    }

    #[tokio::test]
    async fn test_consume() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 2);
        assert_eq!(metadata.total_size, 12);
        
        let consumed_value = namespace.consume("key1").await;
        assert_eq!(consumed_value, Some(b"value1".to_vec()));
        
        assert_eq!(namespace.get("key1").await, None);
        assert_eq!(namespace.exists("key1").await, false);
        
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 1);
        assert_eq!(metadata.total_size, 6);
        
        assert_eq!(namespace.get("key2").await, Some(b"value2".to_vec()));
        
        let consumed_value = namespace.consume("nonexistent").await;
        assert_eq!(consumed_value, None);
        
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 1);
        assert_eq!(metadata.total_size, 6);
    }

    #[tokio::test]
    async fn test_consume_all_keys() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        let keys = vec!["key1", "key2", "key3"];
        for key in &keys {
            namespace.set(key.to_string(), format!("value_{}", key).into_bytes()).await.unwrap();
        }
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 3);
        
        for key in &keys {
            let consumed_value = namespace.consume(key).await;
            assert_eq!(consumed_value, Some(format!("value_{}", key).into_bytes()));
        }
        
        for key in &keys {
            assert_eq!(namespace.get(key).await, None);
            assert_eq!(namespace.exists(key).await, false);
        }
        
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 0);
        assert_eq!(metadata.total_size, 0);
    }

    #[tokio::test]
    async fn test_set_multiple_success() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        let items = vec![
            ("key1".to_string(), b"value1".to_vec()),
            ("key2".to_string(), b"value2".to_vec()),
            ("key3".to_string(), b"value3".to_vec()),
        ];
        
        let result = namespace.set_multiple(items).await;
        
        assert!(result.is_success());
        assert!(!result.has_errors());
        assert_eq!(result.total_processed, 3);
        assert_eq!(result.success_rate(), 1.0);
        
        assert_eq!(namespace.get("key1").await, Some(b"value1".to_vec()));
        assert_eq!(namespace.get("key2").await, Some(b"value2".to_vec()));
        assert_eq!(namespace.get("key3").await, Some(b"value3".to_vec()));
        
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 3);
        assert_eq!(metadata.total_size, 18);
    }

    #[tokio::test]
    async fn test_set_multiple_validation_error() {
        let config = NamespaceConfig {
            max_keys: Some(2),
            max_value_size: Some(5),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        let items = vec![
            ("key1".to_string(), b"val1".to_vec()), // OK (4 bytes)
            ("key2".to_string(), b"toolongvalue".to_vec()), // Too long (12 bytes)
            ("key3".to_string(), b"val3".to_vec()), // Would exceed key limit (4 bytes)
        ];
        
        let result = namespace.set_multiple(items).await;
        
        assert!(!result.is_success());
        assert!(result.has_errors());
        assert_eq!(result.total_processed, 3);
        assert_eq!(result.errors.len(), 1); // key2 fails on size
        assert_eq!(result.errors[0].key, "key2");
        assert_eq!(result.errors[0].operation, "set");
        assert!(result.errors[0].error_message.contains("exceeds maximum"));
        
        assert_eq!(namespace.get("key1").await, None);
        assert_eq!(namespace.get("key2").await, None);
        assert_eq!(namespace.get("key3").await, None);
    }

    #[tokio::test]
    async fn test_get_multiple_success() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        namespace.set("key3".to_string(), b"value3".to_vec()).await.unwrap();
        
        let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        let result = namespace.get_multiple(keys).await;
        
        assert!(result.is_success());
        assert!(!result.has_errors());
        assert_eq!(result.total_processed, 3);
        
        if let Some(data) = result.data {
            assert_eq!(data.len(), 3);
            let mut keys: Vec<String> = data.iter().map(|(k, _)| k.clone()).collect();
            keys.sort();
            assert_eq!(keys, vec!["key1", "key2", "key3"]);
        }
    }

    #[tokio::test]
    async fn test_get_multiple_missing_key() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        
        let keys = vec!["key1".to_string(), "missing".to_string(), "key2".to_string()];
        let result = namespace.get_multiple(keys).await;
        
        assert!(!result.is_success());
        assert!(result.has_errors());
        assert_eq!(result.total_processed, 3);
        assert_eq!(result.errors.len(), 2); // Both missing keys cause errors
        assert_eq!(result.errors[0].key, "missing");
        assert_eq!(result.errors[0].operation, "get");
        assert_eq!(result.errors[1].key, "key2");
        assert_eq!(result.errors[1].operation, "get");
    }

    #[tokio::test]
    async fn test_delete_multiple_success() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        namespace.set("key3".to_string(), b"value3".to_vec()).await.unwrap();
        
        let keys = vec!["key1".to_string(), "key2".to_string()];
        let result = namespace.delete_multiple(keys).await;
        
        assert!(result.is_success());
        assert!(!result.has_errors());
        assert_eq!(result.total_processed, 2);
        
        assert_eq!(namespace.get("key1").await, None);
        assert_eq!(namespace.get("key2").await, None);
        assert_eq!(namespace.get("key3").await, Some(b"value3".to_vec()));
        
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 1);
        assert_eq!(metadata.total_size, 6);
    }

    #[tokio::test]
    async fn test_delete_multiple_missing_key() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        
        let keys = vec!["key1".to_string(), "missing".to_string()];
        let result = namespace.delete_multiple(keys).await;
        
        assert!(!result.is_success());
        assert!(result.has_errors());
        assert_eq!(result.total_processed, 2);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].key, "missing");
        assert_eq!(result.errors[0].operation, "delete");
        
        assert_eq!(namespace.get("key1").await, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_consume_multiple_success() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        namespace.set("key3".to_string(), b"value3".to_vec()).await.unwrap();
        
        let keys = vec!["key1".to_string(), "key2".to_string()];
        let result = namespace.consume_multiple(keys).await;
        
        assert!(result.is_success());
        assert!(!result.has_errors());
        assert_eq!(result.total_processed, 2);
        
        if let Some(data) = result.data {
            assert_eq!(data.len(), 2);
            let mut keys: Vec<String> = data.iter().map(|(k, _)| k.clone()).collect();
            keys.sort();
            assert_eq!(keys, vec!["key1", "key2"]);
        }
        
        assert_eq!(namespace.get("key1").await, None);
        assert_eq!(namespace.get("key2").await, None);
        assert_eq!(namespace.get("key3").await, Some(b"value3".to_vec()));
        
        let metadata = namespace.get_metadata().await;
        assert_eq!(metadata.key_count, 1);
        assert_eq!(metadata.total_size, 6);
    }

    #[tokio::test]
    async fn test_consume_multiple_missing_key() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Namespace::new("test".to_string(), config);
        
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        
        let keys = vec!["key1".to_string(), "missing".to_string()];
        let result = namespace.consume_multiple(keys).await;
        
        assert!(!result.is_success());
        assert!(result.has_errors());
        assert_eq!(result.total_processed, 2);
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].key, "missing");
        assert_eq!(result.errors[0].operation, "consume");
        
        assert_eq!(namespace.get("key1").await, Some(b"value1".to_vec()));
    }
}

