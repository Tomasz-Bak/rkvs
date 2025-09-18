use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};

use crate::Result;
use super::types::{NamespaceStats, StorageConfig, NamespaceConfig, HashUtils};
use super::namespace::Namespace;
use super::persistence::FilePersistence;

/// Storage manager for hashtable-based key-value storage with namespaces
/// 
/// The `StorageManager` is the main entry point for working with RKVS. It manages
/// multiple namespaces and provides persistence capabilities.
/// 
/// # Example
/// 
/// ```rust
/// use rkvs::{StorageManager, StorageConfig, NamespaceConfig};
/// 
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let storage = StorageManager::builder()
///         .with_config(StorageConfig::default())
///         .with_persistence("/tmp/rkvs_data".into())
///         .build();
///     
///     storage.initialize().await?;
///     
///     let config = NamespaceConfig {
///         max_keys: Some(1000),
///         max_value_size: Some(1024 * 1024),
///     };
///     let ns_hash = storage.create_namespace("my_app", Some(config)).await?;
///     
///     let namespace = storage.namespace(ns_hash).await?;
///     namespace.set("key".to_string(), b"value".to_vec()).await?;
///     
///     Ok(())
/// }
/// ```
pub struct StorageManager {
    namespaces: Arc<Mutex<HashMap<[u8; 32], Arc<Namespace>>>>,
    config: StorageConfig,
    persistence: Option<FilePersistence>,
    initialized: Arc<AtomicBool>,
}

/// Builder for creating StorageManager instances
/// 
/// Provides a fluent API for configuring and creating `StorageManager` instances.
/// 
/// # Example
/// 
/// ```rust
/// use rkvs::{StorageManager, StorageConfig};
/// 
/// let storage = StorageManager::builder()
///     .with_config(StorageConfig {
///         max_namespaces: Some(100),
///         default_max_keys_per_namespace: Some(10000),
///         default_max_value_size: Some(1024 * 1024),
///     })
///     .with_persistence("/data/rkvs".into())
///     .build();
/// ```
pub struct StorageManagerBuilder {
    config: Option<StorageConfig>,
    persistence: Option<FilePersistence>,
}

impl StorageManagerBuilder {
    /// Create a new builder with default configuration
    pub fn new() -> Self {
        Self { 
            config: None,
            persistence: None,
        }
    }
    
    /// Set the storage configuration
    /// 
    /// # Arguments
    /// 
    /// * `config` - The storage configuration to use
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::{StorageManager, StorageConfig};
    /// 
    /// let storage = StorageManager::builder()
    ///     .with_config(StorageConfig {
    ///         max_namespaces: Some(100),
    ///         default_max_keys_per_namespace: Some(10000),
    ///         default_max_value_size: Some(1024 * 1024),
    ///     })
    ///     .build();
    /// ```
    pub fn with_config(mut self, config: StorageConfig) -> Self {
        self.config = Some(config);
        self
    }
    
    /// Enable file-based persistence
    /// 
    /// # Arguments
    /// 
    /// * `path` - The directory path where data will be stored
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::StorageManager;
    /// 
    /// let storage = StorageManager::builder()
    ///     .with_persistence("/data/rkvs".into())
    ///     .build();
    /// ```
    pub fn with_persistence(mut self, path: PathBuf) -> Self {
        self.persistence = Some(FilePersistence::new(path));
        self
    }
    
    /// Build the StorageManager with the configured options
    /// 
    /// # Returns
    /// 
    /// A new `StorageManager` instance with the specified configuration.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::StorageManager;
    /// 
    /// let storage = StorageManager::builder()
    ///     .with_persistence("/tmp/rkvs".into())
    ///     .build();
    /// ```
    pub fn build(self) -> StorageManager {
        let config = self.config.unwrap_or_default();
        StorageManager::new(config, self.persistence)
    }
}

impl StorageManager {
    fn new(config: StorageConfig, persistence: Option<FilePersistence>) -> Self {
        Self {
            namespaces: Arc::new(Mutex::new(HashMap::new())),
            config,
            persistence,
            initialized: Arc::new(AtomicBool::new(false)),
        }
    }
    
    /// Initialize storage (call this once at startup)
    /// 
    /// This method must be called before performing any operations on the storage.
    /// If persistence is enabled, it will load existing data from disk.
    /// 
    /// # Returns
    /// 
    /// Returns `Ok(())` if initialization succeeds, or an error if it fails.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::StorageManager;
    /// 
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let storage = StorageManager::builder()
    ///         .with_persistence("/tmp/rkvs".into())
    ///         .build();
    ///     
    ///     // Initialize before use
    ///     storage.initialize().await?;
    ///     
    ///     // Now you can use the storage
    ///     Ok(())
    /// }
    /// ```
    pub async fn initialize(&self) -> Result<()> {
        if self.initialized.load(Ordering::Relaxed) {
            return Ok(());
        }
        
        if let Some(persistence) = &self.persistence {
            let data = persistence.load().await?;
            let mut namespaces = self.namespaces.lock().unwrap();
            *namespaces = data;
        }
        
        self.initialized.store(true, Ordering::Relaxed);
        Ok(())
    }
    
    /// Save storage to disk (only works if persistence is enabled)
    /// 
    /// Persists all namespace data to disk. This method only works if persistence
    /// was enabled when creating the StorageManager.
    /// 
    /// # Returns
    /// 
    /// Returns `Ok(())` if save succeeds, or an error if it fails.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::StorageManager;
    /// 
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let storage = StorageManager::builder()
    ///         .with_persistence("/tmp/rkvs".into())
    ///         .build();
    ///     
    ///     storage.initialize().await?;
    ///     
    ///     // ... perform operations ...
    ///     
    ///     // Save to disk
    ///     storage.save().await?;
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub async fn save(&self) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            let namespaces = self.namespaces.lock().unwrap();
            persistence.save(&namespaces).await?;
        }
        Ok(())
    }
    
    /// Check if storage is initialized
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Relaxed)
    }
    
    /// Ensure storage is initialized before operations
    async fn ensure_initialized(&self) -> Result<()> {
        if !self.is_initialized() {
            return Err(crate::RkvsError::Storage("Storage not initialized. Call initialize() first.".to_string()));
        }
        Ok(())
    }

    /// Create a new namespace with optional custom configuration
    /// 
    /// Creates a new namespace with the given name and optional configuration.
    /// If no configuration is provided, default values from StorageConfig are used.
    /// 
    /// # Arguments
    /// 
    /// * `name` - The name of the namespace to create
    /// * `config` - Optional namespace-specific configuration
    /// 
    /// # Returns
    /// 
    /// Returns the namespace hash that can be used to access the namespace,
    /// or an error if creation fails.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::{StorageManager, NamespaceConfig};
    /// 
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let storage = StorageManager::builder().build();
    ///     storage.initialize().await?;
    ///     
    ///     // Create namespace with default config
    ///     let ns_hash = storage.create_namespace("my_app", None).await?;
    ///     
    ///     // Create namespace with custom config
    ///     let config = NamespaceConfig {
    ///         max_keys: Some(1000),
    ///         max_value_size: Some(1024 * 1024),
    ///     };
    ///     let ns_hash = storage.create_namespace("my_app", Some(config)).await?;
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub async fn create_namespace(&self, name: &str, config: Option<NamespaceConfig>) -> Result<[u8; 32]> {
        self.ensure_initialized().await?;
        
        let namespace_hash = HashUtils::hash(name);
        
        let mut namespaces = self.namespaces.lock().unwrap();
        
        if namespaces.contains_key(&namespace_hash) {
            return Err(crate::RkvsError::Storage(format!("Namespace '{}' already exists", name)));
        }
        
        if let Some(max_namespaces) = self.config.max_namespaces {
            if namespaces.len() >= max_namespaces {
                return Err(crate::RkvsError::Storage(format!(
                    "Maximum number of namespaces ({}) reached", max_namespaces
                )));
            }
        }
        
        let namespace_config = config.unwrap_or_else(|| NamespaceConfig {
            max_keys: self.config.default_max_keys_per_namespace,
            max_value_size: self.config.default_max_value_size,
        });
        
        let namespace = Arc::new(Namespace::new(name.to_string(), namespace_config));
        namespaces.insert(namespace_hash, namespace);
        Ok(namespace_hash)
    }

    /// Create a new namespace with custom configuration
    pub async fn create_namespace_with_config(&self, name: &str, config: NamespaceConfig) -> Result<[u8; 32]> {
        self.create_namespace(name, Some(config)).await
    }

    /// Hash a namespace name for internal use
    pub fn hash_namespace_name(&self, name: &str) -> [u8; 32] {
        HashUtils::hash(name)
    }

    /// Update the configuration of an existing namespace using a pre-computed hash
    pub async fn update_namespace_config(&self, namespace_hash: [u8; 32], new_config: NamespaceConfig) -> Result<()> {
        self.ensure_initialized().await?;
        
        let namespaces = self.namespaces.lock().unwrap();
        
        if let Some(namespace) = namespaces.get(&namespace_hash) {
            namespace.update_config(new_config).await
        } else {
            Err(crate::RkvsError::Storage(format!("Namespace with hash {:02x?} does not exist", namespace_hash)))
        }
    }

    /// Get a namespace handle using a pre-computed hash
    /// Returns an error if the namespace doesn't exist
    pub async fn namespace(&self, namespace_hash: [u8; 32]) -> Result<Arc<Namespace>> {
        self.ensure_initialized().await?;
        
        let namespaces = self.namespaces.lock().unwrap();
        
        if let Some(namespace) = namespaces.get(&namespace_hash) {
            Ok(namespace.clone())
        } else {
            Err(crate::RkvsError::Storage(format!("Namespace with hash {:02x?} does not exist", namespace_hash)))
        }
    }



    /// Delete an entire namespace using a pre-computed hash
    pub async fn delete_namespace(&self, namespace_hash: [u8; 32]) -> Result<()> {
        self.ensure_initialized().await?;
        
        let mut namespaces = self.namespaces.lock().unwrap();
        
        if namespaces.remove(&namespace_hash).is_some() {
            Ok(())
        } else {
            Err(crate::RkvsError::Storage(format!("Namespace with hash {:02x?} does not exist", namespace_hash)))
        }
    }


    /// List all namespaces
    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        self.ensure_initialized().await?;
        
        let namespaces = self.namespaces.lock().unwrap();
        let mut names = Vec::new();
        
        for namespace in namespaces.values() {
            let metadata = namespace.get_metadata().await;
            names.push(metadata.name);
        }
        
        Ok(names)
    }

    /// Get namespace statistics using a pre-computed hash
    pub async fn get_namespace_stats(&self, namespace_hash: [u8; 32]) -> Result<Option<NamespaceStats>> {
        self.ensure_initialized().await?;
        
        let ns = self.namespace(namespace_hash).await?;
        let metadata = ns.get_metadata().await;
        
        Ok(Some(NamespaceStats {
            name: metadata.name,
            key_count: metadata.key_count,
            total_size: metadata.total_size,
        }))
    }

    /// Get all namespace statistics
    pub async fn get_all_namespace_stats(&self) -> Result<Vec<NamespaceStats>> {
        self.ensure_initialized().await?;
        
        let namespaces = self.namespaces.lock().unwrap();
        let mut stats = Vec::new();
        
        for namespace in namespaces.values() {
            let metadata = namespace.get_metadata().await;
            stats.push(NamespaceStats {
                name: metadata.name,
                key_count: metadata.key_count,
                total_size: metadata.total_size,
            });
        }
        
        Ok(stats)
    }

    /// Get total number of namespaces
    pub async fn get_namespace_count(&self) -> Result<usize> {
        self.ensure_initialized().await?;
        
        let namespaces = self.namespaces.lock().unwrap();
        Ok(namespaces.len())
    }

    /// Get total number of keys across all namespaces
    pub async fn get_total_key_count(&self) -> Result<usize> {
        self.ensure_initialized().await?;
        
        let namespaces = self.namespaces.lock().unwrap();
        let mut total = 0;
        
        for namespace in namespaces.values() {
            let metadata = namespace.get_metadata().await;
            total += metadata.key_count;
        }
        
        Ok(total)
    }

    /// Get storage configuration
    pub fn get_config(&self) -> &StorageConfig {
        &self.config
    }

    /// Update storage configuration
    pub fn update_config(&mut self, config: StorageConfig) {
        self.config = config;
    }

    /// Check if persistence is enabled
    pub fn has_persistence(&self) -> bool {
        self.persistence.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_storage() -> StorageManager {
        StorageManagerBuilder::new()
            .with_config(StorageConfig {
                max_namespaces: Some(10),
                default_max_keys_per_namespace: Some(100),
                default_max_value_size: Some(1024),
            })
            .build()
    }


    #[tokio::test]
    async fn test_storage_creation() {
        let storage = create_test_storage();
        assert!(!storage.has_persistence());
        assert!(!storage.is_initialized());
    }

    #[tokio::test]
    async fn test_storage_initialization() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        assert!(storage.is_initialized());
    }

    #[tokio::test]
    async fn test_create_namespace() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        let namespace_hash = storage.create_namespace("test_namespace", None).await.unwrap();
        assert!(!namespace_hash.is_empty());
        
        // Try to create the same namespace again - should fail
        let result = storage.create_namespace("test_namespace", None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_create_namespace_with_config() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        let custom_config = NamespaceConfig {
            max_keys: Some(50),
            max_value_size: Some(512),
        };
        
        let namespace_hash = storage.create_namespace_with_config("test", custom_config).await.unwrap();
        assert!(!namespace_hash.is_empty());
    }

    #[tokio::test]
    async fn test_namespace_operations() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        let namespace_hash = storage.create_namespace("test", None).await.unwrap();
        let namespace = storage.namespace(namespace_hash).await.unwrap();
        
        // Test basic operations
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        assert_eq!(namespace.get("key1").await, Some(b"value1".to_vec()));
        assert!(namespace.exists("key1").await);
        
        let keys = namespace.list_keys().await;
        assert_eq!(keys.len(), 1);
        assert!(keys.contains(&"key1".to_string()));
    }

    #[tokio::test]
    async fn test_delete_namespace() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        let namespace_hash = storage.create_namespace("test", None).await.unwrap();
        
        // Delete the namespace
        storage.delete_namespace(namespace_hash).await.unwrap();
        
        // Try to access the deleted namespace - should fail
        let result = storage.namespace(namespace_hash).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not exist"));
    }

    #[tokio::test]
    async fn test_list_namespaces() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        // Initially empty
        let namespaces = storage.list_namespaces().await.unwrap();
        assert!(namespaces.is_empty());
        
        // Add some namespaces
        storage.create_namespace("namespace1", None).await.unwrap();
        storage.create_namespace("namespace2", None).await.unwrap();
        storage.create_namespace("namespace3", None).await.unwrap();
        
        let namespaces = storage.list_namespaces().await.unwrap();
        assert_eq!(namespaces.len(), 3);
        assert!(namespaces.contains(&"namespace1".to_string()));
        assert!(namespaces.contains(&"namespace2".to_string()));
        assert!(namespaces.contains(&"namespace3".to_string()));
    }

    #[tokio::test]
    async fn test_get_namespace_stats() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        let namespace_hash = storage.create_namespace("test", None).await.unwrap();
        let namespace = storage.namespace(namespace_hash).await.unwrap();
        
        // Add some data
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        
        let stats = storage.get_namespace_stats(namespace_hash).await.unwrap().unwrap();
        assert_eq!(stats.name, "test");
        assert_eq!(stats.key_count, 2);
        assert_eq!(stats.total_size, 12);
    }

    #[tokio::test]
    async fn test_get_all_namespace_stats() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        // Create multiple namespaces with data
        let ns1_hash = storage.create_namespace("ns1", None).await.unwrap();
        let ns1 = storage.namespace(ns1_hash).await.unwrap();
        ns1.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        
        let ns2_hash = storage.create_namespace("ns2", None).await.unwrap();
        let ns2 = storage.namespace(ns2_hash).await.unwrap();
        ns2.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        ns2.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        
        let all_stats = storage.get_all_namespace_stats().await.unwrap();
        assert_eq!(all_stats.len(), 2);
        
        // Find the stats for each namespace
        let ns1_stats = all_stats.iter().find(|s| s.name == "ns1").unwrap();
        assert_eq!(ns1_stats.key_count, 1);
        assert_eq!(ns1_stats.total_size, 6);
        
        let ns2_stats = all_stats.iter().find(|s| s.name == "ns2").unwrap();
        assert_eq!(ns2_stats.key_count, 2);
        assert_eq!(ns2_stats.total_size, 12);
    }

    #[tokio::test]
    async fn test_get_namespace_count() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        assert_eq!(storage.get_namespace_count().await.unwrap(), 0);
        
        storage.create_namespace("ns1", None).await.unwrap();
        assert_eq!(storage.get_namespace_count().await.unwrap(), 1);
        
        storage.create_namespace("ns2", None).await.unwrap();
        assert_eq!(storage.get_namespace_count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_get_total_key_count() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        assert_eq!(storage.get_total_key_count().await.unwrap(), 0);
        
        let ns1_hash = storage.create_namespace("ns1", None).await.unwrap();
        let ns1 = storage.namespace(ns1_hash).await.unwrap();
        ns1.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        ns1.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        
        assert_eq!(storage.get_total_key_count().await.unwrap(), 2);
        
        let ns2_hash = storage.create_namespace("ns2", None).await.unwrap();
        let ns2 = storage.namespace(ns2_hash).await.unwrap();
        ns2.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        
        assert_eq!(storage.get_total_key_count().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_max_namespaces_limit() {
        let storage = StorageManagerBuilder::new()
            .with_config(StorageConfig {
                max_namespaces: Some(2),
                default_max_keys_per_namespace: Some(100),
                default_max_value_size: Some(1024),
            })
            .build();
        
        storage.initialize().await.unwrap();
        
        // Create namespaces up to the limit
        storage.create_namespace("ns1", None).await.unwrap();
        storage.create_namespace("ns2", None).await.unwrap();
        
        // Try to create one more - should fail
        let result = storage.create_namespace("ns3", None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Maximum number of namespaces"));
    }

    #[tokio::test]
    async fn test_persistence_save_and_load() {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().join("test_storage.bin");
        
        // Create first storage instance
        let storage = StorageManagerBuilder::new()
            .with_config(StorageConfig {
                max_namespaces: Some(10),
                default_max_keys_per_namespace: Some(100),
                default_max_value_size: Some(1024),
            })
            .with_persistence(storage_path.clone())
            .build();
        
        storage.initialize().await.unwrap();
        
        // Add some data
        let ns_hash = storage.create_namespace("test", None).await.unwrap();
        let namespace = storage.namespace(ns_hash).await.unwrap();
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        
        // Save to disk
        storage.save().await.unwrap();
        
        // Create a new storage instance with the same path and load from disk
        let loaded_storage = StorageManagerBuilder::new()
            .with_config(StorageConfig {
                max_namespaces: Some(10),
                default_max_keys_per_namespace: Some(100),
                default_max_value_size: Some(1024),
            })
            .with_persistence(storage_path)
            .build();
        
        loaded_storage.initialize().await.unwrap();
        
        // Verify the data was loaded
        let namespaces = loaded_storage.list_namespaces().await.unwrap();
        assert_eq!(namespaces.len(), 1);
        assert!(namespaces.contains(&"test".to_string()));
        
        let loaded_ns = loaded_storage.namespace(ns_hash).await.unwrap();
        assert_eq!(loaded_ns.get("key1").await, Some(b"value1".to_vec()));
        assert_eq!(loaded_ns.get("key2").await, Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_ensure_initialized() {
        let storage = create_test_storage();
        
        // Try to use storage before initialization - should fail
        let result = storage.create_namespace("test", None).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not initialized"));
        
        // Initialize and try again - should work
        storage.initialize().await.unwrap();
        let result = storage.create_namespace("test", None).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_hash_namespace_name() {
        let storage = create_test_storage();
        
        let hash1 = storage.hash_namespace_name("test");
        let hash2 = storage.hash_namespace_name("test");
        let hash3 = storage.hash_namespace_name("different");
        
        assert_eq!(hash1, hash2); // Same input should produce same hash
        assert_ne!(hash1, hash3); // Different input should produce different hash
        assert_eq!(hash1.len(), 32); // Should be 32 bytes
    }

    #[tokio::test]
    async fn test_update_namespace_config() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        let namespace_hash = storage.create_namespace("test", None).await.unwrap();
        let namespace = storage.namespace(namespace_hash).await.unwrap();
        
        // Add some data
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        
        // Update config with valid limits
        let new_config = NamespaceConfig {
            max_keys: Some(5),
            max_value_size: Some(50),
        };
        storage.update_namespace_config(namespace_hash, new_config).await.unwrap();
        
        // Verify the config was updated
        let updated_config = namespace.get_config().await;
        assert_eq!(updated_config.max_keys, Some(5));
        assert_eq!(updated_config.max_value_size, Some(50));
    }

    #[tokio::test]
    async fn test_update_namespace_config_validation() {
        let storage = create_test_storage();
        storage.initialize().await.unwrap();
        
        let namespace_hash = storage.create_namespace("test", None).await.unwrap();
        let namespace = storage.namespace(namespace_hash).await.unwrap();
        
        // Add some data
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        
        // Try to update with max_keys smaller than current key count - should fail
        let invalid_config = NamespaceConfig {
            max_keys: Some(1), // Only 1 key allowed, but we have 2
            max_value_size: Some(50),
        };
        let result = storage.update_namespace_config(namespace_hash, invalid_config).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot set max_keys to 1 when namespace already has 2 keys"));
    }
}