//! Contains the `Namespace` struct, the primary handle for all key-value operations,
//! sharding logic, and configuration management for a single, isolated data store.
use std::sync::Arc;
use std::time::Instant;
use std::collections::HashMap;
use tokio::sync::RwLock;
use crate::{Result, RkvsError};
use super::data_table::{DataTable};
use super::types::{NamespaceConfig, NamespaceMetadata, BatchResult, BatchError, NamespaceSnapshot};

/// Batch operation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchMode {
    /// All operations must succeed or none are applied
    AllOrNothing,
    /// Continue processing on errors, apply successful operations
    BestEffort,
}

/// Jump consistent hash implementation
/// Returns a shard index in the range [0, num_shards)
fn jump_consistent_hash(key: &[u8], num_shards: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(key, &mut hasher);
    let mut hash = std::hash::Hasher::finish(&hasher);
    
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    
    while j < num_shards as i64 {
        b = j;
        hash = hash.wrapping_mul(2862933555777941757).wrapping_add(1);
        j = ((b.wrapping_add(1) as f64) * ((1i64 << 31) as f64) / (((hash >> 33).wrapping_add(1)) as f64)) as i64;
    }
    
    b as usize
}

/// Used as a default capacity hint to avoid needless realocation for small key numbers
const DEFAULT_SHARD_CAPACITY: usize = 10000;

/// A namespace handle for working with a specific namespace
#[derive(Debug)]
pub struct Namespace {
    shards: Arc<RwLock<Vec<Arc<RwLock<DataTable>>>>>,
    metadata: NamespaceMetadata,
    config: NamespaceConfig,
}

// MARK: - Core & Construction
impl Namespace {
    /// Creates a new, empty namespace with the given name and configuration.
    pub fn new(name: String, config: NamespaceConfig) -> Self {
        // Ensure shard_count is at least 1
        if config.shard_count() == 0 {
            config.set_shard_count(1);
        }
        // Set unbounded limits if not specified
        if config.max_keys() == 0 {
            config.set_max_keys(usize::MAX);
        }
        if config.max_value_size() == 0 {
            config.set_max_value_size(usize::MAX);
        }

        let shards = (0..config.shard_count())
            .map(|_| Arc::new(RwLock::new(DataTable::with_capacity(DEFAULT_SHARD_CAPACITY))))
            .collect();

        Self {
            shards: Arc::new(RwLock::new(shards)),
            metadata: NamespaceMetadata::new(name),
            config,
        }
    }

    /// Creates a new namespace from existing data, metadata, and configuration.
    /// This is primarily used for restoring a namespace from a snapshot.
    pub fn from_snapshot(snapshot: NamespaceSnapshot) -> Self {
        let shards = snapshot.shards
            .into_iter()
            .map(|table| Arc::new(RwLock::new(table)))
            .collect();
        
        Self {
            shards: Arc::new(RwLock::new(shards)),
            metadata: snapshot.metadata,
            config: snapshot.config,
        }
    }

    /// Creates a complete, serializable snapshot of the namespace's state.
    pub async fn create_snapshot(&self) -> NamespaceSnapshot {
        let shards_guard = self.shards.read().await;
        let mut tables = Vec::with_capacity(shards_guard.len());
        
        for shard in shards_guard.iter() {
            let data = shard.read().await;
            tables.push(data.snapshot());
        }
        
        NamespaceSnapshot { shards: tables, metadata: self.metadata.clone(), config: self.config.clone() }
    }
}

// MARK: - Internal Helpers
impl Namespace {
    /// Helper to acquire a read lock with a configured timeout.
    async fn timeout_read_lock<'a, T>(&self, lock: &'a RwLock<T>, context: &str) -> Result<tokio::sync::RwLockReadGuard<'a, T>> {
        tokio::time::timeout(self.config.lock_timeout(), lock.read())
            .await
            .map_err(|_| RkvsError::Storage(format!("Timeout while waiting to acquire read lock for {}", context)))
    }

    /// Helper to acquire a write lock with a configured timeout.
    async fn timeout_write_lock<'a, T>(&self, lock: &'a RwLock<T>, context: &str) -> Result<tokio::sync::RwLockWriteGuard<'a, T>> {
        tokio::time::timeout(self.config.lock_timeout(), lock.write())
            .await
            .map_err(|_| RkvsError::Storage(format!("Timeout while waiting to acquire write lock for {}", context)))
    }

    /// Get the shard for a given key
    async fn get_shard(&self, key: &[u8]) -> Result<(usize, Arc<RwLock<DataTable>>)> {
        let shards_guard = self.timeout_read_lock(&self.shards, "shard list").await?;
        let shard_count = shards_guard.len();
        let shard_idx = jump_consistent_hash(key, shard_count);
        let shard = Arc::clone(&shards_guard[shard_idx]);
        Ok((shard_idx, shard))
    }
}

// MARK: - Configuration
impl Namespace {
    /// Resizes the namespace to use a new number of shards.
    /// Only supports increasing shard count
    pub async fn resize_shards(&self, new_shard_count: usize) -> Result<()> {
        let mut shards_guard = self.timeout_write_lock(&self.shards, "resize_shards").await?;
        let current_shard_count = shards_guard.len();
        
        if new_shard_count <= current_shard_count {
            return Err(crate::RkvsError::Storage(
                "New shard count must be greater than current shard count".to_string()
            ));
        }

        let num_new_shards = new_shard_count - current_shard_count;
        
        let new_shards: Vec<Arc<RwLock<DataTable>>> = (0..num_new_shards)
            .map(|_| Arc::new(RwLock::new(DataTable::with_capacity(DEFAULT_SHARD_CAPACITY))))
            .collect();

        // Append new shards to existing ones
        shards_guard.extend(new_shards);

        // Now redistribute keys that need to move to new shards
        // We only check the first current_shard_count shards (the old ones)
        for shard_idx in 0..current_shard_count {
            let shard = Arc::clone(&shards_guard[shard_idx]);
            let mut data = self.timeout_write_lock(&shard, &format!("data redistribution from shard {}", shard_idx)).await?;
            let keys_to_check: Vec<_> = data.all_keys();
            
            for key in keys_to_check {
                let new_shard_idx = jump_consistent_hash(&key, new_shard_count);
                
                // If key should move to a different shard
                if new_shard_idx != shard_idx {
                    if let Some(value) = data.delete_value(&key) {
                        // Move to new shard
                        let target_shard = Arc::clone(&shards_guard[new_shard_idx]);
                        let mut target_data = self.timeout_write_lock(&target_shard, &format!("data redistribution to shard {}", new_shard_idx)).await?;
                        target_data.set_value(key, (*value).clone());
                    }
                }
                // If new_shard_idx == shard_idx, key stays where it is
            }
        }

        self.config.set_shard_count(new_shard_count);

        Ok(())
    }

    /// Gets a clone of the namespace's configuration.
    pub async fn get_config(&self) -> NamespaceConfig {
        self.config.clone()
    }

    /// Sets the maximum number of keys allowed in the namespace.
    /// 
    /// # Arguments
    /// 
    /// * `value` - The new maximum key count
    /// 
    /// # Errors
    /// 
    /// Returns an error if the new value is less than the current number of keys
    pub async fn set_max_keys(&self, value: usize) -> Result<()> {
        let new_max = if value == 0 { usize::MAX } else { value };
        if self.metadata.key_count() > new_max {
            return Err(crate::RkvsError::Storage(format!(
                "Cannot set max_keys to {} when namespace already has {} keys",
                new_max,
                self.metadata.key_count()
            )));
        }
        self.config.set_max_keys(new_max);
        Ok(())
    }

    /// Sets the maximum value size allowed in the namespace.
    /// 
    /// # Arguments
    /// 
    /// * `value` - The new maximum value size in bytes
    pub async fn set_max_value_size(&self, value: usize) -> Result<()> {
        let new_max = if value == 0 { usize::MAX } else { value };
        self.config.set_max_value_size(new_max);
        Ok(())
    }

    /// Sets the lock acquisition timeout for this namespace.
    ///
    /// # Arguments
    ///
    /// * `value_ms` - The new timeout in milliseconds
    pub async fn set_lock_timeout(&self, value_ms: usize) -> Result<()> {
        if value_ms == 0 {
            return Err(RkvsError::Storage("Lock timeout must be greater than 0.".to_string()));
        }
        self.config.set_lock_timeout(value_ms);
        Ok(())
    }

    /// Updates multiple config fields at once.
    /// 
    /// Note: This method ignores changes to `shard_count`. The number of shards can only be
    /// changed after initialization via the `resize_shards` method.
    ///
    /// # Arguments
    /// 
    /// * `new_config` - The new configuration to apply
    /// 
    /// # Errors
    /// 
    /// Returns an error if any validation fails (e.g., `max_keys` is set below the current key count).
    pub async fn update_config(&self, new_config: NamespaceConfig) -> Result<()> {
        self.set_max_keys(new_config.max_keys()).await?;
        self.set_max_value_size(new_config.max_value_size()).await?;
        self.set_lock_timeout(new_config.lock_timeout.load(std::sync::atomic::Ordering::SeqCst)).await?;
        Ok(())
    }
}

// MARK: - Single-Key Operations
impl Namespace {
    /// Gets a value by its string key.
    ///
    /// This is a convenience wrapper around `get_bytes`.
    ///
    /// # Arguments
    ///
    /// * `key` - The string slice representing the key to retrieve.
    ///
    /// # Returns
    ///
    /// An `Option` containing an `Arc<Vec<u8>>` with the value if the key exists, otherwise `None`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use rkvs::{StorageManager, Result};
    /// # async fn run() -> Result<()> {
    /// # let storage = StorageManager::builder().build().await?;
    /// # storage.initialize(None).await?;
    /// # storage.create_namespace("my_app", None).await?;
    /// # let namespace = storage.namespace("my_app").await?;
    /// namespace.set("my_key", b"my_value".to_vec()).await?;
    ///
    /// if let Some(value) = namespace.get("my_key").await {
    ///     assert_eq!(*value, b"my_value".to_vec());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.get_bytes(key.as_bytes()).await
    }

    /// Gets a value by its binary key (`&[u8]`).
    /// This is the most direct way to retrieve a value.
    pub async fn get_bytes(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        let (_idx, shard) = self.get_shard(key).await.ok()?;
        let data = match self.timeout_read_lock(&shard, "get_bytes").await {
            Ok(guard) => guard,
            Err(_) => return None,
        };
        data.get_value(key)
    }

    /// Sets a key-value pair using a string key.
    ///
    /// If the key already exists, the value is updated and the old value is returned.
    /// If the key is new, it is inserted and `None` is returned.
    /// This is a convenience wrapper around `set_bytes`.
    ///
    /// # Arguments
    ///
    /// * `key` - The string slice representing the key.
    /// * `value` - The `Vec<u8>` value to store.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option` with the old value if the key was updated,
    /// or `None` if the key was newly inserted.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use rkvs::{StorageManager, Result};
    /// # async fn run() -> Result<()> {
    /// # let storage = StorageManager::builder().build().await?;
    /// # storage.initialize(None).await?;
    /// # storage.create_namespace("my_app", None).await?;
    /// # let namespace = storage.namespace("my_app").await?;
    /// // Insert a new key
    /// let result1 = namespace.set("new_key", b"value1".to_vec()).await?;
    /// assert!(result1.is_none());
    ///
    /// // Update an existing key
    /// let result2 = namespace.set("new_key", b"value2".to_vec()).await?;
    /// assert_eq!(*result2.unwrap(), b"value1".to_vec());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set(&self, key: &str, value: Vec<u8>) -> Result<Option<Arc<Vec<u8>>>> {
        self.set_bytes(key.as_bytes(), value).await
    }

    /// Sets a key-value pair using a binary key (`&[u8]`).
    ///
    /// This is the most direct method for writing data. It performs validation against
    /// configured limits (`max_value_size`, `max_keys`) before writing.
    pub async fn set_bytes(&self, key: &[u8], value: Vec<u8>) -> Result<Option<Arc<Vec<u8>>>> {
        if value.len() > self.config.max_value_size() {
            return Err(crate::RkvsError::Storage(format!(
                "Value size {} exceeds maximum allowed size {}",
                value.len(),
                self.config.max_value_size()
            )));
        }

        let max_keys = self.config.max_keys();
        let (_idx, shard) = self.get_shard(key).await?;
        
        let mut data = self.timeout_write_lock(&shard, "set_bytes").await?;
        
        // Check if we are at the key limit. If so, we can only allow updates, not new keys.
        if self.metadata.key_count() >= max_keys {
            if !data.has_key(key) {
                return Err(crate::RkvsError::Storage(format!(
                    "Maximum number of keys ({}) reached for this namespace", max_keys
                )));
            }
        }

        let old_value = data.set_value(key.to_vec(), value.clone());

        if let Some(old) = old_value {
            self.metadata.update_total_size((value.len() as isize) - (old.len() as isize));

            return Ok(Some(old));
        } else {
            self.metadata.increment_key_count();
            self.metadata.update_total_size(value.len() as isize);

            return Ok(None);
        }
    }

    /// Updates an existing key-value pair using a string key.
    /// This operation will fail if the key does not already exist.
    pub async fn update(&self, key: &str, value: Vec<u8>) -> Result<Arc<Vec<u8>>> {
        self.update_bytes(key.as_bytes(), value).await
    }

    /// Updates an existing key-value pair using a binary key (`&[u8]`).
    /// This operation will fail if the key does not already exist.
    /// Returns the old value upon success.
    pub async fn update_bytes(&self, key: &[u8], value: Vec<u8>) -> Result<Arc<Vec<u8>>> {
        if value.len() > self.config.max_value_size() {
            return Err(crate::RkvsError::Storage(format!(
                "Value size {} exceeds maximum allowed size {}",
                value.len(),
                self.config.max_value_size()
            )));
        }

        let (_idx, shard) = self.get_shard(key).await?;
        let mut data = self.timeout_write_lock(&shard, "update_bytes").await?;

        if !data.has_key(key) {
            return Err(crate::RkvsError::Storage(format!(
                "Key not found for update: '{}'",
                String::from_utf8_lossy(key)
            )));
        }

        if let Some(old_value) = data.set_value(key.to_vec(), value.clone()) {
            self.metadata.update_total_size(value.len() as isize - old_value.len() as isize);
            Ok(old_value)
        } else {
            Err(crate::RkvsError::Internal("Failed to update a key that should exist.".to_string()))
        }
    }

    /// Deletes a key-value pair using a string key.
    ///
    /// # Returns
    ///
    /// Returns `true` if the key existed and was deleted, otherwise `false`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use rkvs::{StorageManager, Result};
    /// # async fn run() -> Result<()> {
    /// # let storage = StorageManager::builder().build().await?;
    /// # storage.initialize(None).await?;
    /// # storage.create_namespace("my_app", None).await?;
    /// # let namespace = storage.namespace("my_app").await?;
    /// namespace.set("my_key", b"value".to_vec()).await?;
    ///
    /// assert_eq!(namespace.delete("my_key").await, true);
    /// assert_eq!(namespace.delete("my_key").await, false); // Already deleted
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&self, key: &str) -> bool {
        self.delete_bytes(key.as_bytes()).await
    }

    /// Deletes a key-value pair using a binary key (`&[u8]`).
    ///
    /// # Returns
    ///
    /// Returns `true` if the key existed and was deleted, otherwise `false`.
    pub async fn delete_bytes(&self, key: &[u8]) -> bool {
        self.consume_bytes(key).await.is_some()
    }

    /// Atomically gets and deletes a key-value pair using a string key.
    ///
    /// This is useful for queue-like patterns where you want to retrieve an item
    /// and ensure it's removed in a single, atomic operation.
    ///
    /// # Returns
    ///
    /// Returns `Some(value)` if the key existed, otherwise `None`.
    pub async fn consume(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.consume_bytes(key.as_bytes()).await
    }

    /// Atomically gets and deletes a key-value pair using a binary key (`&[u8]`).
    ///
    /// This is the most direct method for performing a consume operation.
    ///
    /// # Arguments
    ///
    /// * `key` - The binary key to consume.
    ///
    /// # Returns
    ///
    /// Returns `Some(value)` if the key existed, otherwise `None`.
    pub async fn consume_bytes(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        let (_idx, shard) = self.get_shard(key).await.ok()?;
        let mut data = match self.timeout_write_lock(&shard, "consume_bytes").await {
            Ok(guard) => guard,
            Err(_) => return None,
        };
        
        if let Some(value) = data.delete_value(key) {
            self.metadata.decrement_key_count();
            self.metadata.update_total_size(-(value.len() as isize));
            Some(value)
        } else {
            None
        }
    }

    /// Checks if a key exists using a string key.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use rkvs::{StorageManager, Result};
    /// # async fn run() -> Result<()> {
    /// # let storage = StorageManager::builder().build().await?;
    /// # storage.initialize(None).await?;
    /// # storage.create_namespace("my_app", None).await?;
    /// # let namespace = storage.namespace("my_app").await?;
    /// namespace.set("my_key", b"value".to_vec()).await?;
    ///
    /// assert_eq!(namespace.exists("my_key").await, true);
    /// assert_eq!(namespace.exists("non_existent_key").await, false);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn exists(&self, key: &str) -> bool {
        self.exists_bytes(key.as_bytes()).await
    }

    /// Checks if a key exists using a binary key (`&[u8]`).
    ///
    /// This is more efficient than `get` if you only need to check for presence.
    pub async fn exists_bytes(&self, key: &[u8]) -> bool {
        let (_idx, shard) = match self.get_shard(key).await {
            Ok(s) => s,
            Err(_) => return false,
        };
        let data = match self.timeout_read_lock(&shard, "exists_bytes").await {
            Ok(guard) => guard,
            Err(_) => return false,
        };
        data.has_key(key)
    }
}

// MARK: - Batch Operations
impl Namespace {
    /// Returns a list of all keys in the namespace.
    ///
    /// This operation iterates through all shards and collects all keys. It can be
    /// expensive for namespaces with a very large number of keys and should be used
    /// with caution in performance-sensitive code.
    ///
    /// Keys that are not valid UTF-8 will be converted to a `String` lossily.
    ///
    /// # Returns
    ///
    /// A `Vec<String>` containing all keys in the namespace.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use rkvs::{StorageManager, Result};
    /// # async fn run() -> Result<()> {
    /// # let storage = StorageManager::builder().build().await?;
    /// # storage.initialize(None).await?;
    /// # storage.create_namespace("my_app", None).await?;
    /// # let namespace = storage.namespace("my_app").await?;
    /// namespace.set("key1", b"value1".to_vec()).await?;
    /// namespace.set("key2", b"value2".to_vec()).await?;
    ///
    /// let mut keys = namespace.list_keys().await;
    /// keys.sort(); // Sort for predictable order in test
    ///
    /// assert_eq!(keys, vec!["key1".to_string(), "key2".to_string()]);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_keys(&self) -> Vec<String> {
        let shards_guard = self.timeout_read_lock(&self.shards, "list_keys").await.unwrap(); // Should not fail
        let mut all_keys = Vec::with_capacity(self.metadata.key_count());
        
        for shard in shards_guard.iter() {
            if let Ok(data) = self.timeout_read_lock(shard, "list_keys shard").await {
                for key in data.all_keys() {
                    all_keys.push(String::from_utf8_lossy(&key).to_string());
                }
            }
        }
        
        all_keys
    }

    /// Sets multiple key-value pairs in a single batch operation.
    ///
    /// # Arguments
    /// * `items` - A vector of `(key, value)` pairs to set.
    /// * `mode` - The `BatchMode` to use (`AllOrNothing` or `BestEffort`).
    ///
    /// # Returns
    /// A `BatchResult` containing a vector of `(key, Option<old_value>)` for each successful operation.
    pub async fn set_multiple(&self, items: Vec<(String, Vec<u8>)>, mode: BatchMode) -> BatchResult<Vec<(String, Option<Vec<u8>>)>> {
        let byte_items = items.into_iter().map(|(k, v)| (k.into_bytes(), v)).collect();
        let result = self.set_multiple_bytes(byte_items, mode).await;

        let data = result.data.map(|op_results| {
            op_results
                .into_iter()
                .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v))
                .collect()
        });

        BatchResult {
            data,
            total_processed: result.total_processed,
            duration: result.duration,
            errors: result.errors,
        }
    }

    /// Sets multiple key-value pairs using binary keys in a single batch operation.
    ///
    /// # Arguments
    /// * `items` - A vector of `(key, value)` pairs to set.
    /// * `mode` - The `BatchMode` to use (`AllOrNothing` or `BestEffort`).
    ///
    /// # Returns
    /// A `BatchResult` containing a vector of `(key, Option<old_value>)` for each successful operation.
    pub async fn set_multiple_bytes(&self, items: Vec<(Vec<u8>, Vec<u8>)>, mode: BatchMode) -> BatchResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_items = items.len();
        
        // Group items by shard
        let shards_guard = self.timeout_read_lock(&self.shards, "set_multiple_bytes").await.unwrap(); // Should not fail
        let shard_count = shards_guard.len();
        let mut items_by_shard: HashMap<usize, Vec<(usize, Vec<u8>, Vec<u8>)>> = HashMap::with_capacity(shard_count);

        let max_value_size = self.config.max_value_size();
        for (index, (key_bytes, value)) in items.into_iter().enumerate() {
            // Basic validation that doesn't require locks
            if value.len() > max_value_size {
                errors.push(BatchError {
                    key: String::from_utf8_lossy(&key_bytes).to_string(),
                    operation: "set".to_string(),
                    error_message: format!("Value size {} exceeds maximum {}", value.len(), max_value_size),
                    index,
                });
                continue;
            }
            let shard_idx = jump_consistent_hash(&key_bytes, shard_count);
            items_by_shard.entry(shard_idx).or_default().push((index, key_bytes, value));
        }

        let mut operation_results = Vec::with_capacity(total_items);

        match mode {
            BatchMode::AllOrNothing => {
                // If basic validation already failed, abort.
                if !errors.is_empty() {
                    return BatchResult { data: None, total_processed: total_items, duration: start.elapsed(), errors: Some(errors) };
                }

                let mut shard_indices: Vec<usize> = items_by_shard.keys().copied().collect();
                shard_indices.sort_unstable();

                let mut locked_shards = Vec::with_capacity(shard_indices.len());
                for shard_idx in &shard_indices {
                    match self.timeout_write_lock(&shards_guard[*shard_idx], &format!("set_multiple shard {}", shard_idx)).await {
                        Ok(guard) => locked_shards.push((*shard_idx, guard)),
                        Err(_) => return BatchResult {
                            data: None,
                            total_processed: total_items,
                            duration: start.elapsed(),
                            errors: Some(vec![BatchError { key: "batch".to_string(), operation: "set_multiple".to_string(),
                                error_message: format!("Failed to acquire lock on shard {} within timeout", shard_idx), index: 0 }]),
                        },
                    }
                }

                // With all shards locked, perform final validation and prepare changes.
                let mut total_size_increase: isize = 0;
                let mut new_keys_count: isize = 0;
                for (shard_idx, shard_guard) in &locked_shards {
                    if let Some(shard_items) = items_by_shard.get(shard_idx) {
                        for (_, key_bytes, value) in shard_items {
                            if let Some(existing) = shard_guard.get_value(key_bytes) {
                                total_size_increase += value.len() as isize - existing.len() as isize;
                            } else {
                                new_keys_count += 1;
                                total_size_increase += value.len() as isize;
                            }
                        }
                    }
                }

                // Validate max_keys limit with the accurate new_keys_count
                let max_keys = self.config.max_keys();
                if self.metadata.key_count() + new_keys_count as usize > max_keys {
                    return BatchResult {
                        data: None,
                        total_processed: total_items,
                        duration: start.elapsed(),
                        errors: Some(vec![BatchError {
                            key: "batch".to_string(),
                            operation: "set_multiple".to_string(),
                            error_message: format!("Key count limit {} would be exceeded", max_keys),
                            index: 0,
                        }]),
                    };
                }

                // Execute all writes
                for (shard_idx, mut shard_guard) in locked_shards {
                    if let Some(shard_items) = items_by_shard.get(&shard_idx) {
                        for (_, key, value) in shard_items {
                            let old_value = shard_guard.set_value(key.clone(), value.clone());
                            operation_results.push((key.clone(), old_value.map(|v| (*v).clone())));
                        }
                    }
                }

                self.metadata.update_key_count(new_keys_count);
                self.metadata.update_total_size(total_size_increase);
            }
            BatchMode::BestEffort => {
                let mut total_size_increase: isize = 0;
                let mut new_keys_count: isize = 0;

                for (shard_idx, shard_items) in items_by_shard {
                    let mut data = match self.timeout_write_lock(&shards_guard[shard_idx], &format!("set_multiple shard {}", shard_idx)).await {
                        Ok(guard) => guard,
                        Err(_) => continue, // Skip this shard on timeout in BestEffort mode
                    };

                    for (_, key, value) in shard_items {
                        let old_value = data.set_value(key.clone(), value.clone());
                        operation_results.push((key.clone(), old_value.as_ref().map(|v| (**v).clone())));
                        if let Some(old) = &old_value {
                            total_size_increase += value.len() as isize - old.len() as isize;
                        } else {
                            new_keys_count += 1;
                            total_size_increase += value.len() as isize;
                        }
                    }
                }
                self.metadata.update_key_count(new_keys_count);
                self.metadata.update_total_size(total_size_increase);
            }
        }

        BatchResult {
            data: Some(operation_results),
            total_processed: total_items,
            duration: start.elapsed(),
            errors: if errors.is_empty() { None } else { Some(errors) },
        }
    }

    /// Gets multiple key-value pairs in a single batch operation.
    /// 
    /// # Arguments
    /// 
    /// * `keys` - Vector of keys to retrieve
    /// * `mode` - Batch operation mode (AllOrNothing or BestEffort)
    /// 
    /// # Modes
    /// 
    /// - `BatchMode::AllOrNothing`: If any key is not found, returns None and errors for all missing keys
    /// - `BatchMode::BestEffort`: Returns found keys even if some are missing. Missing keys are reported in errors
    pub async fn get_multiple(
        &self,
        keys: Vec<String>,
        mode: BatchMode,
    ) -> BatchResult<Vec<(String, Vec<u8>)>> {
        let byte_keys = keys.into_iter().map(|k| k.into_bytes()).collect();
        let result = self.get_multiple_bytes(byte_keys, mode).await;

        let data = result.data.map(|items| {
            items
                .into_iter()
                .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v))
                .collect()
        });

        BatchResult {
            data,
            total_processed: result.total_processed,
            duration: result.duration,
            errors: result.errors,
        }
    }

    /// Gets multiple key-value pairs using binary keys in a single batch operation.
    ///
    /// # Arguments
    /// * `keys` - A vector of binary keys to retrieve.
    /// * `mode` - The `BatchMode` to use (`AllOrNothing` or `BestEffort`).
    ///
    pub async fn get_multiple_bytes(&self, keys: Vec<Vec<u8>>, mode: BatchMode) -> BatchResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_keys = keys.len();

        let shards_guard = self.timeout_read_lock(&self.shards, "get_multiple_bytes").await.unwrap(); // Should not fail
        let shard_count = shards_guard.len();
        
        // Group keys by shard
        let mut keys_by_shard: HashMap<usize, Vec<(usize, Vec<u8>)>> = HashMap::with_capacity(shard_count);
        for (index, key_bytes) in keys.into_iter().enumerate() {
            let shard_idx = jump_consistent_hash(&key_bytes, shard_count);
            keys_by_shard.entry(shard_idx).or_default().push((index, key_bytes));
        }

        let mut items = Vec::with_capacity(total_keys);

        match mode {
            BatchMode::AllOrNothing => {
                // Validate all keys exist before returning any data
                for (shard_idx, shard_keys) in &keys_by_shard {
                    let data = match self.timeout_read_lock(&shards_guard[*shard_idx], &format!("get_multiple shard {}", shard_idx)).await {
                        Ok(guard) => guard,
                        Err(e) => {
                            // On timeout, the entire AllOrNothing operation fails.
                            return BatchResult {
                                data: None,
                                total_processed: total_keys,
                                duration: start.elapsed(),
                                errors: Some(vec![BatchError { key: "batch".to_string(), operation: "get".to_string(), error_message: e.to_string(), index: 0 }]),
                            };
                        }
                    };
                    for (index, key_bytes) in shard_keys {
                        if !data.has_key(key_bytes) {
                            errors.push(BatchError {
                                key: String::from_utf8_lossy(key_bytes).to_string(),
                                operation: "get".to_string(),
                                error_message: "Key not found".to_string(),
                                index: *index,
                            });
                        }
                    }
                }

                if !errors.is_empty() {
                    return BatchResult { data: None, total_processed: total_keys, duration: start.elapsed(), errors: Some(errors) };
                }

                // If all keys exist, retrieve them
                for (shard_idx, shard_keys) in keys_by_shard {
                    let data = match self.timeout_read_lock(&shards_guard[shard_idx], &format!("get_multiple shard {}", shard_idx)).await {
                        Ok(guard) => guard,
                        Err(e) => {
                            // This should be unreachable if validation passed, but we handle it for safety.
                            return BatchResult {
                                data: None,
                                total_processed: total_keys,
                                duration: start.elapsed(),
                                errors: Some(vec![BatchError { key: "batch".to_string(), operation: "get".to_string(), error_message: e.to_string(), index: 0 }]),
                            };
                        }
                    };
                    for (_, key_bytes) in shard_keys {
                        if let Some(value) = data.get_value(&key_bytes) {
                            items.push((key_bytes, (*value).clone()));
                        }
                    }
                }
            }
            BatchMode::BestEffort => {
                for (shard_idx, shard_keys) in keys_by_shard {
                    let data = match self.timeout_read_lock(&shards_guard[shard_idx], &format!("get_multiple shard {}", shard_idx)).await {
                        Ok(guard) => guard,
                        Err(e) => {
                            // If the shard lock times out, report an error for all keys in that shard.
                            for (index, key_bytes) in shard_keys {
                                errors.push(BatchError {
                                    key: String::from_utf8_lossy(&key_bytes).to_string(),
                                    operation: "get".to_string(),
                                    error_message: e.to_string(),
                                    index,
                                });
                            }
                            continue;
                        }
                    };
                    for (index, key_bytes) in shard_keys {
                        if let Some(value) = data.get_value(&key_bytes) {
                            items.push((key_bytes, (*value).clone()));
                        } else {
                            errors.push(BatchError { key: String::from_utf8_lossy(&key_bytes).to_string(), operation: "get".to_string(), error_message: "Key not found".to_string(), index });
                        }
                    }
                }
            }
        }

        BatchResult {
            data: Some(items),
            total_processed: total_keys,
            duration: start.elapsed(),
            errors: if errors.is_empty() { None } else { Some(errors) },
        }
    }

    /// Deletes multiple keys in a single batch operation.
    /// 
    /// # Arguments
    /// 
    /// * `keys` - Vector of keys to delete
    /// * `mode` - Batch operation mode (AllOrNothing or BestEffort)
    /// 
    /// # Modes
    /// 
    /// - `BatchMode::AllOrNothing`: If any key is not found, no keys are deleted. All affected shards are locked for the entire operation.
    /// - `BatchMode::BestEffort`: Deletes found keys even if some are missing. Missing keys are reported in errors
    pub async fn delete_multiple(&self, keys: Vec<String>, mode: BatchMode) -> BatchResult<()> {
        let byte_keys = keys.into_iter().map(|k| k.into_bytes()).collect();
        self.delete_multiple_bytes(byte_keys, mode).await
    }

    /// Deletes multiple keys using binary keys in a single batch operation.
    ///
    /// # Arguments
    /// * `keys` - A vector of binary keys to delete.
    /// * `mode` - The `BatchMode` to use (`AllOrNothing` or `BestEffort`).
    ///
    pub async fn delete_multiple_bytes(&self, keys: Vec<Vec<u8>>, mode: BatchMode) -> BatchResult<()> {
        let result = self.consume_multiple_bytes(keys, mode).await;

        // We don't need the consumed data, just the status of the operation.
        BatchResult {
            data: result.data.map(|_| ()),
            total_processed: result.total_processed,
            duration: result.duration,
            errors: result.errors,
        }
    }

    /// Consumes multiple key-value pairs in a single batch operation.
    /// 
    /// # Arguments
    /// 
    /// * `keys` - Vector of keys to consume (get and delete atomically)
    /// * `mode` - Batch operation mode (AllOrNothing or BestEffort)
    /// 
    /// # Modes
    /// 
    /// - `BatchMode::AllOrNothing`: If any key is not found, no keys are consumed. All affected shards are locked for the entire operation.
    /// - `BatchMode::BestEffort`: Consumes found keys even if some are missing. Missing keys are reported in errors
    pub async fn consume_multiple(&self, keys: Vec<String>, mode: BatchMode) -> BatchResult<Vec<(String, Vec<u8>)>> {
        let start = Instant::now();
        let byte_keys: Vec<Vec<u8>> = keys.into_iter().map(|k| k.into_bytes()).collect();
        let result = self.consume_multiple_bytes(byte_keys, mode).await;

        let data = result.data.map(|items| {
            items
                .into_iter()
                .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v))
                .collect()
        });

        BatchResult {
            data,
            total_processed: result.total_processed,
            duration: start.elapsed(),
            errors: result.errors,
        }
    }

    /// Consumes multiple key-value pairs using binary keys in a single batch operation.
    ///
    /// # Arguments
    /// * `keys` - A vector of binary keys to consume.
    /// * `mode` - The `BatchMode` to use (`AllOrNothing` or `BestEffort`).
    ///
    pub async fn consume_multiple_bytes(&self, keys: Vec<Vec<u8>>, mode: BatchMode) -> BatchResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let start = Instant::now();
        let total_keys = keys.len();

        let shards_guard = self.timeout_read_lock(&self.shards, "consume_multiple_bytes").await.unwrap(); // Should not fail
        let shard_count = shards_guard.len();
        
        // Group keys by shard
        let mut keys_by_shard: HashMap<usize, Vec<(usize, Vec<u8>)>> = HashMap::with_capacity(shard_count);
        for (index, key) in keys.into_iter().enumerate() {
            let shard_idx = jump_consistent_hash(&key, shard_count);
            keys_by_shard.entry(shard_idx).or_default().push((index, key));
        }

        let mut errors = Vec::new();
        let mut items = Vec::with_capacity(total_keys);

        match mode {
            BatchMode::AllOrNothing => {
            let mut shard_indices: Vec<usize> = keys_by_shard.keys().copied().collect();
            shard_indices.sort_unstable();
            
            let mut locked_shards = Vec::with_capacity(shard_indices.len());
            for shard_idx in &shard_indices {
                match self.timeout_write_lock(&shards_guard[*shard_idx], &format!("consume_multiple shard {}", shard_idx)).await {
                    Ok(guard) => locked_shards.push((*shard_idx, guard)),
                    Err(_) => return BatchResult {
                        data: None,
                        total_processed: total_keys,
                        duration: start.elapsed(),
                        errors: Some(vec![BatchError { key: "batch".to_string(), operation: "consume".to_string(),
                            error_message: format!("Failed to acquire lock on shard {} within timeout", shard_idx), index: 0 }]),
                    },
                }
            }
            
            // Validate all keys exist
            for (shard_idx, shard_guard) in &locked_shards {
                if let Some(shard_keys) = keys_by_shard.get(shard_idx) {
                    for (index, key) in shard_keys {
                        if !shard_guard.has_key(key) {
                            return BatchResult {
                                data: None,
                                total_processed: total_keys,
                                duration: start.elapsed(),
                                errors: Some(vec!(BatchError {
                                    key: String::from_utf8_lossy(key).to_string(),
                                    operation: "delete".to_string(),
                                    error_message: "Key not found".to_string(),
                                    index: *index,
                                })),
                            };
                        }
                    }
                }
            }
            
            // All keys exist, proceed with consumption
            let mut consumed_keys_count: isize = 0;
            let mut total_size_decrease: isize = 0;
            
            for (shard_idx, shard_guard) in locked_shards.iter_mut() {
                if let Some(shard_keys) = keys_by_shard.get(shard_idx) {
                    for (_, key) in shard_keys {
                        if let Some(value) = shard_guard.delete_value(key) {
                            consumed_keys_count += 1;
                            total_size_decrease += value.len() as isize;
                            items.push((key.clone(), (*value).clone()));
                        }
                    }
                }
            }
            
            self.metadata.update_key_count(-consumed_keys_count);
            self.metadata.update_total_size(-total_size_decrease);
            
            }
            BatchMode::BestEffort => {
                let mut consumed_keys_count: isize = 0;
                let mut total_size_decrease: isize = 0;

                for (shard_idx, shard_keys) in keys_by_shard {
                    let mut data = match self.timeout_write_lock(&shards_guard[shard_idx], &format!("consume_multiple shard {}", shard_idx)).await {
                        Ok(guard) => guard,
                        Err(_) => continue, // Skip this shard on timeout
                    };
                    for (index, key) in shard_keys {
                        if let Some(value) = data.delete_value(&key) {
                            consumed_keys_count += 1;
                            total_size_decrease += value.len() as isize;
                            items.push((key, (*value).clone()));
                        } else {
                            errors.push(BatchError {
                                key: String::from_utf8_lossy(&key).to_string(),
                                operation: "consume".to_string(),
                                error_message: "Key not found".to_string(),
                                index,
                            });
                        }
                    }
                }
                self.metadata.update_key_count(-consumed_keys_count);
                self.metadata.update_total_size(-total_size_decrease);
            }
        }

        BatchResult {
            data: Some(items),
            total_processed: total_keys,
            duration: start.elapsed(),
            errors: if errors.is_empty() { None } else { Some(errors) },
        }
    }
}

// MARK: - Metadata & Inspection
impl Namespace {
    /// Gets a clone of the namespace's metadata.
    pub async fn get_metadata(&self) -> NamespaceMetadata {
        self.metadata.clone()
    }
}

// MARK: - Inspection
impl Namespace {
    /// Returns a vector containing the number of keys in each shard.
    /// The index of the vector corresponds to the shard index.
    pub async fn get_shard_key_counts(&self) -> Vec<usize> {
        let shards_guard = self.shards.read().await;
        let mut counts = Vec::with_capacity(shards_guard.len());

        for shard in shards_guard.iter() {
            let data = shard.read().await;
            counts.push(data.len());
        }
        counts
    }
}