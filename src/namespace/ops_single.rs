//! Contains all single-key operations for the `Namespace`.
use super::{Namespace, Result};
use std::sync::Arc;

impl Namespace {
    /// Gets a value by its string key.
    pub async fn get(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.get_bytes(key.as_bytes()).await
    }

    /// Gets a value by its binary key (`&[u8]`).
    pub async fn get_bytes(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        let (_idx, shard) = self.get_shard(key).await.ok()?;
        let data = match self.timeout_read_lock(&shard, "get_bytes").await {
            Ok(guard) => guard,
            Err(_) => return None,
        };
        data.get_value(key)
    }

    /// Sets a key-value pair using a string key.
    pub async fn set(&self, key: &str, value: Vec<u8>) -> Result<Option<Arc<Vec<u8>>>> {
        self.set_bytes(key.as_bytes(), value).await
    }

    /// Sets a key-value pair using a binary key (`&[u8]`).
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
                    "Maximum number of keys ({}) reached for this namespace",
                    max_keys
                )));
            }
        }

        let old_value = data.set_value(key.to_vec(), value.clone());

        if let Some(old) = old_value {
            self.metadata
                .update_total_size((value.len() as isize) - (old.len() as isize));

            return Ok(Some(old));
        } else {
            self.metadata.increment_key_count();
            self.metadata.update_total_size(value.len() as isize);

            return Ok(None);
        }
    }

    /// Updates an existing key-value pair using a string key.
    pub async fn update(&self, key: &str, value: Vec<u8>) -> Result<Arc<Vec<u8>>> {
        self.update_bytes(key.as_bytes(), value).await
    }

    /// Updates an existing key-value pair using a binary key (`&[u8]`).
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
            self.metadata
                .update_total_size(value.len() as isize - old_value.len() as isize);
            Ok(old_value)
        } else {
            Err(crate::RkvsError::Internal(
                "Failed to update a key that should exist.".to_string(),
            ))
        }
    }

    /// Deletes a key-value pair using a string key.
    pub async fn delete(&self, key: &str) -> bool {
        self.delete_bytes(key.as_bytes()).await
    }

    /// Deletes a key-value pair using a binary key (`&[u8]`).
    pub async fn delete_bytes(&self, key: &[u8]) -> bool {
        self.consume_bytes(key).await.is_some()
    }

    /// Atomically gets and deletes a key-value pair using a string key.
    pub async fn consume(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.consume_bytes(key.as_bytes()).await
    }

    /// Atomically gets and deletes a key-value pair using a binary key (`&[u8]`).
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
    pub async fn exists(&self, key: &str) -> bool {
        self.exists_bytes(key.as_bytes()).await
    }

    /// Checks if a key exists using a binary key (`&[u8]`).
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
