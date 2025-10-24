//! Contains all configuration-related methods for the `Namespace`.
use super::{DEFAULT_SHARD_CAPACITY, Namespace, Result, RkvsError};
use crate::data_table::DataTable;
use crate::types::NamespaceConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

impl Namespace {
    /// Resizes the namespace to use a new number of shards.
    /// Only supports increasing shard count
    pub async fn resize_shards(&self, new_shard_count: usize) -> Result<()> {
        let mut shards_guard = self
            .timeout_write_lock(&self.shards, "resize_shards")
            .await?;
        let current_shard_count = shards_guard.len();

        if new_shard_count <= current_shard_count {
            return Err(crate::RkvsError::Storage(
                "New shard count must be greater than current shard count".to_string(),
            ));
        }

        let num_new_shards = new_shard_count - current_shard_count;

        let new_shards: Vec<Arc<RwLock<DataTable>>> = (0..num_new_shards)
            .map(|_| {
                Arc::new(RwLock::new(DataTable::with_capacity(
                    DEFAULT_SHARD_CAPACITY,
                )))
            })
            .collect();

        // Append new shards to existing ones
        shards_guard.extend(new_shards);

        // Now redistribute keys that need to move to new shards
        // We only check the first current_shard_count shards (the old ones)
        for shard_idx in 0..current_shard_count {
            let shard = Arc::clone(&shards_guard[shard_idx]);
            let mut data = self
                .timeout_write_lock(
                    &shard,
                    &format!("data redistribution from shard {}", shard_idx),
                )
                .await?;
            let keys_to_check: Vec<_> = data.all_keys();

            for key in keys_to_check {
                let new_shard_idx = super::helpers::jump_consistent_hash(&key, new_shard_count);

                // If key should move to a different shard
                if new_shard_idx != shard_idx {
                    if let Some(value) = data.delete_value(&key) {
                        // Move to new shard
                        let target_shard = Arc::clone(&shards_guard[new_shard_idx]);
                        let mut target_data = self
                            .timeout_write_lock(
                                &target_shard,
                                &format!("data redistribution to shard {}", new_shard_idx),
                            )
                            .await?;
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
    pub async fn set_max_value_size(&self, value: usize) -> Result<()> {
        let new_max = if value == 0 { usize::MAX } else { value };
        self.config.set_max_value_size(new_max);
        Ok(())
    }

    /// Sets the lock acquisition timeout for this namespace.
    pub async fn set_lock_timeout(&self, value_ms: usize) -> Result<()> {
        if value_ms == 0 {
            return Err(RkvsError::Storage(
                "Lock timeout must be greater than 0.".to_string(),
            ));
        }
        self.config.set_lock_timeout(value_ms);
        Ok(())
    }

    /// Updates multiple config fields at once.
    pub async fn update_config(&self, new_config: NamespaceConfig) -> Result<()> {
        self.set_max_keys(new_config.max_keys()).await?;
        self.set_max_value_size(new_config.max_value_size()).await?;
        self.set_lock_timeout(
            new_config
                .lock_timeout
                .load(std::sync::atomic::Ordering::SeqCst),
        )
        .await?;
        Ok(())
    }
}
