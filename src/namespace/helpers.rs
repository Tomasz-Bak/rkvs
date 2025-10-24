//! Contains internal helper functions for the `Namespace` module.
use super::{Namespace, Result, RkvsError};
use crate::data_table::DataTable;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Jump consistent hash implementation
/// Returns a shard index in the range [0, num_shards)
pub(super) fn jump_consistent_hash(key: &[u8], num_shards: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(key, &mut hasher);
    let mut hash = std::hash::Hasher::finish(&hasher);

    let mut b: i64 = -1;
    let mut j: i64 = 0;

    while j < num_shards as i64 {
        b = j;
        hash = hash.wrapping_mul(2862933555777941757).wrapping_add(1);
        j = ((b.wrapping_add(1) as f64) * ((1i64 << 31) as f64)
            / (((hash >> 33).wrapping_add(1)) as f64)) as i64;
    }

    b as usize
}

impl Namespace {
    /// Helper to acquire a read lock with a configured timeout.
    pub(super) async fn timeout_read_lock<'a, T>(
        &self,
        lock: &'a RwLock<T>,
        context: &str,
    ) -> Result<tokio::sync::RwLockReadGuard<'a, T>> {
        tokio::time::timeout(self.config.lock_timeout(), lock.read())
            .await
            .map_err(|_| {
                RkvsError::Storage(format!(
                    "Timeout while waiting to acquire read lock for {}",
                    context
                ))
            })
    }

    /// Helper to acquire a write lock with a configured timeout.
    pub(super) async fn timeout_write_lock<'a, T>(
        &self,
        lock: &'a RwLock<T>,
        context: &str,
    ) -> Result<tokio::sync::RwLockWriteGuard<'a, T>> {
        tokio::time::timeout(self.config.lock_timeout(), lock.write())
            .await
            .map_err(|_| {
                RkvsError::Storage(format!(
                    "Timeout while waiting to acquire write lock for {}",
                    context
                ))
            })
    }

    /// Get the shard for a given key
    pub(super) async fn get_shard(&self, key: &[u8]) -> Result<(usize, Arc<RwLock<DataTable>>)> {
        let shards_guard = self.timeout_read_lock(&self.shards, "shard list").await?;
        let shard_count = shards_guard.len();
        let shard_idx = jump_consistent_hash(key, shard_count);
        let shard = Arc::clone(&shards_guard[shard_idx]);
        Ok((shard_idx, shard))
    }
}
