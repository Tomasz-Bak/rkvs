//! Defines the primary public data structures and configuration types for the crate.
use crate::data_table::DataTable;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Statistics for a namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceStats {
    pub name: String,
    pub key_count: usize,
    pub total_size: usize,
}

/// Configuration for automatic snapshotting of the entire storage manager.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManagerAutosaveConfig {
    pub interval: Duration,
    pub filename: String,
}

/// Configuration for automatic snapshotting of a single namespace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NamespaceAutosaveConfig {
    pub namespace_name: String,
    pub interval: Duration,
    pub filename_pattern: String, // e.g., "backup-{ns}-{ts}.rkvs"
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageConfig {
    pub max_namespaces: Option<usize>,
    pub manager_autosave: Option<ManagerAutosaveConfig>,
    pub namespace_autosave: Vec<NamespaceAutosaveConfig>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_namespaces: None,
            manager_autosave: None,
            namespace_autosave: Vec::new(),
        }
    }
}

/// Namespace configuration
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NamespaceConfig {
    pub max_keys: AtomicUsize,
    pub max_value_size: AtomicUsize,
    pub shard_count: AtomicUsize,
    pub lock_timeout: AtomicUsize, // in milliseconds
}

impl NamespaceConfig {
    pub fn max_keys(&self) -> usize {
        self.max_keys.load(Ordering::Relaxed)
    }

    pub fn max_value_size(&self) -> usize {
        self.max_value_size.load(Ordering::Relaxed)
    }

    pub fn shard_count(&self) -> usize {
        self.shard_count.load(Ordering::Relaxed)
    }

    pub fn lock_timeout(&self) -> Duration {
        Duration::from_millis(self.lock_timeout.load(Ordering::Relaxed) as u64)
    }

    pub fn set_max_keys(&self, value: usize) {
        self.max_keys.store(value, Ordering::Relaxed)
    }

    pub fn set_max_value_size(&self, value: usize) {
        self.max_value_size.store(value, Ordering::Relaxed)
    }

    pub fn set_shard_count(&self, value: usize) {
        self.shard_count.store(value, Ordering::Relaxed)
    }

    pub fn set_lock_timeout(&self, value_ms: usize) {
        self.lock_timeout.store(value_ms, Ordering::Relaxed)
    }
}

impl Clone for NamespaceConfig {
    fn clone(&self) -> Self {
        Self {
            max_keys: AtomicUsize::new(self.max_keys()),
            max_value_size: AtomicUsize::new(self.max_value_size()),
            shard_count: AtomicUsize::new(self.shard_count()),
            lock_timeout: AtomicUsize::new(self.lock_timeout.load(Ordering::Relaxed)),
        }
    }
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        Self {
            max_keys: 0.into(),
            max_value_size: 0.into(),
            shard_count: 4.into(),    // Default to 4 shards
            lock_timeout: 500.into(), // Default lock timeout of 500ms
        }
    }
}

/// A complete, serializable snapshot of a namespace's state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceSnapshot {
    pub shards: Vec<DataTable>,
    pub metadata: NamespaceMetadata,
    pub config: NamespaceConfig,
}

/// Namespace metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct NamespaceMetadata {
    pub name: String,
    pub key_count: AtomicUsize,
    pub total_size: AtomicUsize,
}

impl NamespaceMetadata {
    pub fn new(name: String) -> Self {
        Self {
            name,
            key_count: AtomicUsize::new(0),
            total_size: AtomicUsize::new(0),
        }
    }

    pub fn increment_key_count(&self) {
        self.key_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_key_count(&self) {
        self.key_count.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn update_key_count(&self, diff: isize) {
        if diff > 0 {
            self.key_count.fetch_add(diff as usize, Ordering::Relaxed);
        } else if diff < 0 {
            self.key_count
                .fetch_sub(diff.abs() as usize, Ordering::Relaxed);
        }
    }

    pub fn set_key_count(&self, new_count: usize) {
        self.key_count.store(new_count, Ordering::Relaxed);
    }

    pub fn key_count(&self) -> usize {
        self.key_count.load(Ordering::Relaxed)
    }

    pub fn update_total_size(&self, diff: isize) {
        if diff > 0 {
            self.total_size.fetch_add(diff as usize, Ordering::Relaxed);
        } else if diff < 0 {
            self.total_size
                .fetch_sub(diff.abs() as usize, Ordering::Relaxed);
        }
    }

    pub fn set_total_size(&self, new_count: usize) {
        self.total_size.store(new_count, Ordering::Relaxed);
    }

    pub fn total_size(&self) -> usize {
        self.total_size.load(Ordering::Relaxed)
    }
}

impl Clone for NamespaceMetadata {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(), // clone the string
            key_count: AtomicUsize::new(self.key_count.load(Ordering::Relaxed)),
            total_size: AtomicUsize::new(self.total_size.load(Ordering::Relaxed)),
        }
    }
}

/// Result of a batch operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResult<T> {
    pub data: Option<T>,
    pub total_processed: usize,
    pub duration: Duration,
    pub errors: Option<Vec<BatchError>>,
}

/// Error information for a failed item in a batch operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchError {
    pub key: String,
    pub operation: String,
    pub error_message: String,
    pub index: usize,
}

impl<T> BatchResult<T> {
    pub fn is_success(&self) -> bool {
        self.errors.is_some()
    }
}
