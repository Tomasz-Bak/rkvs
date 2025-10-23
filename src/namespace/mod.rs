//! Contains the `Namespace` struct, the primary handle for all key-value operations,
//! sharding logic, and configuration management for a single, isolated data store.

mod config;
mod helpers;
mod inspection;
mod ops_batch;
mod ops_single;

use crate::data_table::DataTable;
use crate::types::{NamespaceConfig, NamespaceMetadata, NamespaceSnapshot};
use crate::{Result, RkvsError};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Batch operation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchMode {
    /// All operations must succeed or none are applied
    AllOrNothing,
    /// Continue processing on errors, apply successful operations
    BestEffort,
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
            .map(|_| {
                Arc::new(RwLock::new(DataTable::with_capacity(DEFAULT_SHARD_CAPACITY)))
            })
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
        let shards = snapshot
            .shards
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

        NamespaceSnapshot {
            shards: tables,
            metadata: self.metadata.clone(),
            config: self.config.clone(),
        }
    }
}