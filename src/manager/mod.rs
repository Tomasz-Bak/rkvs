//! Contains the `StorageManager`, the main entry point for creating, managing,
//! and persisting namespaces.

mod autosave;
mod builder;
mod inspection;
mod management;
mod persistence;

use self::builder::StorageManagerBuilder;
use super::namespace::Namespace;
use super::persistence::FilePersistence;
use super::types::StorageConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use tokio::sync::RwLock;

/// Storage manager for key-value storage with namespaces
pub struct StorageManager {
    pub namespaces: Arc<RwLock<HashMap<String, Arc<Namespace>>>>,
    config: RwLock<StorageConfig>,
    persistence: Option<FilePersistence>,
    initialized: Arc<AtomicBool>,
    autosave_tasks: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
    manager_autosave_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl StorageManager {
    /// Creates a new builder for configuring a `StorageManager`.
    /// This is the main entry point for creating a new storage instance.
    pub fn builder() -> StorageManagerBuilder {
        StorageManagerBuilder::default()
    }

    fn new(config: StorageConfig, persistence: Option<FilePersistence>) -> Self {
        Self {
            namespaces: Arc::new(RwLock::new(HashMap::new())),
            config: RwLock::new(config),
            persistence,
            initialized: Arc::new(AtomicBool::new(false)),
            autosave_tasks: Arc::new(RwLock::new(HashMap::new())),
            manager_autosave_task: Arc::new(RwLock::new(None)),
        }
    }
}
