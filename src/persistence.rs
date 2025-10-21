//! Handles the serialization and file I/O for saving and loading storage state.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::namespace::Namespace;
use crate::types::NamespaceSnapshot;
use crate::{Result, RkvsError};
use crate::manager::StorageManager;
use crate::types::StorageConfig;

/// Provides file-based persistence for the storage manager.
#[derive(Debug)]
pub struct FilePersistence {
    base_path: PathBuf,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct FullStorageSnapshot {
    namespaces: HashMap<String, NamespaceSnapshot>,
    config: StorageConfig,
}

impl FilePersistence {
    /// Creates a new `FilePersistence` instance.
    /// It ensures the base directory exists.
    pub async fn new(base_path: PathBuf) -> Result<Self> {
        if !base_path.exists() {
            fs::create_dir_all(&base_path).await?;
        }
        Ok(Self { base_path })
    }

    /// Saves a snapshot of all namespaces to a file.
    pub async fn save_all(&self, manager: &StorageManager, filename: &str) -> Result<()> {
        let namespaces_guard = manager.namespaces.read().await;
        let manager_config = manager.get_config().await;

        let mut snapshots = HashMap::new();
        for (name, ns) in namespaces_guard.iter() {
            snapshots.insert(name.clone(), ns.create_snapshot().await);
        }

        let full_snapshot = FullStorageSnapshot { namespaces: snapshots, config: manager_config};
        let bytes = bincode::serialize(&full_snapshot)?;
        let path = self.base_path.join(filename);
        let mut file = File::create(&path).await?;
        file.write_all(&bytes).await?;
        Ok(())
    }

    /// Loads a full storage snapshot from a file.
    pub async fn load_all(&self, filename: &str) -> Result<(HashMap<String, Arc<Namespace>>, StorageConfig)> {
        let path = self.base_path.join(filename);
        if !path.exists() {
            return Err(RkvsError::SnapshotNotFound(path.display().to_string()));
        }
    
        let mut file = File::open(&path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
    
        let full_snapshot: FullStorageSnapshot = bincode::deserialize(&buffer)?;
        let mut namespaces = HashMap::new();
        for (_, snapshot) in full_snapshot.namespaces {
            let ns = Namespace::from_snapshot(snapshot);
            namespaces.insert(ns.get_metadata().await.name, Arc::new(ns));
        }
    
        Ok((namespaces, full_snapshot.config)) // StorageConfig loading is handled by the manager
    }

    /// Saves a single namespace snapshot to a file.
    pub async fn save_snapshot(&self, snapshot: &NamespaceSnapshot, filename: &str) -> Result<()> {
        let bytes = bincode::serialize(snapshot)?;
        let path = self.base_path.join(filename);
        let mut file = File::create(&path).await?;
        file.write_all(&bytes).await?;
        Ok(())
    }

    /// Loads a single namespace snapshot from a file.
    pub async fn load_snapshot(&self, filename: &str) -> Result<NamespaceSnapshot> {
        let path = self.base_path.join(filename);
        let mut file = File::open(&path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;
        let snapshot: NamespaceSnapshot = bincode::deserialize(&buffer)?;
        Ok(snapshot)
    }
}