//! Contains all persistence and lifecycle methods for the `StorageManager`.
use super::StorageManager;
use crate::namespace::Namespace;
use crate::Result;
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl StorageManager {
    /// Initializes storage (call this once at startup).
    ///
    /// This method must be called before performing any operations. If persistence is enabled
    /// and a `filename` is provided, it will attempt to load data from that file.
    /// If the file does not exist, the storage will start fresh without an error.
    /// An error will only be returned for other issues, such as a corrupt file.
    /// If `filename` is `None`, the storage will start empty.
    pub async fn initialize(&self, filename: Option<&str>) -> Result<()> {
        if self.initialized.load(Ordering::Relaxed) {
            return Ok(());
        }

        if let (Some(persistence), Some(fname)) = (&self.persistence, filename) {
            match persistence.load_all(fname).await {
                Ok((data, loaded_config)) => {
                    let mut config = self.config.write().await;
                    *config = loaded_config;

                    let mut namespaces = self.namespaces.write().await;
                    *namespaces = data;
                }
                Err(crate::RkvsError::SnapshotNotFound(_)) => {
                    // This is not a fatal error. It just means we're starting fresh.
                    // The namespaces map will remain empty, which is the correct state.
                }
                Err(e) => {
                    // Any other error (e.g., deserialization) is fatal.
                    return Err(e);
                }
            }
        }

        self.initialized.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Saves a snapshot of all namespaces to a file.
    ///
    /// This method only works if persistence was enabled when creating the `StorageManager`.
    pub async fn save_all(&self, filename: &str) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            persistence.save_all(self, filename).await?;
        }
        Ok(())
    }

    /// Saves a snapshot of a single namespace to a file.
    /// This is useful for creating backups of individual namespaces.
    pub async fn save_namespace(&self, namespace_name: &str, filename: &str) -> Result<()> {
        self.ensure_initialized().await?;

        if let Some(persistence) = &self.persistence {
            let namespace = self.namespace(namespace_name).await?;
            let snapshot = namespace.create_snapshot().await;
            persistence.save_snapshot(&snapshot, filename).await?;
            Ok(())
        } else {
            Err(crate::RkvsError::Storage(
                "Persistence is not enabled.".to_string(),
            ))
        }
    }

    /// Loads a single namespace from a snapshot file and adds it to the manager.
    ///
    /// This method will fail if a namespace with the same name already exists or if
    /// the `max_namespaces` limit has been reached. It requires persistence to be enabled.
    ///
    /// # Returns
    ///
    /// Returns `Ok(namespace_name)` on success, or an error if it fails.
    pub async fn load_namespace(&self, filename: &str) -> Result<String> {
        self.ensure_initialized().await?;

        let persistence = self.persistence.as_ref().ok_or_else(|| {
            crate::RkvsError::Storage("Persistence is not enabled.".to_string())
        })?;

        // Load the snapshot data from the file
        let snapshot = persistence.load_snapshot(filename).await?;
        let namespace_name = snapshot.metadata.name.clone();

        // Create the namespace object from the snapshot
        let namespace = Arc::new(Namespace::from_snapshot(snapshot));

        let mut namespaces = self.namespaces.write().await;
        let config_guard = self.config.read().await;

        // Check if the namespace already exists
        if namespaces.contains_key(&namespace_name) {
            return Err(crate::RkvsError::Storage(format!(
                "Namespace '{}' already exists",
                namespace_name
            )));
        }

        // Check if we've reached the max namespaces limit
        if let Some(max) = config_guard.max_namespaces {
            if namespaces.len() >= max {
                return Err(crate::RkvsError::Storage(format!(
                    "Maximum number of namespaces ({}) reached",
                    max
                )));
            }
        }

        namespaces.insert(namespace_name.clone(), namespace);
        Ok(namespace_name)
    }

    /// Loads a single namespace from a snapshot file and adds it to the manager with a new name.
    ///
    /// This allows creating multiple namespaces from the same snapshot file.
    /// This method will fail if a namespace with the `new_name` already exists or if
    /// the `max_namespaces` limit has been reached. It requires persistence to be enabled.
    ///
    /// # Returns
    ///
    /// Returns `Ok(new_name)` on success, or an error if it fails.
    pub async fn load_namespace_with_name(&self, filename: &str, new_name: &str) -> Result<String> {
        self.ensure_initialized().await?;

        let persistence = self.persistence.as_ref().ok_or_else(|| {
            crate::RkvsError::Storage("Persistence is not enabled.".to_string())
        })?;

        // Load the snapshot data from the file and update its name
        let mut snapshot = persistence.load_snapshot(filename).await?;
        snapshot.metadata.name = new_name.to_string();

        // Create the namespace object from the modified snapshot
        let namespace = Arc::new(Namespace::from_snapshot(snapshot));

        let mut namespaces = self.namespaces.write().await;
        let config_guard = self.config.read().await;

        if namespaces.contains_key(new_name) {
            return Err(crate::RkvsError::Storage(format!(
                "Namespace '{}' already exists",
                new_name
            )));
        }

        if let Some(max) = config_guard.max_namespaces {
            if namespaces.len() >= max {
                return Err(crate::RkvsError::Storage(format!(
                    "Maximum number of namespaces ({}) reached",
                    max
                )));
            }
        }

        namespaces.insert(new_name.to_string(), namespace);
        Ok(new_name.to_string())
    }
}