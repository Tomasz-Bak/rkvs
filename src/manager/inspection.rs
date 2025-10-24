//! Contains all inspection and statistics methods for the `StorageManager`.
use super::StorageManager;
use crate::Result;
use crate::types::{NamespaceStats, StorageConfig};
use std::sync::atomic::Ordering;

impl StorageManager {
    /// Gets statistics for a specific namespace.
    pub async fn get_namespace_stats(&self, name: &str) -> Result<Option<NamespaceStats>> {
        self.ensure_initialized().await?;

        let ns = self.namespace(name).await?;
        let metadata = ns.get_metadata().await;

        Ok(Some(NamespaceStats {
            name: metadata.name.clone(),
            key_count: metadata.key_count(),
            total_size: metadata.total_size(),
        }))
    }

    /// Gets all namespace statistics.
    pub async fn get_all_namespace_stats(&self) -> Result<Vec<NamespaceStats>> {
        self.ensure_initialized().await?;

        let namespaces = self.namespaces.read().await;
        let mut stats = Vec::new();

        for namespace in namespaces.values() {
            let metadata = namespace.get_metadata().await;
            stats.push(NamespaceStats {
                name: metadata.name.clone(),
                key_count: metadata.key_count(),
                total_size: metadata.total_size(),
            });
        }

        Ok(stats)
    }

    /// Gets the total number of namespaces.
    pub async fn get_namespace_count(&self) -> Result<usize> {
        self.ensure_initialized().await?;

        let namespaces = self.namespaces.read().await;
        Ok(namespaces.len())
    }

    /// Gets the total number of keys across all namespaces.
    pub async fn get_total_key_count(&self) -> Result<usize> {
        self.ensure_initialized().await?;

        let namespaces = self.namespaces.read().await;
        let mut total = 0;

        for namespace in namespaces.values() {
            let metadata = namespace.get_metadata().await;
            total += metadata.key_count();
        }

        Ok(total)
    }

    /// Gets the storage configuration.
    pub async fn get_config(&self) -> StorageConfig {
        self.config.read().await.clone()
    }

    /// Updates the storage configuration. Note: This does not dynamically change running tasks.
    pub async fn update_config(&self, config: StorageConfig) {
        *self.config.write().await = config;
    }

    /// Checks if persistence is enabled.
    pub fn has_persistence(&self) -> bool {
        self.persistence.is_some()
    }

    /// Checks if storage is initialized.
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Relaxed)
    }

    /// Ensures storage is initialized before operations.
    pub(super) async fn ensure_initialized(&self) -> Result<()> {
        if !self.is_initialized() {
            return Err(crate::RkvsError::NotInitialized);
        }
        Ok(())
    }
}
