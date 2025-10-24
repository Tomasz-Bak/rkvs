//! Contains all namespace management methods for the `StorageManager`.
use super::StorageManager;
use crate::namespace::Namespace;
use crate::types::NamespaceConfig;
use crate::{Result, RkvsError};
use std::sync::Arc;

impl StorageManager {
    /// Create a new namespace with optional custom configuration
    ///
    /// Creates a new namespace with the given name and optional configuration.
    /// If no configuration is provided, `NamespaceConfig::default()` is used.
    pub async fn create_namespace(
        &self,
        name: &str,
        config: Option<NamespaceConfig>,
    ) -> Result<()> {
        self.ensure_initialized().await?;

        let mut namespaces = self.namespaces.write().await;

        if namespaces.contains_key(name) {
            return Err(RkvsError::NamespaceAlreadyExists(name.to_string()));
        }

        let config_guard = self.config.read().await;
        if let Some(max_namespaces) = config_guard.max_namespaces {
            if namespaces.len() >= max_namespaces {
                return Err(RkvsError::MaxNamespacesReached(max_namespaces));
            }
        }

        let namespace_config = config.unwrap_or_default();
        let namespace = Arc::new(Namespace::new(name.to_string(), namespace_config));
        namespaces.insert(name.to_string(), namespace);
        Ok(())
    }

    /// Creates a new namespace with a specific configuration.
    ///
    /// This is a convenience method that calls `create_namespace` with `Some(config)`.
    pub async fn create_namespace_with_config(
        &self,
        name: &str,
        config: NamespaceConfig,
    ) -> Result<()> {
        self.create_namespace(name, Some(config)).await
    }

    /// Updates the configuration of an existing namespace.
    pub async fn update_namespace_config(
        &self,
        name: &str,
        new_config: NamespaceConfig,
    ) -> Result<()> {
        self.ensure_initialized().await?;

        let namespaces = self.namespaces.read().await;

        match namespaces.get(name) {
            Some(namespace) => namespace.update_config(new_config).await,
            _ => Err(RkvsError::NamespaceNotFound(name.to_string())),
        }
    }

    /// Gets a handle to a specific namespace.
    /// Returns an error if the namespace doesn't exist.
    pub async fn namespace(&self, name: &str) -> Result<Arc<Namespace>> {
        self.ensure_initialized().await?;

        let namespaces = self.namespaces.read().await;

        if let Some(namespace) = namespaces.get(name) {
            Ok(namespace.clone())
        } else {
            Err(RkvsError::NamespaceNotFound(name.to_string()))
        }
    }

    /// Deletes an entire namespace and any associated autosave tasks.
    pub async fn delete_namespace(&self, name: &str) -> Result<()> {
        self.ensure_initialized().await?;

        // Acquire write locks on both maps to ensure atomicity.
        let mut namespaces = self.namespaces.write().await;
        let mut tasks = self.autosave_tasks.write().await;

        // Abort and remove the autosave task if it exists.
        if let Some(handle) = tasks.remove(name) {
            handle.abort();
        }

        if namespaces.remove(name).is_some() {
            Ok(())
        } else {
            Err(RkvsError::NamespaceNotFound(name.to_string()))
        }
    }

    /// Lists all namespaces.
    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        self.ensure_initialized().await?;

        let namespaces = self.namespaces.read().await;
        let mut names = Vec::new();

        for namespace in namespaces.values() {
            let metadata = namespace.get_metadata().await;
            names.push(metadata.name);
        }

        Ok(names)
    }
}
