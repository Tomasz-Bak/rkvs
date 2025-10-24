//! Contains the `StorageManagerBuilder` for fluently constructing a `StorageManager`.
use super::{FilePersistence, StorageConfig, StorageManager};
use crate::Result;
use std::path::PathBuf;
use std::sync::Arc;

/// Builder for creating StorageManager instances
///
/// Provides a fluent API for configuring and creating `StorageManager` instances.
///
/// # Example
///
/// ```rust
/// use rkvs::{StorageManager, StorageConfig, ManagerAutosaveConfig};
/// use std::time::Duration;
/// # async fn run() -> rkvs::Result<()> {
/// let storage_config = StorageConfig {
///     max_namespaces: Some(10),
///     manager_autosave: Some(ManagerAutosaveConfig {
///         interval: Duration::from_secs(300),
///         filename: "rkvs-snapshot.bin".to_string(),
///     }),
///     namespace_autosave: vec![],
/// };
/// let storage = StorageManager::builder()
///     .with_config(storage_config)
///     .with_persistence("/tmp/rkvs_data".into())
///     .build().await?;
/// # Ok(())
/// # }
/// ```
pub struct StorageManagerBuilder {
    config: Option<StorageConfig>,
    persistence_path: Option<PathBuf>,
}

impl StorageManagerBuilder {
    /// Create a new builder with default configuration
    pub fn new() -> Self {
        Self {
            config: None,
            persistence_path: None,
        }
    }

    /// Set the storage configuration
    ///
    /// # Arguments
    ///
    /// * `config` - The storage configuration to use
    pub fn with_config(mut self, config: StorageConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Enable file-based persistence
    ///
    /// # Arguments
    ///
    /// * `path` - The directory path where data will be stored
    pub fn with_persistence(mut self, path: PathBuf) -> Self {
        self.persistence_path = Some(path);
        self
    }

    /// Build the StorageManager with the configured options
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Arc<StorageManager>` instance, ready for use.
    ///
    /// # Example
    ///
    /// ```rust
    /// use rkvs::{StorageManager, Result};
    ///
    /// # async fn run() -> Result<()> {
    /// let storage = StorageManager::builder()
    ///     .with_persistence("/tmp/rkvs_data".into())
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn build(self) -> Result<Arc<StorageManager>> {
        let config = self.config.unwrap_or_default();
        let persistence = if let Some(path) = self.persistence_path {
            Some(FilePersistence::new(path).await?)
        } else {
            None
        };
        let manager = StorageManager::new(config, persistence);
        let arc_manager = Arc::new(manager);
        arc_manager.spawn_autosave_tasks().await;
        Ok(arc_manager)
    }
}

impl Default for StorageManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
