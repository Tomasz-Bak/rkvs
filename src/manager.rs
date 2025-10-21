//! Contains the `StorageManager`, the main entry point for creating, managing,
//! and persisting namespaces.
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::{sync::RwLock, time};

use crate::Result;
use super::types::{NamespaceStats, StorageConfig, NamespaceConfig};
use super::namespace::Namespace;
use super::persistence::FilePersistence;

/// Storage manager for key-value storage with namespaces
/// 
/// The `StorageManager` is the main entry point for working with RKVS. It manages
/// multiple namespaces and provides persistence capabilities.
/// 
/// # Example
/// 
/// ```rust
/// # use rkvs::{StorageManager, NamespaceConfig, Result};
/// # use std::env::temp_dir;
/// # async fn run() -> Result<()> {
///     // Use a temporary directory. The persistence layer will create it if it doesn't exist.
///     let temp_path = temp_dir().join("rkvs_docs_main");
/// 
///     let storage = StorageManager::builder()
///         .with_persistence(temp_path)
///         .build().await?;
///     
///     // Initialize from a snapshot. If the file doesn't exist, it will start fresh.
///     // Passing `None` would skip loading and always start fresh.
///     storage.initialize(Some("my-snapshot.bin")).await?;
///     
///     let mut config = NamespaceConfig::default();
///     config.set_max_keys(1000);
///     config.set_max_value_size(1024 * 1024); // This is an atomic operation
/// 
///     storage.create_namespace("my_app", Some(config)).await?;
///     
///     let namespace = storage.namespace("my_app").await?;
///     namespace.set("key", b"value".to_vec()).await?;
///     
///     Ok(())
/// # }
/// ```
pub struct StorageManager {
    namespaces: Arc<RwLock<HashMap<String, Arc<Namespace>>>>,
    config: StorageConfig,
    persistence: Option<FilePersistence>,
    initialized: Arc<AtomicBool>,
    autosave_tasks: Arc<RwLock<HashMap<String, tokio::task::JoinHandle<()>>>>,
    manager_autosave_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

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
    /// 
    /// # Example
    /// 
    /// ```rust
    /// use rkvs::{StorageManager, StorageConfig};
    /// 
    /// # async fn run() -> rkvs::Result<()> {
    /// let storage = StorageManager::builder()
    ///     .with_config(StorageConfig::default())
    ///     .build().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_config(mut self, config: StorageConfig) -> Self {
        self.config = Some(config);
        self
    }
    
    /// Enable file-based persistence
    /// 
    /// # Arguments
    /// 
    /// * `path` - The directory path where data will be stored
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
        arc_manager.spawn_autosave_tasks();
        Ok(arc_manager)
    }
}

// MARK: - Core & Construction
impl StorageManager {
    /// Creates a new builder for configuring a `StorageManager`.
    /// This is the main entry point for creating a new storage instance.
    pub fn builder() -> StorageManagerBuilder {
        StorageManagerBuilder::new()
    }
    
    fn new(config: StorageConfig, persistence: Option<FilePersistence>) -> Self {
        Self {
            namespaces: Arc::new(RwLock::new(HashMap::new())),
            config,
            persistence,
            initialized: Arc::new(AtomicBool::new(false)),
            autosave_tasks: Arc::new(RwLock::new(HashMap::new())),
            manager_autosave_task: Arc::new(RwLock::new(None)),
        }
    }
}

// MARK: - Autosave Task Management
impl StorageManager {
    /// Spawns background tasks for autosaving based on the initial configuration.
    fn spawn_autosave_tasks(self: &Arc<Self>) {
        if self.persistence.is_none() {
            return;
        }

        // Spawn task for full manager autosave
        if let Some(manager_config) = &self.config.manager_autosave {
            let manager_task = Self::create_manager_autosave_task(Arc::downgrade(self), manager_config.clone());
            let handle = tokio::spawn(manager_task);
            let mut task_guard = self.manager_autosave_task.blocking_write();
            *task_guard = Some(handle);
        }

        // Spawn tasks for individual namespace autosaves
        for ns_config in &self.config.namespace_autosave {
            let task = Self::create_namespace_autosave_task(Arc::downgrade(self), ns_config.clone());
            let handle = tokio::spawn(task);
            let mut tasks = self.autosave_tasks.blocking_write();
            tasks.insert(ns_config.namespace_name.clone(), handle);
        }
    }

    /// Creates the future for the manager autosave task.
    async fn create_manager_autosave_task(weak_self: Weak<Self>, config: crate::ManagerAutosaveConfig) {
        let mut interval = time::interval(config.interval);
        loop {
            interval.tick().await;
            if let Some(strong_self) = weak_self.upgrade() {
                if let Err(e) = strong_self.save_all(&config.filename).await {
                    eprintln!("Failed to autosave full snapshot: {}", e);
                }
            } else {
                break; // StorageManager was dropped, exit task
            }
        }
    }

    /// Creates the future for a single namespace autosave task.
    async fn create_namespace_autosave_task(weak_self: Weak<Self>, config: crate::NamespaceAutosaveConfig) {
        let mut interval = time::interval(config.interval);
        loop {
            interval.tick().await;
            if let Some(strong_self) = weak_self.upgrade() {
                let filename = config.filename_pattern
                    .replace("{ns}", &config.namespace_name)
                    .replace("{ts}", &chrono::Utc::now().to_rfc3339());
                if let Err(e) = strong_self.save_namespace(&config.namespace_name, &filename).await {
                    eprintln!("Failed to autosave namespace '{}': {}", config.namespace_name, e);
                }
            } else {
                break; // StorageManager was dropped, exit task
            }
        }
    }

    /// Dynamically adds and spawns a new autosave task for a single namespace.
    /// This requires the `StorageManager` to have persistence enabled.
    pub async fn add_namespace_autosave_task(self: Arc<Self>, config: crate::NamespaceAutosaveConfig) -> Result<()> {
        self.ensure_initialized().await?;

        if self.persistence.is_none() {
            return Err(crate::RkvsError::Storage("Persistence is not enabled.".to_string()));
        }

        // Ensure the namespace exists before setting up a task for it.
        self.namespace(&config.namespace_name).await?;

        let namespace_name = config.namespace_name.clone();
        let mut tasks = self.autosave_tasks.write().await;
        let task = Self::create_namespace_autosave_task(Arc::downgrade(&self), config);
        let handle = tokio::spawn(task);
        tasks.insert(namespace_name, handle);

        Ok(())
    }

    /// Stops a running autosave task for a specific namespace.
    pub async fn stop_namespace_autosave(&self, namespace_name: &str) -> Result<()> {
        self.ensure_initialized().await?;
        let mut tasks = self.autosave_tasks.write().await;

        if let Some(handle) = tasks.remove(namespace_name) {
            handle.abort();
            Ok(())
        } else {
            Err(crate::RkvsError::Storage(format!(
                "No active autosave task found for namespace '{}'",
                namespace_name
            )))
        }
    }

    /// Stops the running autosave task for the entire storage manager.
    pub async fn stop_manager_autosave(&self) -> Result<()> {
        self.ensure_initialized().await?;
        let mut task_guard = self.manager_autosave_task.write().await;

        if let Some(handle) = task_guard.take() {
            handle.abort();
        }

        Ok(())
    }
}

// MARK: - Lifecycle & Persistence
impl StorageManager {
    /// Initializes storage (call this once at startup).
    /// 
    /// This method must be called before performing any operations. If persistence is enabled
    /// and a `filename` is provided, it will attempt to load data from that file.
    /// If the file does not exist, the storage will start fresh without an error.
    /// An error will only be returned for other issues, such as a corrupt file.
    /// If `filename` is `None`, the storage will start empty.
    /// 
    /// # Returns
    /// 
    /// Returns `Ok(())` if initialization succeeds, or an error if it fails (e.g., due to a corrupt file).
    /// 
    /// # Example
    /// 
    /// ```rust
    /// # use rkvs::{StorageManager, Result};
    /// # use std::env::temp_dir;
    /// # async fn run() -> Result<()> {
    ///     // Use a temporary directory. The persistence layer will create it if it doesn't exist.
    ///     let temp_path = temp_dir().join("rkvs_docs_init");
    /// 
    ///     let storage = StorageManager::builder()
    ///         .with_persistence(temp_path)
    ///         .build().await?;
    ///     
    ///     // Initialize from a snapshot. If "my-snapshot.bin" doesn't exist,
    ///     // the storage will simply start fresh.
    ///     storage.initialize(Some("my-snapshot.bin")).await?;
    ///     
    ///     // Now you can use the storage
    ///     Ok(())
    /// }
    /// ```
    pub async fn initialize(&self, filename: Option<&str>) -> Result<()> {
        if self.initialized.load(Ordering::Relaxed) {
            return Ok(());
        }
        
        if let (Some(persistence), Some(fname)) = (&self.persistence, filename) {
            match persistence.load_all(fname).await {
                Ok(data) => {
                    // Successfully loaded data from the snapshot
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
    /// 
    /// # Arguments
    /// 
    /// * `filename` - The name of the file to save the snapshot to.
    /// 
    /// # Returns
    /// 
    /// Returns `Ok(())` if save succeeds, or an error if it fails.
    /// 
    /// # Example
    /// ```rust
    /// # use rkvs::{StorageManager, Result};
    /// # async fn run() -> Result<()> {
    /// # let storage = StorageManager::builder()
    /// #    .with_persistence("/tmp/rkvs_data".into())
    /// #    .build().await?;
    /// # storage.initialize(None).await?;
    ///     
    ///     // ... perform operations ...
    ///
    ///     // Save to disk
    ///     storage.save_all("my-snapshot.bin").await?;
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub async fn save_all(&self, filename: &str) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            let namespaces = self.namespaces.read().await;
            persistence.save_all(&namespaces, filename).await?;
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
            Err(crate::RkvsError::Storage("Persistence is not enabled.".to_string()))
        }
    }
}

// MARK: - Namespace Management
impl StorageManager {
    /// Create a new namespace with optional custom configuration
    /// 
    /// Creates a new namespace with the given name and optional configuration.
    /// If no configuration is provided, `NamespaceConfig::default()` is used.
    /// 
    /// # Arguments
    /// 
    /// * `name` - The name of the namespace to create
    /// * `config` - Optional namespace-specific configuration
    /// 
    /// # Returns
    /// 
    /// Returns `Ok(())` if creation succeeds, or an error if it fails.
    /// 
    /// # Example
    /// 
    /// ```rust
    /// 
    /// # use rkvs::{StorageManager, NamespaceConfig, Result};
    /// # async fn run() -> Result<()> {
    /// # let storage = StorageManager::builder().build().await?;
    /// # storage.initialize(None).await?;
    ///     
    ///     // Create namespace with default config
    ///     storage.create_namespace("my_app", None).await?;
    ///     
    ///     // Create namespace with custom config
    ///     let mut config = NamespaceConfig::default();
    ///     config.set_max_keys(1000);
    ///     config.set_max_value_size(1024 * 1024); // This is an atomic operation
    ///     storage.create_namespace("my_app2", Some(config)).await?;
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub async fn create_namespace(&self, name: &str, config: Option<NamespaceConfig>) -> Result<()> {
        self.ensure_initialized().await?;
        
        let mut namespaces = self.namespaces.write().await;
        
        if namespaces.contains_key(name) {
            return Err(crate::RkvsError::Storage(format!("Namespace '{}' already exists", name)));
        }
        
        if let Some(max_namespaces) = self.config.max_namespaces {
            if namespaces.len() >= max_namespaces {
                return Err(crate::RkvsError::Storage(format!(
                    "Maximum number of namespaces ({}) reached", max_namespaces
                )));
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
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the namespace to create.
    /// * `config` - The namespace-specific configuration to apply.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if creation succeeds, or an error if it fails.
    ///
    /// # Example
    ///
    /// ```rust
    /// use rkvs::{StorageManager, NamespaceConfig, Result};
    ///
    /// # async fn run() -> Result<()> {
    /// # let storage = StorageManager::builder().build().await?;
    /// # storage.initialize(None).await?;
    /// let mut config = NamespaceConfig::default();
    /// config.set_max_keys(5000);
    ///
    /// storage.create_namespace_with_config("my_configured_app", config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_namespace_with_config(&self, name: &str, config: NamespaceConfig) -> Result<()> {
        self.create_namespace(name, Some(config)).await
    }

    /// Updates the configuration of an existing namespace.
    pub async fn update_namespace_config(&self, name: &str, new_config: NamespaceConfig) -> Result<()> {
        self.ensure_initialized().await?;
        
        let namespaces = self.namespaces.read().await;
        
        match namespaces.get(name) {
            Some(namespace) => namespace.update_config(new_config).await,
            None => Err(crate::RkvsError::Storage(format!("Namespace with name '{}' does not exist", name))),
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
            Err(crate::RkvsError::Storage(format!("Namespace with name {} does not exist", name)))
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
            Err(crate::RkvsError::Storage(format!("Namespace with name {} does not exist", name)))
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

// MARK: - Statistics & Status
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
    pub fn get_config(&self) -> &StorageConfig {
        &self.config
    }

    /// Updates the storage configuration. Note: This does not dynamically change running tasks.
    pub fn update_config(&mut self, config: StorageConfig) {
        self.config = config;
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
    async fn ensure_initialized(&self) -> Result<()> {
        if !self.is_initialized() {
            return Err(crate::RkvsError::Storage("Storage not initialized. Call initialize() first.".to_string()));
        }
        Ok(())
    }
}
