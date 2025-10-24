//! Contains all autosave task management methods for the `StorageManager`.
use super::{StorageManager, Weak};
use crate::{Result, RkvsError};
use std::sync::Arc;
use tokio::time;

impl StorageManager {
    /// Spawns background tasks for autosaving based on the initial configuration.
    pub(super) async fn spawn_autosave_tasks(self: &Arc<Self>) {
        if self.persistence.is_none() {
            return;
        }

        // Spawn task for full manager autosave
        let config_guard = self.config.read().await;
        if let Some(manager_config) = &config_guard.manager_autosave {
            let manager_task =
                Self::create_manager_autosave_task(Arc::downgrade(self), manager_config.clone());
            let handle = tokio::spawn(manager_task);
            let mut task_guard = self.manager_autosave_task.write().await;
            *task_guard = Some(handle);
        }

        for ns_config in &config_guard.namespace_autosave {
            let task =
                Self::create_namespace_autosave_task(Arc::downgrade(self), ns_config.clone());
            let handle = tokio::spawn(task);
            let mut tasks = self.autosave_tasks.write().await;
            tasks.insert(ns_config.namespace_name.clone(), handle);
        }
    }

    /// Creates the future for the manager autosave task.
    async fn create_manager_autosave_task(
        weak_self: Weak<Self>,
        config: crate::ManagerAutosaveConfig,
    ) {
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
    async fn create_namespace_autosave_task(
        weak_self: Weak<Self>,
        config: crate::NamespaceAutosaveConfig,
    ) {
        let mut interval = time::interval(config.interval);
        loop {
            interval.tick().await;
            if let Some(strong_self) = weak_self.upgrade() {
                let filename = config
                    .filename_pattern
                    .replace("{ns}", &config.namespace_name)
                    .replace("{ts}", &chrono::Utc::now().to_rfc3339());
                if let Err(e) = strong_self
                    .save_namespace(&config.namespace_name, &filename)
                    .await
                {
                    eprintln!(
                        "Failed to autosave namespace '{}': {}",
                        config.namespace_name, e
                    );
                }
            } else {
                break; // StorageManager was dropped, exit task
            }
        }
    }

    /// Dynamically adds and spawns a new autosave task for a single namespace.
    /// This requires the `StorageManager` to have persistence enabled.
    pub async fn add_namespace_autosave_task(
        self: Arc<Self>,
        config: crate::NamespaceAutosaveConfig,
    ) -> Result<()> {
        self.ensure_initialized().await?;

        if self.persistence.is_none() {
            return Err(RkvsError::PersistenceNotEnabled);
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

    /// Starts a previously configured autosave task for a specific namespace.
    ///
    /// This will only succeed if an autosave configuration for the namespace exists
    /// and the task is not already running.
    pub async fn start_namespace_autosave_task(
        self: Arc<Self>,
        namespace_name: &str,
    ) -> Result<()> {
        self.ensure_initialized().await?;

        if self.persistence.is_none() {
            return Err(RkvsError::PersistenceNotEnabled);
        }

        let mut tasks = self.autosave_tasks.write().await;
        if tasks.contains_key(namespace_name) {
            return Err(RkvsError::AutosaveTaskAlreadyExists(
                namespace_name.to_string(),
            ));
        }

        let config_guard = self.config.read().await;
        if let Some(config) = config_guard
            .namespace_autosave
            .iter()
            .find(|c| c.namespace_name == *namespace_name)
        {
            let task = Self::create_namespace_autosave_task(Arc::downgrade(&self), config.clone());
            let handle = tokio::spawn(task);
            tasks.insert(namespace_name.to_string(), handle);
            Ok(())
        } else {
            Err(RkvsError::AutosaveConfigNotFound(
                namespace_name.to_string(),
            ))
        }
    }

    /// Scans the configuration and starts any autosave tasks that are not currently running.
    ///
    /// This is useful for restarting tasks that were stopped or failed to start.
    pub async fn start_missing_autosave_tasks(self: Arc<Self>) -> Result<()> {
        self.ensure_initialized().await?;

        if self.persistence.is_none() {
            return Ok(()); // Nothing to do if persistence is off
        }

        let config_guard = self.config.read().await;

        // Check and start manager-level autosave if needed
        if let Some(manager_config) = &config_guard.manager_autosave {
            let mut task_guard = self.manager_autosave_task.write().await;
            if task_guard.is_none() {
                let manager_task = Self::create_manager_autosave_task(
                    Arc::downgrade(&self),
                    manager_config.clone(),
                );
                *task_guard = Some(tokio::spawn(manager_task));
            }
        }

        // Check and start namespace-level autosaves if needed
        let mut tasks = self.autosave_tasks.write().await;
        for ns_config in &config_guard.namespace_autosave {
            if !tasks.contains_key(&ns_config.namespace_name) {
                let task =
                    Self::create_namespace_autosave_task(Arc::downgrade(&self), ns_config.clone());
                let handle = tokio::spawn(task);
                tasks.insert(ns_config.namespace_name.clone(), handle);
            }
        }

        Ok(())
    }

    /// Lists the names of all currently running autosave tasks.
    ///
    /// Returns a tuple where the first element is a boolean indicating if the
    /// manager-level autosave task is active, and the second element is a vector
    /// of namespace names with active autosave tasks.
    pub async fn list_running_autosave_tasks(&self) -> Result<(bool, Vec<String>)> {
        self.ensure_initialized().await?;

        let manager_task_running = self.manager_autosave_task.read().await.is_some();

        let tasks = self.autosave_tasks.read().await;
        let namespace_tasks = tasks.keys().cloned().collect();

        Ok((manager_task_running, namespace_tasks))
    }

    /// Stops a running autosave task for a specific namespace.
    ///
    /// If a task is found for the given namespace name, it will be aborted and removed.
    pub async fn stop_namespace_autosave(&self, namespace_name: &str) -> Result<()> {
        self.ensure_initialized().await?;
        let mut tasks = self.autosave_tasks.write().await;

        if let Some(handle) = tasks.remove(namespace_name) {
            handle.abort();
            Ok(())
        } else {
            Err(RkvsError::AutosaveTaskNotFound(namespace_name.to_string()))
        }
    }

    /// Stops the running autosave task for the entire storage manager.
    ///
    /// If the manager-level autosave task is running, it will be aborted.
    pub async fn stop_manager_autosave(&self) -> Result<()> {
        self.ensure_initialized().await?;
        let mut task_guard = self.manager_autosave_task.write().await;

        if let Some(handle) = task_guard.take() {
            handle.abort();
        }

        Ok(())
    }
}
