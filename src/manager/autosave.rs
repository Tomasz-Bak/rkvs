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
            return Err(RkvsError::Storage(
                "Persistence is not enabled.".to_string(),
            ));
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
}