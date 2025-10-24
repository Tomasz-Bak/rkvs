//! Defines the custom error types and `Result` alias for the crate.
use thiserror::Error;

/// The primary error type for all fallible operations in the `rkvs` library.
#[derive(Error, Debug)]
pub enum RkvsError {
    /// A generic storage-related error for cases not covered by other variants.
    #[error("Storage error: {0}")]
    Storage(String),

    /// An error indicating that a snapshot file was not found during a load operation.
    #[error("Snapshot file not found: {0}")]
    SnapshotNotFound(String),

    /// An error that occurred during data serialization or deserialization.
    #[error("Serialization error")]
    Serialization(#[from] bincode::Error),

    /// An error related to I/O operations, such as reading or writing files.
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    /// An internal error that indicates a logic bug or unexpected state.
    #[error("Internal error: {0}")]
    Internal(String),

    /// An operation was attempted before the storage manager was initialized.
    #[error("Storage not initialized. Call initialize() first.")]
    NotInitialized,

    /// An operation required persistence, but it was not enabled on the `StorageManager`.
    #[error("Persistence is not enabled")]
    PersistenceNotEnabled,

    /// An attempt was made to create a namespace that already exists.
    #[error("Namespace '{0}' already exists")]
    NamespaceAlreadyExists(String),

    /// An operation was attempted on a namespace that does not exist.
    #[error("Namespace '{0}' not found")]
    NamespaceNotFound(String),

    /// The maximum number of namespaces has been reached.
    #[error("Maximum number of namespaces ({0}) reached")]
    MaxNamespacesReached(usize),

    /// An autosave task for the given namespace already exists and is running.
    #[error("Autosave task for namespace '{0}' is already running")]
    AutosaveTaskAlreadyExists(String),

    /// No autosave configuration was found for the specified namespace.
    #[error("No autosave configuration found for namespace '{0}'")]
    AutosaveConfigNotFound(String),

    /// No active autosave task was found for the specified namespace.
    #[error("No active autosave task found for namespace '{0}'")]
    AutosaveTaskNotFound(String),
}

/// A specialized `Result` type for RKVS operations.
pub type Result<T> = std::result::Result<T, RkvsError>;