//! Defines the custom error types and `Result` alias for the crate.
use thiserror::Error;

/// The main error type for the RKVS system.
#[derive(Error, Debug)]
pub enum RkvsError {
    /// Errors related to storage operations, file I/O, or configuration issues.
    #[error("Storage error: {0}")]
    Storage(String),

    /// A specific error indicating that a snapshot file was not found during loading.
    #[error("Snapshot file not found: {0}")]
    SnapshotNotFound(String),

    /// Errors that occur during data serialization or deserialization, typically with `bincode`.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Internal errors that indicate a logic bug or unexpected state.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// A specialized `Result` type for RKVS operations.
pub type Result<T> = std::result::Result<T, RkvsError>;

// From trait implementations to allow for easy conversion using `?`

impl From<std::io::Error> for RkvsError {
    fn from(err: std::io::Error) -> Self {
        RkvsError::Storage(err.to_string())
    }
}

impl From<bincode::Error> for RkvsError {
    fn from(err: bincode::Error) -> Self {
        RkvsError::Serialization(err.to_string())
    }
}