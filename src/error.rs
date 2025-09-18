//! Unified error handling for RKVS
//! 
//! This module provides a single error type that covers all possible error conditions
//! in the RKVS system, with proper error chaining and context.

use thiserror::Error;

/// Unified error type for all RKVS operations
#[derive(Debug, Clone, Error)]
pub enum RkvsError {
    /// Storage/IO errors
    #[error("Storage error: {0}")]
    Storage(String),
    
    /// Serialization/deserialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    /// Internal system errors
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias for RKVS operations
pub type Result<T> = std::result::Result<T, RkvsError>;

// From trait implementations
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

impl From<String> for RkvsError {
    fn from(err: String) -> Self {
        RkvsError::Internal(err)
    }
}

impl From<&str> for RkvsError {
    fn from(err: &str) -> Self {
        RkvsError::Internal(err.to_string())
    }
}

