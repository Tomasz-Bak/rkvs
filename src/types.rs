use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::time::Duration;
use sha2::{Sha256, Digest};

/// Statistics for a namespace
#[derive(Debug, Clone)]
pub struct NamespaceStats {
    pub name: String,
    pub key_count: usize,
    pub total_size: usize,
}

impl From<(&String, &HashMap<[u8; 32], Vec<u8>>)> for NamespaceStats {
    fn from((name, data): (&String, &HashMap<[u8; 32], Vec<u8>>)) -> Self {
        let key_count = data.len();
        let total_size = data.values().map(|value| value.len()).sum();

        Self {
            name: name.clone(),
            key_count,
            total_size,
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub max_namespaces: Option<usize>,
    pub default_max_keys_per_namespace: Option<usize>,
    pub default_max_value_size: Option<usize>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            max_namespaces: None,
            default_max_keys_per_namespace: None,
            default_max_value_size: None,
        }
    }
}

/// Namespace configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NamespaceConfig {
    pub max_keys: Option<usize>,
    pub max_value_size: Option<usize>,
}

/// Hash utilities for key and namespace hashing
pub struct HashUtils;

impl HashUtils {
    /// Hash a string to SHA256
    pub fn hash(input: &str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        hasher.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_stats_from_tuple() {
        let mut data = HashMap::new();
        data.insert([1u8; 32], b"value1".to_vec());
        data.insert([2u8; 32], b"value2".to_vec());
        data.insert([3u8; 32], b"value3".to_vec());
        
        let name = "test_namespace".to_string();
        let stats = NamespaceStats::from((&name, &data));
        
        assert_eq!(stats.name, "test_namespace");
        assert_eq!(stats.key_count, 3);
        assert_eq!(stats.total_size, 18);
    }

    #[test]
    fn test_storage_config_default() {
        let config = StorageConfig::default();
        
        assert_eq!(config.max_namespaces, None);
        assert_eq!(config.default_max_keys_per_namespace, None);
        assert_eq!(config.default_max_value_size, None);
    }

    #[test]
    fn test_storage_config_creation() {
        let config = StorageConfig {
            max_namespaces: Some(10),
            default_max_keys_per_namespace: Some(100),
            default_max_value_size: Some(1024),
        };
        
        assert_eq!(config.max_namespaces, Some(10));
        assert_eq!(config.default_max_keys_per_namespace, Some(100));
        assert_eq!(config.default_max_value_size, Some(1024));
    }

    #[test]
    fn test_namespace_config_creation() {
        let config = NamespaceConfig {
            max_keys: Some(50),
            max_value_size: Some(512),
        };
        
        assert_eq!(config.max_keys, Some(50));
        assert_eq!(config.max_value_size, Some(512));
    }

    #[test]
    fn test_hash_utils_hash() {
        let input1 = "test_string";
        let input2 = "test_string";
        let input3 = "different_string";
        
        let hash1 = HashUtils::hash(input1);
        let hash2 = HashUtils::hash(input2);
        let hash3 = HashUtils::hash(input3);
        
        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
        assert_eq!(hash1.len(), 32);
        assert_eq!(hash3.len(), 32);
        let hash1_again = HashUtils::hash(input1);
        assert_eq!(hash1, hash1_again);
    }

    #[test]
    fn test_hash_utils_empty_string() {
        let hash = HashUtils::hash("");
        assert_eq!(hash.len(), 32);
        
        let hash2 = HashUtils::hash("");
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_utils_unicode() {
        let unicode_input = "Hello 世界 🌍";
        let hash = HashUtils::hash(unicode_input);
        
        assert_eq!(hash.len(), 32);
        
        let hash2 = HashUtils::hash(unicode_input);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_hash_utils_long_string() {
        let long_input = "a".repeat(10000);
        let hash = HashUtils::hash(&long_input);
        
        assert_eq!(hash.len(), 32);
        
        let hash2 = HashUtils::hash(&long_input);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_namespace_stats_clone() {
        let stats = NamespaceStats {
            name: "test".to_string(),
            key_count: 5,
            total_size: 100,
        };
        
        let cloned = stats.clone();
        assert_eq!(cloned.name, "test");
        assert_eq!(cloned.key_count, 5);
        assert_eq!(cloned.total_size, 100);
    }

    #[test]
    fn test_storage_config_clone() {
        let config = StorageConfig {
            max_namespaces: Some(10),
            default_max_keys_per_namespace: Some(100),
            default_max_value_size: Some(1024),
        };
        
        let cloned = config.clone();
        assert_eq!(cloned.max_namespaces, Some(10));
        assert_eq!(cloned.default_max_keys_per_namespace, Some(100));
        assert_eq!(cloned.default_max_value_size, Some(1024));
    }

    #[test]
    fn test_namespace_config_clone() {
        let config = NamespaceConfig {
            max_keys: Some(50),
            max_value_size: Some(512),
        };
        
        let cloned = config.clone();
        assert_eq!(cloned.max_keys, Some(50));
        assert_eq!(cloned.max_value_size, Some(512));
    }
}

/// Result of a batch operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchResult<T> {
    pub data: Option<T>,
    pub total_processed: usize,
    pub duration: Duration,
    pub errors: Vec<BatchError>,
}

/// Error information for a failed item in a batch operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchError {
    pub key: String,
    pub operation: String,
    pub error_message: String,
    pub index: usize,
}

impl<T> BatchResult<T> {
    /// Check if the batch operation was successful
    pub fn is_success(&self) -> bool {
        self.errors.is_empty() && self.data.is_some()
    }
    
    /// Check if the batch operation had any errors
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
    
    /// Get the success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_processed == 0 {
            0.0
        } else {
            (self.total_processed - self.errors.len()) as f64 / self.total_processed as f64
        }
    }
}

#[cfg(test)]
mod batch_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_batch_result_success() {
        let result = BatchResult {
            data: Some(vec![1, 2, 3]),
            total_processed: 3,
            duration: Duration::from_millis(1),
            errors: Vec::new(),
        };
        
        assert!(result.is_success());
        assert!(!result.has_errors());
        assert_eq!(result.success_rate(), 1.0);
    }

    #[test]
    fn test_batch_result_with_errors() {
        let result: BatchResult<Vec<i32>> = BatchResult {
            data: None,
            total_processed: 5,
            duration: Duration::from_millis(2),
            errors: vec![
                BatchError {
                    key: "key1".to_string(),
                    operation: "set".to_string(),
                    error_message: "Value too large".to_string(),
                    index: 0,
                },
                BatchError {
                    key: "key3".to_string(),
                    operation: "set".to_string(),
                    error_message: "Key not found".to_string(),
                    index: 2,
                },
            ],
        };
        
        assert!(!result.is_success());
        assert!(result.has_errors());
        assert_eq!(result.success_rate(), 0.6); // 3/5 successful
    }
}
