use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use serde::{Serialize, Deserialize};

use crate::Result;
use super::namespace::Namespace;
use super::types::NamespaceConfig;

/// Serializable representation of namespace data
#[derive(Serialize, Deserialize)]
struct SerializableNamespace {
    data: HashMap<[u8; 32], Vec<u8>>,
    key_mappings: HashMap<[u8; 32], String>,
    metadata: SerializableNamespaceMetadata,
    config: NamespaceConfig,
}

/// Serializable representation of namespace metadata
#[derive(Serialize, Deserialize)]
struct SerializableNamespaceMetadata {
    name: String,
    key_count: usize,
    total_size: usize,
}

/// Serializable storage data
#[derive(Serialize, Deserialize)]
struct StorageData {
    namespaces: HashMap<[u8; 32], SerializableNamespace>,
}

/// File-based persistence provider
pub struct FilePersistence {
    path: PathBuf,
}

impl FilePersistence {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
    
    pub async fn load(&self) -> Result<HashMap<[u8; 32], Arc<Namespace>>> {
        if !self.path.exists() {
            return Ok(HashMap::new());
        }
        
        let data = tokio::fs::read(&self.path).await?;
        let storage_data: StorageData = bincode::deserialize(&data)?;
        
        let mut namespaces = HashMap::new();
        for (hash, serializable_ns) in storage_data.namespaces {
            let namespace = Arc::new(SerializableNamespace::to_namespace(serializable_ns).await?);
            namespaces.insert(hash, namespace);
        }
        
        Ok(namespaces)
    }
    
    pub async fn save(&self, data: &HashMap<[u8; 32], Arc<Namespace>>) -> Result<()> {
        let mut serializable_namespaces = HashMap::new();
        
        for (hash, namespace) in data {
            let serializable_ns = SerializableNamespace::from_namespace(namespace).await?;
            serializable_namespaces.insert(*hash, serializable_ns);
        }
        
        let storage_data = StorageData {
            namespaces: serializable_namespaces,
        };
        
        let serialized = bincode::serialize(&storage_data)?;
        tokio::fs::write(&self.path, serialized).await?;
        Ok(())
    }
    
    pub async fn exists(&self) -> bool {
        self.path.exists()
    }
}

impl SerializableNamespace {
    /// Convert from Arc<Namespace> to SerializableNamespace by copying data
    async fn from_namespace(namespace: &Arc<Namespace>) -> Result<Self> {
        let (data, key_mappings, metadata) = namespace.get_all_data().await;
        let config = namespace.get_config().await;
        
        Ok(Self {
            data,
            key_mappings,
            metadata: SerializableNamespaceMetadata {
                name: metadata.name,
                key_count: metadata.key_count,
                total_size: metadata.total_size,
            },
            config,
        })
    }
    
    /// Convert from SerializableNamespace to Namespace
    async fn to_namespace(self) -> Result<Namespace> {
        let name = self.metadata.name.clone();
        let namespace = Namespace::new(name.clone(), self.config);
        
        let metadata = super::namespace::NamespaceMetadata {
            name,
            key_count: self.metadata.key_count,
            total_size: self.metadata.total_size,
        };
        
        namespace.restore_data(self.data, self.key_mappings, metadata).await;
        
        Ok(namespace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::types::HashUtils;

    fn create_test_persistence() -> (FilePersistence, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().join("test_storage.bin");
        let persistence = FilePersistence::new(storage_path);
        (persistence, temp_dir)
    }

    async fn create_test_namespace() -> Arc<Namespace> {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let namespace = Arc::new(Namespace::new("test".to_string(), config));
        
        // Add some test data
        namespace.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespace.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        
        namespace
    }

    #[tokio::test]
    async fn test_file_persistence_creation() {
        let (persistence, _temp_dir) = create_test_persistence();
        assert!(!persistence.exists().await);
    }

    #[tokio::test]
    async fn test_file_persistence_save_and_load() {
        let (persistence, _temp_dir) = create_test_persistence();
        
        // Create test data
        let mut namespaces = HashMap::new();
        let namespace = create_test_namespace().await;
        let namespace_hash = HashUtils::hash("test");
        namespaces.insert(namespace_hash, namespace);
        
        // Save to disk
        persistence.save(&namespaces).await.unwrap();
        assert!(persistence.exists().await);
        
        // Load from disk
        let loaded_namespaces = persistence.load().await.unwrap();
        assert_eq!(loaded_namespaces.len(), 1);
        
        // Verify the data
        let loaded_namespace = loaded_namespaces.get(&namespace_hash).unwrap();
        assert_eq!(loaded_namespace.get("key1").await, Some(b"value1".to_vec()));
        assert_eq!(loaded_namespace.get("key2").await, Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_file_persistence_load_empty_file() {
        let (persistence, _temp_dir) = create_test_persistence();
        
        // Load from non-existent file should return empty HashMap
        let loaded_namespaces = persistence.load().await.unwrap();
        assert!(loaded_namespaces.is_empty());
    }

    #[tokio::test]
    async fn test_file_persistence_multiple_namespaces() {
        let (persistence, _temp_dir) = create_test_persistence();
        
        // Create multiple namespaces
        let mut namespaces = HashMap::new();
        
        let config1 = NamespaceConfig {
            max_keys: Some(5),
            max_value_size: Some(50),
        };
        let ns1 = Arc::new(Namespace::new("namespace1".to_string(), config1));
        ns1.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        namespaces.insert(HashUtils::hash("namespace1"), ns1);
        
        let config2 = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let ns2 = Arc::new(Namespace::new("namespace2".to_string(), config2));
        ns2.set("key1".to_string(), b"value1".to_vec()).await.unwrap();
        ns2.set("key2".to_string(), b"value2".to_vec()).await.unwrap();
        namespaces.insert(HashUtils::hash("namespace2"), ns2);
        
        // Save to disk
        persistence.save(&namespaces).await.unwrap();
        
        // Load from disk
        let loaded_namespaces = persistence.load().await.unwrap();
        assert_eq!(loaded_namespaces.len(), 2);
        
        // Verify both namespaces
        let ns1_hash = HashUtils::hash("namespace1");
        let ns2_hash = HashUtils::hash("namespace2");
        
        let loaded_ns1 = loaded_namespaces.get(&ns1_hash).unwrap();
        assert_eq!(loaded_ns1.get("key1").await, Some(b"value1".to_vec()));
        
        let loaded_ns2 = loaded_namespaces.get(&ns2_hash).unwrap();
        assert_eq!(loaded_ns2.get("key1").await, Some(b"value1".to_vec()));
        assert_eq!(loaded_ns2.get("key2").await, Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_serializable_namespace_from_namespace() {
        let namespace = create_test_namespace().await;
        let serializable = SerializableNamespace::from_namespace(&namespace).await.unwrap();
        
        assert_eq!(serializable.metadata.name, "test");
        assert_eq!(serializable.metadata.key_count, 2);
        assert_eq!(serializable.metadata.total_size, 12);
        assert_eq!(serializable.data.len(), 2);
        assert_eq!(serializable.key_mappings.len(), 2);
    }

    #[tokio::test]
    async fn test_serializable_namespace_to_namespace() {
        let original_namespace = create_test_namespace().await;
        let serializable = SerializableNamespace::from_namespace(&original_namespace).await.unwrap();
        let restored_namespace = SerializableNamespace::to_namespace(serializable).await.unwrap();
        
        // Verify the restored namespace has the same data
        assert_eq!(restored_namespace.get("key1").await, Some(b"value1".to_vec()));
        assert_eq!(restored_namespace.get("key2").await, Some(b"value2".to_vec()));
        
        let metadata = restored_namespace.get_metadata().await;
        assert_eq!(metadata.name, "test");
        assert_eq!(metadata.key_count, 2);
        assert_eq!(metadata.total_size, 12);
    }

    #[tokio::test]
    async fn test_serializable_namespace_round_trip() {
        let original_namespace = create_test_namespace().await;
        
        // Convert to serializable and back
        let serializable = SerializableNamespace::from_namespace(&original_namespace).await.unwrap();
        let restored_namespace = SerializableNamespace::to_namespace(serializable).await.unwrap();
        
        // Verify all data is preserved
        assert_eq!(restored_namespace.get("key1").await, original_namespace.get("key1").await);
        assert_eq!(restored_namespace.get("key2").await, original_namespace.get("key2").await);
        
        let original_metadata = original_namespace.get_metadata().await;
        let restored_metadata = restored_namespace.get_metadata().await;
        assert_eq!(restored_metadata.name, original_metadata.name);
        assert_eq!(restored_metadata.key_count, original_metadata.key_count);
        assert_eq!(restored_metadata.total_size, original_metadata.total_size);
        
        let original_config = original_namespace.get_config().await;
        let restored_config = restored_namespace.get_config().await;
        assert_eq!(restored_config.max_keys, original_config.max_keys);
        assert_eq!(restored_config.max_value_size, original_config.max_value_size);
    }

    #[tokio::test]
    async fn test_serializable_namespace_empty() {
        let config = NamespaceConfig {
            max_keys: Some(10),
            max_value_size: Some(100),
        };
        let empty_namespace = Arc::new(Namespace::new("empty".to_string(), config));
        
        let serializable = SerializableNamespace::from_namespace(&empty_namespace).await.unwrap();
        assert_eq!(serializable.metadata.name, "empty");
        assert_eq!(serializable.metadata.key_count, 0);
        assert_eq!(serializable.metadata.total_size, 0);
        assert!(serializable.data.is_empty());
        assert!(serializable.key_mappings.is_empty());
        
        let restored = SerializableNamespace::to_namespace(serializable).await.unwrap();
        let metadata = restored.get_metadata().await;
        assert_eq!(metadata.name, "empty");
        assert_eq!(metadata.key_count, 0);
        assert_eq!(metadata.total_size, 0);
    }
}
