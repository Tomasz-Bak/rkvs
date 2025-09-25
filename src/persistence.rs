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
    data: HashMap<String, Vec<u8>>,
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
    namespaces: HashMap<String, SerializableNamespace>,
}

/// File-based persistence provider
pub struct FilePersistence {
    path: PathBuf,
}

impl FilePersistence {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
    
    pub async fn load(&self) -> Result<HashMap<String, Arc<Namespace>>> {
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
    
    pub async fn save(&self, data: &HashMap<String, Arc<Namespace>>) -> Result<()> {
        let mut serializable_namespaces = HashMap::new();
        
        for (hash, namespace) in data.iter() {
            let serializable_ns = SerializableNamespace::from_namespace(namespace).await?;
            serializable_namespaces.insert(hash.clone(), serializable_ns);
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
        let (data, metadata) = namespace.get_all_data().await;
        let config = namespace.get_config().await;
        
        Ok(Self {
            data,
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
        
        namespace.restore_data(self.data, metadata).await;
        
        Ok(namespace)
    }
}
