//! Contains the `DataTable` struct, the core in-memory data structure for a single shard.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// DataTable newtype optimized for snapshots with Arc-wrapped values
/// Keys are Vec<u8>, values are Arc<Vec<u8>> for efficient cloning during snapshots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataTable(HashMap<Vec<u8>, Arc<Vec<u8>>>);

impl DataTable {
    /// Create a new data table with specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self(HashMap::with_capacity(capacity))
    }

    /// Create a new empty data table
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Get a value by key
    pub fn get_value(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        self.0.get(key).cloned()
    }

    /// Set a key-value pair, returns the old value if it existed
    pub fn set_value(&mut self, key: Vec<u8>, value: Vec<u8>) -> Option<Arc<Vec<u8>>> {
        self.0.insert(key, Arc::new(value))
    }

    /// Delete a key-value pair, returns the old value if it existed
    pub fn delete_value(&mut self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        self.0.remove(key)
    }

    /// Check if a key exists
    pub fn has_key(&self, key: &[u8]) -> bool {
        self.0.contains_key(key)
    }

    /// Get the number of keys
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the table is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// List all keys in the table
    pub fn all_keys(&self) -> Vec<Vec<u8>> {
        self.0.keys().cloned().collect()
    }

    /// Get multiple values by keys
    pub fn get_multiple(&self, keys: &[Vec<u8>]) -> Vec<(Vec<u8>, Arc<Vec<u8>>)> {
        keys.iter()
            .filter_map(|key| self.0.get(key).map(|value| (key.clone(), value.clone())))
            .collect()
    }

    /// Set multiple key-value pairs, returns the old values
    pub fn set_multiple(
        &mut self,
        items: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Vec<(Vec<u8>, Option<Arc<Vec<u8>>>)> {
        items
            .into_iter()
            .map(|(key, value)| {
                let old_value = self.0.insert(key.clone(), Arc::new(value));
                (key, old_value)
            })
            .collect()
    }

    /// Delete multiple keys, returns the old values
    pub fn delete_multiple(&mut self, keys: Vec<Vec<u8>>) -> Vec<(Vec<u8>, Option<Arc<Vec<u8>>>)> {
        keys.into_iter()
            .map(|key| {
                let old_value = self.0.remove(&key);
                (key, old_value)
            })
            .collect()
    }

    /// Create a snapshot of the data table
    /// This is efficient because Arc<Vec<u8>> values are reference-counted
    pub fn snapshot(&self) -> Self {
        Self(self.0.clone())
    }

    /// Restore from a snapshot
    pub fn restore_snapshot(&mut self, snapshot: DataTable) {
        self.0 = snapshot.0;
    }

    /// Clear all data from the table
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Get the total size of all values in bytes
    pub fn total_value_size(&self) -> usize {
        self.0.values().map(|v| v.len()).sum()
    }

    /// Get the total size of all keys in bytes
    pub fn total_key_size(&self) -> usize {
        self.0.keys().map(|k| k.len()).sum()
    }

    /// Get the total memory usage (keys + values)
    pub fn total_size(&self) -> usize {
        self.0.iter().map(|(k, v)| k.len() + v.len()).sum()
    }

    /// Get an iterator over the entries
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Arc<Vec<u8>>)> {
        self.0.iter()
    }
}

impl Default for DataTable {
    fn default() -> Self {
        Self::new()
    }
}

// Allow deref to HashMap for advanced usage
impl std::ops::Deref for DataTable {
    type Target = HashMap<Vec<u8>, Arc<Vec<u8>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for DataTable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
