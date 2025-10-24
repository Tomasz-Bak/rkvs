//! Contains methods for inspecting the `Namespace` state.
use super::Namespace;
use crate::types::NamespaceMetadata;

impl Namespace {
    /// Gets a clone of the namespace's metadata.
    pub async fn get_metadata(&self) -> NamespaceMetadata {
        self.metadata.clone()
    }

    /// Returns a vector containing the number of keys in each shard.
    /// The index of the vector corresponds to the shard index.
    pub async fn get_shard_key_counts(&self) -> Vec<usize> {
        let shards_guard = self.shards.read().await;
        let mut counts = Vec::with_capacity(shards_guard.len());

        for shard in shards_guard.iter() {
            let data = shard.read().await;
            counts.push(data.len());
        }
        counts
    }
}
