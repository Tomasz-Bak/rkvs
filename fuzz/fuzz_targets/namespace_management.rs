#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rkvs::{NamespaceConfig, StorageManager};
use std::collections::HashSet;

const MAX_NAMESPACE_NAME_SIZE: usize = 64;

/// A simplified, `Arbitrary`-derivable version of `NamespaceConfig`.
#[derive(Debug, Arbitrary)]
struct FuzzConfig {
    // Use small types to guide the fuzzer towards more realistic values.
    shard_count: u8,
    max_keys: u32,
    max_value_size: u32,
    lock_timeout: u16,
}

impl From<FuzzConfig> for NamespaceConfig {
    fn from(fuzz_config: FuzzConfig) -> Self {
        let config = NamespaceConfig::default();
        // The fuzzer might generate 0, which our library handles as "unlimited" or "default".
        // We explicitly allow this to test that logic.
        config.set_shard_count(fuzz_config.shard_count as usize);
        config.set_max_keys(fuzz_config.max_keys as usize);
        config.set_max_value_size(fuzz_config.max_value_size as usize);
        // Ensure lock timeout is not 0, as our library rejects that.
        config.set_lock_timeout(fuzz_config.lock_timeout.max(1) as usize);
        config
    }
}

#[derive(Debug, Arbitrary)]
enum FuzzOperation {
    CreateNamespace {
        #[arbitrary(with = |u: &mut Unstructured| {
            let size = u.int_in_range(0..=MAX_NAMESPACE_NAME_SIZE)?;
            String::from_utf8(u.bytes(size)?.to_vec()).map_err(|_| arbitrary::Error::IncorrectFormat)
        })]
        name: String,
        config: FuzzConfig,
    },
    DeleteNamespace {
        #[arbitrary(with = |u: &mut Unstructured| {
            let size = u.int_in_range(0..=MAX_NAMESPACE_NAME_SIZE)?;
            String::from_utf8(u.bytes(size)?.to_vec()).map_err(|_| arbitrary::Error::IncorrectFormat)
        })]
        name: String,
    },
    GetNamespace {
        #[arbitrary(with = |u: &mut Unstructured| {
            let size = u.int_in_range(0..=MAX_NAMESPACE_NAME_SIZE)?;
            String::from_utf8(u.bytes(size)?.to_vec()).map_err(|_| arbitrary::Error::IncorrectFormat)
        })]
        name: String,
    },
    ListNamespaces,
}

fuzz_target!(|operations: Vec<FuzzOperation>| {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        // Use a temporary directory for each run to ensure isolation.
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = StorageManager::builder()
            .with_persistence(temp_dir.path().to_path_buf())
            .build()
            .await
            .unwrap();

        // Keep a simple model of the expected state to verify at the end.
        let mut expected_namespaces = HashSet::new();

        storage.initialize(None).await.unwrap();
        for op in operations {
            match op {
                FuzzOperation::CreateNamespace { name, config } => {
                    if storage.create_namespace(&name, Some(config.into())).await.is_ok() {
                        expected_namespaces.insert(name);
                    }
                }
                FuzzOperation::DeleteNamespace { name } => {
                    if storage.delete_namespace(&name).await.is_ok() {
                        expected_namespaces.remove(&name);
                    }
                }
                FuzzOperation::GetNamespace { name } => {
                    let _ = storage.namespace(&name).await;
                }
                FuzzOperation::ListNamespaces => {
                    let _ = storage.list_namespaces().await;
                }
            }
        }

        // Final assertion: does the storage manager's list of namespaces match our model?
        // We must first unwrap the Result from list_namespaces, then iterate over the Vec.
        let final_namespaces: HashSet<_> = storage.list_namespaces().await.unwrap().into_iter().collect();
        assert_eq!(expected_namespaces, final_namespaces, "The final list of namespaces should match the expected state.");
    });
});