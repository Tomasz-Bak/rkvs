#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use rkvs::{namespace::Namespace, NamespaceConfig};
use std::sync::Arc;

/// A simplified, `Arbitrary`-derivable version of `NamespaceConfig`.
#[derive(Debug, Arbitrary)]
struct FuzzConfig {
    shard_count: u8,
    max_keys: u32,
    max_value_size: u32,
    lock_timeout: u16,
}

impl From<FuzzConfig> for NamespaceConfig {
    fn from(fuzz_config: FuzzConfig) -> Self {
        let config = NamespaceConfig::default();
        config.set_shard_count(fuzz_config.shard_count as usize);
        config.set_max_keys(fuzz_config.max_keys as usize);
        config.set_max_value_size(fuzz_config.max_value_size as usize);
        config.set_lock_timeout(fuzz_config.lock_timeout.max(1) as usize);
        config
    }
}

#[derive(Debug, Arbitrary)]
enum FuzzConfigOperation {
    ResizeShards {
        // u8 keeps the number of shards from getting astronomically large,
        // which would slow down the fuzzer. It still allows for 0, 1, and 255.
        new_count: u8,
    },
    SetMaxKeys {
        max_keys: u32,
    },
    SetMaxValueSize {
        max_value_size: u32,
    },
    SetLockTimeout {
        timeout_ms: u16,
    },
    UpdateConfig {
        config: FuzzConfig,
    },
}

fuzz_target!(|operations: Vec<FuzzConfigOperation>| {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        // Create a new namespace for each run.
        let ns = Arc::new(Namespace::new(
            "fuzz_config_ns".to_string(),
            NamespaceConfig::default(),
        ));

        // Execute the sequence of configuration changes.
        // We only care about panics, so we ignore the results.
        for op in operations {
            match op {
                FuzzConfigOperation::ResizeShards { new_count } => {
                    let _ = ns.resize_shards(new_count as usize).await;
                }
                FuzzConfigOperation::SetMaxKeys { max_keys } => {
                    let _ = ns.set_max_keys(max_keys as usize).await;
                }
                FuzzConfigOperation::SetMaxValueSize { max_value_size } => {
                    let _ = ns.set_max_value_size(max_value_size as usize).await;
                }
                FuzzConfigOperation::SetLockTimeout { timeout_ms } => {
                    let _ = ns.set_lock_timeout(timeout_ms as usize).await;
                }
                FuzzConfigOperation::UpdateConfig { config } => {
                    let _ = ns.update_config(config.into()).await;
                }
            }
        }
    });
});