use rkvs::{
    ManagerAutosaveConfig, NamespaceAutosaveConfig, NamespaceConfig, Result, StorageConfig,
    StorageManager,
};
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn test_save_and_load_manager_snapshot() -> Result<()> {
    // 1. Setup a temporary directory for persistence.
    let temp_dir = tempdir()?;
    let persistence_path = temp_dir.path().to_path_buf();
    let snapshot_filename = "test_full_snapshot.bin";

    let ns1_name = "users";
    let ns1_key = "user:1";
    let ns1_value = b"Alice";

    let ns2_name = "products";
    let ns2_key = "product:xyz";
    let ns2_value = b"Widget";

    // --- Phase 1: Create data and save it ---
    {
        // Create a StorageManager with a specific config
        let original_storage_config = StorageConfig {
            max_namespaces: Some(10),
            manager_autosave: Some(ManagerAutosaveConfig {
                interval: Duration::from_secs(300),
                filename: "autosave_manager.bin".to_string(),
            }),
            namespace_autosave: vec![NamespaceAutosaveConfig {
                namespace_name: ns1_name.to_string(), // This config is for ns1
                interval: Duration::from_secs(60),
                filename_pattern: "autosave_ns1.bin".to_string(),
            }],
        };

        let storage1 = StorageManager::builder()
            .with_config(original_storage_config.clone()) // Clone to keep original for comparison
            .with_persistence(persistence_path.clone())
            .build()
            .await?;

        storage1.initialize(None).await?;

        // Create two namespaces with different configs
        let ns1_config = NamespaceConfig::default();
        ns1_config.set_shard_count(2);
        storage1
            .create_namespace(ns1_name, Some(ns1_config))
            .await?;

        let ns2_config = NamespaceConfig::default();
        ns2_config.set_shard_count(4);
        storage1
            .create_namespace(ns2_name, Some(ns2_config))
            .await?;

        // Add data to both namespaces
        let namespace1 = storage1.namespace(ns1_name).await?;
        namespace1.set(ns1_key, ns1_value.to_vec()).await?;

        let namespace2 = storage1.namespace(ns2_name).await?;
        namespace2.set(ns2_key, ns2_value.to_vec()).await?;

        // Verify data exists before saving
        assert_eq!(
            namespace1.get(ns1_key).await.map(|v| (*v).clone()),
            Some(ns1_value.to_vec())
        );
        assert_eq!(
            namespace2.get(ns2_key).await.map(|v| (*v).clone()),
            Some(ns2_value.to_vec())
        );

        // Save a snapshot
        storage1.save_all(snapshot_filename).await?;

        // --- Phase 2: Load data into a new instance and verify ---
        // Create a *new* StorageManager instance with a default config
        let storage2 = StorageManager::builder()
            .with_persistence(persistence_path.clone())
            .build()
            .await?;

        // Initialize from the snapshot
        storage2.initialize(Some(snapshot_filename)).await?;

        // Verify manager-level configuration was restored
        let loaded_storage_config = storage2.get_config().await;
        assert_eq!(loaded_storage_config, original_storage_config);

        // Verify both namespaces and their data were loaded
        let loaded_ns1 = storage2.namespace(ns1_name).await?;
        assert_eq!(
            loaded_ns1.get(ns1_key).await.map(|v| (*v).clone()),
            Some(ns1_value.to_vec())
        );

        let loaded_ns2 = storage2.namespace(ns2_name).await?;
        assert_eq!(
            loaded_ns2.get(ns2_key).await.map(|v| (*v).clone()),
            Some(ns2_value.to_vec())
        );

        // Verify namespace-specific configurations were restored
        let loaded_ns1_config = loaded_ns1.get_config().await;
        assert_eq!(loaded_ns1_config.shard_count(), 2);

        let loaded_ns2_config = loaded_ns2.get_config().await;
        assert_eq!(loaded_ns2_config.shard_count(), 4);
    }

    Ok(())
}

#[tokio::test]
async fn test_save_and_load_single_namespace_snapshot() -> Result<()> {
    // 1. Setup a temporary directory for persistence.
    let temp_dir = tempdir()?;
    let persistence_path = temp_dir.path().to_path_buf();
    let ns_snapshot_filename = "test_single_ns_snapshot.bin";

    let ns1_name = "namespace_to_save";
    let ns1_key = "data_key_1";
    let ns1_value = b"Value for NS1";

    let ns2_name = "other_namespace";
    let ns2_key = "data_key_2";
    let ns2_value = b"Value for NS2 (should not be loaded)";

    // --- Phase 1: Create data and save a single namespace ---
    {
        let storage1 = StorageManager::builder()
            .with_persistence(persistence_path.clone())
            .build()
            .await?;

        storage1.initialize(None).await?;

        // Create namespace 1 with data
        let ns1_config = NamespaceConfig::default();
        ns1_config.set_shard_count(3);
        storage1
            .create_namespace(ns1_name, Some(ns1_config))
            .await?;
        let namespace1 = storage1.namespace(ns1_name).await?;
        namespace1.set(ns1_key, ns1_value.to_vec()).await?;

        // Create namespace 2 with data (this one will NOT be saved individually)
        let ns2_config = NamespaceConfig::default();
        ns2_config.set_shard_count(5);
        storage1
            .create_namespace(ns2_name, Some(ns2_config))
            .await?;
        let namespace2 = storage1.namespace(ns2_name).await?;
        namespace2.set(ns2_key, ns2_value.to_vec()).await?;

        // Verify data exists before saving
        assert_eq!(
            namespace1.get(ns1_key).await.map(|v| (*v).clone()),
            Some(ns1_value.to_vec())
        );
        assert_eq!(
            namespace2.get(ns2_key).await.map(|v| (*v).clone()),
            Some(ns2_value.to_vec())
        );

        // Save a snapshot of ONLY namespace1
        storage1
            .save_namespace(ns1_name, ns_snapshot_filename)
            .await?;
    }

    // --- Phase 2: Load the single namespace snapshot into a new instance and verify ---
    {
        // Create a *new* StorageManager instance
        let storage2 = StorageManager::builder()
            .with_persistence(persistence_path.clone())
            .build()
            .await?;

        // Initialize without loading a full snapshot, so it starts empty
        storage2.initialize(None).await?;

        // Verify no namespaces are present initially
        assert!(storage2.namespace(ns1_name).await.is_err());
        assert!(storage2.namespace(ns2_name).await.is_err());

        // Load the single namespace snapshot using the new manager function
        let loaded_name = storage2.load_namespace(ns_snapshot_filename).await?;
        assert_eq!(loaded_name, ns1_name);

        // Verify namespace1 and its data were loaded
        let loaded_ns1 = storage2.namespace(ns1_name).await?;
        assert_eq!(
            loaded_ns1.get(ns1_key).await.map(|v| (*v).clone()),
            Some(ns1_value.to_vec())
        );
        assert_eq!(loaded_ns1.get_config().await.shard_count(), 3);

        // Verify namespace2 was NOT loaded
        assert!(storage2.namespace(ns2_name).await.is_err());
    }

    Ok(())
}

#[tokio::test]
async fn test_load_namespace_with_new_name() -> Result<()> {
    // 1. Setup
    let temp_dir = tempdir()?;
    let persistence_path = temp_dir.path().to_path_buf();
    let snapshot_filename = "base_snapshot.bin";

    let original_ns_name = "original_namespace";
    let key = "test_key";
    let value = b"test_value";

    let new_name_1 = "clone_1";
    let new_name_2 = "clone_2";

    // --- Phase 1: Create and save a single namespace snapshot ---
    {
        let storage1 = StorageManager::builder()
            .with_persistence(persistence_path.clone())
            .build()
            .await?;
        storage1.initialize(None).await?;

        let ns_config = NamespaceConfig::default();
        ns_config.set_shard_count(2);
        storage1
            .create_namespace(original_ns_name, Some(ns_config))
            .await?;
        let namespace = storage1.namespace(original_ns_name).await?;
        namespace.set(key, value.to_vec()).await?;

        // Save the snapshot
        storage1
            .save_namespace(original_ns_name, snapshot_filename)
            .await?;
    }

    // --- Phase 2: Load the snapshot multiple times with new names ---
    {
        let storage2 = StorageManager::builder()
            .with_persistence(persistence_path.clone())
            .build()
            .await?;
        storage2.initialize(None).await?;

        // Load the first clone
        let loaded_name_1 = storage2
            .load_namespace_with_name(snapshot_filename, new_name_1)
            .await?;
        assert_eq!(loaded_name_1, new_name_1);

        // Load the second clone
        let loaded_name_2 = storage2
            .load_namespace_with_name(snapshot_filename, new_name_2)
            .await?;
        assert_eq!(loaded_name_2, new_name_2);

        // Verify the original namespace name does NOT exist
        assert!(storage2.namespace(original_ns_name).await.is_err());

        // Verify first clone
        let ns1 = storage2.namespace(new_name_1).await?;
        assert_eq!(
            ns1.get(key).await.map(|v| (*v).clone()),
            Some(value.to_vec())
        );
        assert_eq!(ns1.get_config().await.shard_count(), 2);

        // Verify second clone
        let ns2 = storage2.namespace(new_name_2).await?;
        assert_eq!(
            ns2.get(key).await.map(|v| (*v).clone()),
            Some(value.to_vec())
        );
        assert_eq!(ns2.get_config().await.shard_count(), 2);
    }

    Ok(())
}
