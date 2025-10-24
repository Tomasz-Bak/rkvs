use rkvs::{NamespaceConfig, Result, StorageConfig, StorageManager};

#[tokio::test]
async fn test_uninitialized_error() -> Result<()> {
    let storage = StorageManager::builder().build().await?;

    // Any operation before initialize() should fail
    let err = storage.create_namespace("test", None).await;
    assert!(err.is_err());
    assert!(matches!(err, Err(rkvs::RkvsError::NotInitialized)));
    if let Err(e) = err {
        assert!(e.to_string().contains("Storage not initialized"));
    }

    Ok(())
}

#[tokio::test]
async fn test_double_initialization() -> Result<()> {
    let storage = StorageManager::builder().build().await?;
    storage.initialize(None).await?;
    assert!(storage.is_initialized());

    // Second initialization should be a no-op and succeed
    storage.initialize(None).await?;
    assert!(storage.is_initialized());

    Ok(())
}

#[tokio::test]
async fn test_create_namespace() -> Result<()> {
    let storage = StorageManager::builder().build().await?;
    storage.initialize(None).await?;

    // Create a namespace
    storage.create_namespace("my_ns", None).await?;

    // Verify it exists
    let ns = storage.namespace("my_ns").await?;
    let metadata = ns.get_metadata().await;
    assert_eq!(metadata.name, "my_ns");

    // Verify it's in the list
    let list = storage.list_namespaces().await?;
    assert_eq!(list, vec!["my_ns"]);

    Ok(())
}

#[tokio::test]
async fn test_create_namespace_with_config() -> Result<()> {
    let storage = StorageManager::builder().build().await?;
    storage.initialize(None).await?;

    let config = NamespaceConfig::default();
    config.set_max_keys(1234);
    config.set_shard_count(10);

    storage
        .create_namespace_with_config("configured_ns", config)
        .await?;

    let ns = storage.namespace("configured_ns").await?;
    let ns_config = ns.get_config().await;

    assert_eq!(ns_config.max_keys(), 1234);
    assert_eq!(ns_config.shard_count(), 10);

    Ok(())
}

#[tokio::test]
async fn test_create_duplicate_namespace_fails() -> Result<()> {
    let storage = StorageManager::builder().build().await?;
    storage.initialize(None).await?;

    storage.create_namespace("my_ns", None).await?;

    // Attempting to create it again should fail
    let err = storage.create_namespace("my_ns", None).await;
    assert!(err.is_err());
    if let Err(e) = err {
        assert!(e.to_string().contains("already exists"));
    }

    Ok(())
}

#[tokio::test]
async fn test_max_namespaces_limit() -> Result<()> {
    let storage_config = StorageConfig {
        max_namespaces: Some(2),
        ..Default::default()
    };

    let storage = StorageManager::builder()
        .with_config(storage_config)
        .build()
        .await?;
    storage.initialize(None).await?;

    // Create two namespaces, which should succeed
    storage.create_namespace("ns1", None).await?;
    storage.create_namespace("ns2", None).await?;

    // The third one should fail
    let err = storage.create_namespace("ns3", None).await;
    assert!(err.is_err());
    if let Err(e) = err {
        assert!(
            e.to_string()
                .contains("Maximum number of namespaces (2) reached")
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_delete_namespace() -> Result<()> {
    let storage = StorageManager::builder().build().await?;
    storage.initialize(None).await?;

    storage.create_namespace("ns_to_delete", None).await?;
    assert!(storage.namespace("ns_to_delete").await.is_ok());
    assert_eq!(storage.get_namespace_count().await?, 1);

    // Delete it
    storage.delete_namespace("ns_to_delete").await?;

    // Verify it's gone
    assert!(storage.namespace("ns_to_delete").await.is_err());
    assert_eq!(storage.get_namespace_count().await?, 0);

    // Deleting a non-existent namespace should fail
    let err = storage.delete_namespace("non_existent").await;
    assert!(err.is_err());

    Ok(())
}

#[tokio::test]
async fn test_get_and_update_storage_config() -> Result<()> {
    let original_config = StorageConfig {
        max_namespaces: Some(5),
        ..Default::default()
    };

    let storage = StorageManager::builder()
        .with_config(original_config.clone())
        .build()
        .await?;
    storage.initialize(None).await?;

    // Check initial config
    let current_config = storage.get_config().await;
    assert_eq!(current_config, original_config);

    // Update config
    let new_config = StorageConfig {
        max_namespaces: Some(10),
        ..Default::default()
    };
    storage.update_config(new_config.clone()).await;

    // Verify updated config
    let updated_config = storage.get_config().await;
    assert_eq!(updated_config, new_config);

    Ok(())
}

#[tokio::test]
async fn test_statistics() -> Result<()> {
    let storage = StorageManager::builder().build().await?;
    storage.initialize(None).await?;

    storage.create_namespace("ns1", None).await?;
    storage.create_namespace("ns2", None).await?;

    let ns1 = storage.namespace("ns1").await?;
    ns1.set("key1", vec![1, 2, 3]).await?;
    ns1.set("key2", vec![4, 5]).await?;

    let ns2 = storage.namespace("ns2").await?;
    ns2.set("keyA", vec![10]).await?;

    // Test counts
    assert_eq!(storage.get_namespace_count().await?, 2);
    assert_eq!(storage.get_total_key_count().await?, 3);

    // Test individual stats
    let stats1 = storage.get_namespace_stats("ns1").await?.unwrap();
    assert_eq!(stats1.name, "ns1");
    assert_eq!(stats1.key_count, 2);
    assert_eq!(stats1.total_size, 5); // 3 + 2

    // Test all stats
    let all_stats = storage.get_all_namespace_stats().await?;
    assert_eq!(all_stats.len(), 2);
    assert!(
        all_stats
            .iter()
            .any(|s| s.name == "ns1" && s.key_count == 2)
    );
    assert!(
        all_stats
            .iter()
            .any(|s| s.name == "ns2" && s.key_count == 1)
    );

    Ok(())
}
