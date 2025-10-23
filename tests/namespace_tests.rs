use rkvs::{namespace::Namespace, BatchMode, NamespaceConfig, Result};
use std::sync::Arc;

/// Helper to create a default namespace for tests
fn create_test_namespace(config: Option<NamespaceConfig>) -> Arc<Namespace> {
    let ns_config = config.unwrap_or_default();
    Arc::new(Namespace::new("test_ns".to_string(), ns_config))
}

#[tokio::test]
async fn test_set_and_get() -> Result<()> {
    let ns = create_test_namespace(None);

    // Insert a new key
    let old_value = ns.set("key1", b"value1".to_vec()).await?;
    assert!(old_value.is_none());

    // Retrieve the key
    let value = ns.get("key1").await.unwrap();
    assert_eq!(*value, b"value1".to_vec());

    // Update the key and check the old value
    let old_value = ns.set("key1", b"value2".to_vec()).await?.unwrap();
    assert_eq!(*old_value, b"value1".to_vec());

    // Verify the new value
    let value = ns.get("key1").await.unwrap();
    assert_eq!(*value, b"value2".to_vec());

    Ok(())
}

#[tokio::test]
async fn test_update_and_consume() -> Result<()> {
    let ns = create_test_namespace(None);

    // Update on a non-existent key should fail
    assert!(ns.update("key1", b"new_value".to_vec()).await.is_err());

    ns.set("key1", b"value1".to_vec()).await?;

    // Update on an existing key should succeed
    let old_value = ns.update("key1", b"value2".to_vec()).await?;
    assert_eq!(*old_value, b"value1".to_vec());

    // Consume the key
    let consumed_value = ns.consume("key1").await.unwrap();
    assert_eq!(*consumed_value, b"value2".to_vec());

    // The key should no longer exist
    assert!(ns.get("key1").await.is_none());
    assert!(!ns.exists("key1").await);

    Ok(())
}

#[tokio::test]
async fn test_delete() -> Result<()> {
    let ns = create_test_namespace(None);

    ns.set("key1", b"value".to_vec()).await?;
    assert!(ns.exists("key1").await);

    // Delete should return true for an existing key
    assert!(ns.delete("key1").await);

    // Key should be gone
    assert!(!ns.exists("key1").await);

    // Delete should return false for a non-existent key
    assert!(!ns.delete("key1").await);

    Ok(())
}

#[tokio::test]
async fn test_max_keys_limit() -> Result<()> {
    let config = NamespaceConfig::default();
    config.set_max_keys(2);
    let ns = create_test_namespace(Some(config));

    // Should be able to insert two keys
    ns.set("key1", vec![1]).await?;
    ns.set("key2", vec![2]).await?;
    assert_eq!(ns.get_metadata().await.key_count(), 2);

    // The third insert should fail
    let err = ns.set("key3", vec![3]).await;
    assert!(err.is_err());
    if let Err(e) = err {
        assert!(e.to_string().contains("Maximum number of keys (2) reached"));
    }

    // But updating an existing key should still work
    ns.set("key1", vec![1, 1]).await?;
    assert_eq!(ns.get_metadata().await.key_count(), 2);

    Ok(())
}

#[tokio::test]
async fn test_max_value_size_limit() -> Result<()> {
    let config = NamespaceConfig::default();
    config.set_max_value_size(5);
    let ns = create_test_namespace(Some(config));

    // This should succeed
    ns.set("key1", vec![1, 2, 3, 4, 5]).await?;

    // This should fail
    let err = ns.set("key2", vec![1, 2, 3, 4, 5, 6]).await;
    assert!(err.is_err());
    if let Err(e) = err {
        assert!(e.to_string().contains("Value size 6 exceeds maximum allowed size 5"));
    }

    Ok(())
}

#[tokio::test]
async fn test_update_namespace_config() -> Result<()> {
    let ns = create_test_namespace(None);

    ns.set("key1", vec![1]).await?;
    ns.set("key2", vec![2]).await?;

    // Try to set max_keys lower than current count, should fail
    // 0 = usize::MAX
    let err = ns.set_max_keys(1).await;
    assert!(err.is_err());

    // Update config successfully
    let new_config = NamespaceConfig::default();
    new_config.set_max_keys(100);
    new_config.set_max_value_size(200);
    ns.update_config(new_config).await?;

    let current_config = ns.get_config().await;
    assert_eq!(current_config.max_keys(), 100);
    assert_eq!(current_config.max_value_size(), 200);

    Ok(())
}

#[tokio::test]
async fn test_resize_shards() -> Result<()> {
    let config = NamespaceConfig::default();
    config.set_shard_count(1);
    let ns = create_test_namespace(Some(config));

    // Add 100 keys
    for i in 0..100 {
        ns.set(&format!("key_{}", i), vec![i as u8]).await?;
    }

    assert_eq!(ns.get_config().await.shard_count(), 1);
    let shard_counts_before = ns.get_shard_key_counts().await;
    assert_eq!(shard_counts_before, vec![100]);

    // Resize to 4 shards
    ns.resize_shards(4).await?;
    assert_eq!(ns.get_config().await.shard_count(), 4);

    // Verify all keys are still present
    for i in 0..100 {
        let val = ns.get(&format!("key_{}", i)).await.unwrap();
        assert_eq!(*val, vec![i as u8]);
    }

    // Verify keys have been redistributed
    let shard_counts_after = ns.get_shard_key_counts().await;
    assert_eq!(shard_counts_after.len(), 4);
    assert_eq!(shard_counts_after.iter().sum::<usize>(), 100);
    assert_ne!(shard_counts_after, vec![100, 0, 0, 0]); // Make sure they moved

    // Resizing to a smaller count should fail
    assert!(ns.resize_shards(2).await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_batch_set_all_or_nothing() -> Result<()> {
    let ns = create_test_namespace(None);
    let items = vec![
        ("key1".to_string(), vec![1]),
        ("key2".to_string(), vec![2]),
    ];

    let result = ns.set_multiple(items, BatchMode::AllOrNothing).await;
    assert!(result.errors.is_none());
    assert_eq!(result.data.unwrap().len(), 2);

    assert!(ns.exists("key1").await);
    assert!(ns.exists("key2").await);

    // Test failure case (exceeds max value size)
    let config = NamespaceConfig::default();
    config.set_max_value_size(1);
    let ns2 = create_test_namespace(Some(config));
    let failing_items = vec![
        ("keyA".to_string(), vec![1]),
        ("keyB".to_string(), vec![2, 3]), // This one will fail
    ];

    let result2 = ns2.set_multiple(failing_items, BatchMode::AllOrNothing).await;
    assert!(result2.errors.is_some());
    assert!(result2.data.is_none());

    // Neither key should have been set
    assert!(!ns2.exists("keyA").await);
    assert!(!ns2.exists("keyB").await);

    Ok(())
}

#[tokio::test]
async fn test_batch_get_best_effort() -> Result<()> {
    let ns = create_test_namespace(None);
    ns.set("key1", vec![1]).await?;
    ns.set("key3", vec![3]).await?;

    let keys_to_get = vec![
        "key1".to_string(),
        "key2".to_string(), // Does not exist
        "key3".to_string(),
    ];

    let result = ns.get_multiple(keys_to_get, BatchMode::BestEffort).await;

    // Should have one error for the missing key
    assert_eq!(result.errors.as_ref().unwrap().len(), 1);
    assert_eq!(result.errors.unwrap()[0].key, "key2");

    // Should have data for the two found keys
    let mut data = result.data.unwrap();
    data.sort_by(|a, b| a.0.cmp(&b.0)); // Sort for deterministic check
    assert_eq!(data.len(), 2);
    assert_eq!(data[0], ("key1".to_string(), vec![1]));
    assert_eq!(data[1], ("key3".to_string(), vec![3]));

    Ok(())
}

#[tokio::test]
async fn test_batch_delete_all_or_nothing() -> Result<()> {
    let ns = create_test_namespace(None);
    ns.set("key1", vec![1]).await?;
    ns.set("key2", vec![2]).await?;

    // This batch should fail because key3 doesn't exist
    let keys_to_delete_fail = vec![
        "key1".to_string(),
        "key3".to_string(), // Does not exist
    ];
    let result_fail = ns.delete_multiple(keys_to_delete_fail, BatchMode::AllOrNothing).await;
    assert!(result_fail.errors.is_some());

    // No keys should have been deleted
    assert!(ns.exists("key1").await);
    assert!(ns.exists("key2").await);

    // This batch should succeed
    let keys_to_delete_ok = vec!["key1".to_string(), "key2".to_string()];
    let result_ok = ns.delete_multiple(keys_to_delete_ok, BatchMode::AllOrNothing).await;
    assert!(result_ok.errors.is_none());

    // Both keys should be gone
    assert!(!ns.exists("key1").await);
    assert!(!ns.exists("key2").await);

    Ok(())
}