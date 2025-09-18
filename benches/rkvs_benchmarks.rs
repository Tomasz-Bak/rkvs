use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rkvs::{StorageManager, StorageManagerBuilder, StorageConfig, NamespaceConfig};
use tempfile::TempDir;
use tokio::runtime::Runtime;

fn create_test_storage() -> StorageManager {
    StorageManagerBuilder::new()
        .with_config(StorageConfig {
            max_namespaces: Some(1000),
            default_max_keys_per_namespace: Some(10000),
            default_max_value_size: Some(1024 * 1024), // 1MB
        })
        .build()
}

fn create_test_storage_with_persistence() -> (StorageManager, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let storage_path = temp_dir.path().join("benchmark_storage.bin");
    
    let storage = StorageManagerBuilder::new()
        .with_config(StorageConfig {
            max_namespaces: Some(1000),
            default_max_keys_per_namespace: Some(10000),
            default_max_value_size: Some(1024 * 1024),
        })
        .with_persistence(storage_path)
        .build();
    
    (storage, temp_dir)
}

fn bench_namespace_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("namespace_create", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                storage.create_namespace("benchmark_ns", None).await.unwrap()
            })
        })
    });

    c.bench_function("namespace_get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                namespace
            })
        })
    });

    c.bench_function("namespace_delete", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                storage.delete_namespace(ns_hash).await.unwrap()
            })
        })
    });
}

fn bench_key_value_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("set_small_value", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                namespace.set("key1".to_string(), b"small_value".to_vec()).await.unwrap()
            })
        })
    });

    c.bench_function("set_medium_value", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                let medium_value = vec![0u8; 1024]; // 1KB
                namespace.set("key1".to_string(), medium_value).await.unwrap()
            })
        })
    });

    c.bench_function("set_large_value", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                let large_value = vec![0u8; 64 * 1024]; // 64KB
                namespace.set("key1".to_string(), large_value).await.unwrap()
            })
        })
    });

    c.bench_function("get_existing_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                namespace.set("key1".to_string(), b"test_value".to_vec()).await.unwrap();
                
                namespace.get("key1").await
            })
        })
    });

    c.bench_function("get_nonexistent_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                namespace.get("nonexistent_key").await
            })
        })
    });

    c.bench_function("delete_existing_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                namespace.set("key1".to_string(), b"test_value".to_vec()).await.unwrap();
                
                namespace.delete("key1").await
            })
        })
    });

    c.bench_function("exists_check", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                namespace.set("key1".to_string(), b"test_value".to_vec()).await.unwrap();
                
                namespace.exists("key1").await
            })
        })
    });

    c.bench_function("consume_existing_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                namespace.set("key1".to_string(), b"test_value".to_vec()).await.unwrap();
                
                namespace.consume("key1").await
            })
        })
    });

    c.bench_function("consume_nonexistent_key", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                namespace.consume("nonexistent_key").await
            })
        })
    });
}

fn bench_bulk_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    let sizes = [10, 100, 1000, 10000];
    
    for &size in &sizes {
        c.bench_with_input(BenchmarkId::new("bulk_set", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let storage = create_test_storage();
                    storage.initialize().await.unwrap();
                    let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                    let namespace = storage.namespace(ns_hash).await.unwrap();
                    
                    for i in 0..size {
                        let key = format!("key_{}", i);
                        let value = format!("value_{}", i).into_bytes();
                        namespace.set(key, value).await.unwrap();
                    }
                })
            })
        });
    }

    for &size in &sizes {
        c.bench_with_input(BenchmarkId::new("bulk_get", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let storage = create_test_storage();
                    storage.initialize().await.unwrap();
                    let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                    let namespace = storage.namespace(ns_hash).await.unwrap();
                    
                    for i in 0..size {
                        let key = format!("key_{}", i);
                        let value = format!("value_{}", i).into_bytes();
                        namespace.set(key, value).await.unwrap();
                    }
                    
                    for i in 0..size {
                        let key = format!("key_{}", i);
                        black_box(namespace.get(&key).await);
                    }
                })
            })
        });
    }
}

fn bench_concurrent_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("concurrent_reads", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                for i in 0..100 {
                    let key = format!("key_{}", i);
                    let value = format!("value_{}", i).into_bytes();
                    namespace.set(key, value).await.unwrap();
                }
                
                // Concurrent reads
                let handles: Vec<_> = (0..10).map(|_| {
                    let namespace = namespace.clone();
                    tokio::spawn(async move {
                        for i in 0..100 {
                            let key = format!("key_{}", i);
                            black_box(namespace.get(&key).await);
                        }
                    })
                }).collect();
                
                for handle in handles {
                    handle.await.unwrap();
                }
            })
        })
    });

    c.bench_function("concurrent_writes", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                // Concurrent writes
                let handles: Vec<_> = (0..10).map(|thread_id| {
                    let namespace = namespace.clone();
                    tokio::spawn(async move {
                        for i in 0..100 {
                            let key = format!("key_{}_{}", thread_id, i);
                            let value = format!("value_{}_{}", thread_id, i).into_bytes();
                            namespace.set(key, value).await.unwrap();
                        }
                    })
                }).collect();
                
                for handle in handles {
                    handle.await.unwrap();
                }
            })
        })
    });
}

fn bench_persistence_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("save_to_disk", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage_with_persistence();
                storage.initialize().await.unwrap();
                
                // Add some data
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                for i in 0..1000 {
                    let key = format!("key_{}", i);
                    let value = format!("value_{}", i).into_bytes();
                    namespace.set(key, value).await.unwrap();
                }
                
                storage.save().await.unwrap()
            })
        })
    });

    c.bench_function("load_from_disk", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (storage, _temp_dir) = create_test_storage_with_persistence();
                storage.initialize().await.unwrap();
                
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                for i in 0..1000 {
                    let key = format!("key_{}", i);
                    let value = format!("value_{}", i).into_bytes();
                    namespace.set(key, value).await.unwrap();
                }
                storage.save().await.unwrap();
                
                let (new_storage, _temp_dir) = create_test_storage_with_persistence();
                new_storage.initialize().await.unwrap()
            })
        })
    });
}

fn bench_config_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    
    c.bench_function("update_namespace_config", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                
                let new_config = NamespaceConfig {
                    max_keys: Some(5000),
                    max_value_size: Some(512 * 1024),
                };
                
                storage.update_namespace_config(ns_hash, new_config).await.unwrap()
            })
        })
    });

    c.bench_function("get_namespace_config", |b| {
        b.iter(|| {
            rt.block_on(async {
                let storage = create_test_storage();
                storage.initialize().await.unwrap();
                let ns_hash = storage.create_namespace("benchmark_ns", None).await.unwrap();
                let namespace = storage.namespace(ns_hash).await.unwrap();
                
                namespace.get_config().await
            })
        })
    });
}

criterion_group!(
    benches,
    bench_namespace_operations,
    bench_key_value_operations,
    bench_bulk_operations,
    bench_concurrent_operations,
    bench_persistence_operations,
    bench_config_operations
);
criterion_main!(benches);