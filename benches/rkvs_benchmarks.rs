use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use std::time::Duration;
mod benchmark_helpers;
use benchmark_helpers::BenchmarkHelpers;

/// Comprehensive RKVS benchmark suite with realistic performance measurements
fn benchmark_single_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let (_storage, namespace) = rt.block_on(async {
        let storage = BenchmarkHelpers::create_test_storage();
        storage.initialize().await.unwrap();
        
        storage.create_namespace("single_ops", None).await.unwrap();
        let namespace = storage.namespace("single_ops").await.unwrap();
        
        // Setup test data
        let data = vec![0u8; 1024];
        namespace.set("test_key".to_string(), data.clone()).await.unwrap();
        namespace.set("test_key_2".to_string(), data.clone()).await.unwrap();
        
        (storage, namespace)
    });
    
    let mut group = c.benchmark_group("single_operations");
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));
    
    // Get operation benchmark
    group.bench_function("get_operation", |b| {
        b.iter(|| {
            rt.block_on(async {
                let result = namespace.get("test_key").await;
                // Force computation to prevent optimization
                let _ = result.map(|v| v.len());
            })
        })
    });
    
    // Set operation benchmark
    group.bench_function("set_operation", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut data = vec![0u8; 1024];
                for i in 0..data.len() {
                    data[i] = (i % 256) as u8;
                }
                let _ = namespace.set("test_key".to_string(), data).await;
            })
        })
    });
    
    // Delete operation benchmark
    group.bench_function("delete_operation", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = namespace.delete("test_key").await;
            })
        })
    });
    
    // Exists operation benchmark
    group.bench_function("exists_operation", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = namespace.exists("test_key_2").await;
            })
        })
    });
    
    // Consume operation benchmark
    group.bench_function("consume_operation", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = namespace.consume("test_key_2").await;
            })
        })
    });
    
    group.finish();
}

fn benchmark_bulk_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let (_storage, namespace) = rt.block_on(async {
        let storage = BenchmarkHelpers::create_test_storage();
        storage.initialize().await.unwrap();
        
        storage.create_namespace("bulk_ops", None).await.unwrap();
        let namespace = storage.namespace("bulk_ops").await.unwrap();
        
        // Setup test data
        BenchmarkHelpers::setup_test_data(&namespace, "bulk_key", 100, 1024).await.unwrap();
        
        (storage, namespace)
    });
    
    let mut group = c.benchmark_group("bulk_operations");
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));
    
    // Bulk get operations
    for size in [10, 50, 100].iter() {
        group.bench_with_input(BenchmarkId::new("bulk_get", size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    for i in 0..size {
                        let key = format!("bulk_key_{}", i);
                        let result = namespace.get(&key).await;
                        let _ = result.map(|v| v.len());
                    }
                })
            })
        });
    }
    
    // Bulk set operations
    for size in [10, 50, 100].iter() {
        group.bench_with_input(BenchmarkId::new("bulk_set", size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    for i in 0..size {
                        let key = format!("bulk_set_key_{}", i);
                        let mut data = vec![0u8; 1024];
                        for j in 0..data.len() {
                            data[j] = ((i * j) % 256) as u8;
                        }
                        let _ = namespace.set(key, data).await;
                    }
                })
            })
        });
    }
    
    group.finish();
    
    // Cleanup
    rt.block_on(async {
        BenchmarkHelpers::cleanup_test_data(&namespace, "bulk_key", 100).await.unwrap();
    });
}

fn benchmark_concurrent_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let (_storage, namespace) = rt.block_on(async {
        let storage = BenchmarkHelpers::create_test_storage();
        storage.initialize().await.unwrap();
        
        storage.create_namespace("concurrent_ops", None).await.unwrap();
        let namespace = storage.namespace("concurrent_ops").await.unwrap();
        
        // Setup test data
        BenchmarkHelpers::setup_test_data(&namespace, "concurrent_key", 50, 1024).await.unwrap();
        
        (storage, namespace)
    });
    
    let mut group = c.benchmark_group("concurrent_operations");
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));
    
    // Concurrent read operations
    for concurrency in [2, 5, 10, 20].iter() {
        group.bench_with_input(BenchmarkId::new("concurrent_reads", concurrency), concurrency, |b, &concurrency| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = Vec::new();
                    
                    for _ in 0..concurrency {
                        let namespace = namespace.clone();
                        let handle = tokio::spawn(async move {
                            for i in 0..5 {
                                let key = format!("concurrent_key_{}", i);
                                let result = namespace.get(&key).await;
                                let _ = result.map(|v| v.len());
                            }
                        });
                        handles.push(handle);
                    }
                    
                    for handle in handles {
                        let _ = handle.await;
                    }
                })
            })
        });
    }
    
    // Concurrent write operations
    for concurrency in [2, 5, 10].iter() {
        group.bench_with_input(BenchmarkId::new("concurrent_writes", concurrency), concurrency, |b, &concurrency| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = Vec::new();
                    
                    for thread_id in 0..concurrency {
                        let namespace = namespace.clone();
                        let handle = tokio::spawn(async move {
                            for i in 0..5 {
                                let key = format!("concurrent_write_{}_{}", thread_id, i);
                                let mut data = vec![0u8; 1024];
                                for j in 0..data.len() {
                                    data[j] = ((thread_id * i * j) % 256) as u8;
                                }
                                let _ = namespace.set(key, data).await;
                            }
                        });
                        handles.push(handle);
                    }
                    
                    for handle in handles {
                        let _ = handle.await;
                    }
                })
            })
        });
    }
    
    group.finish();
    
    // Cleanup
    rt.block_on(async {
        BenchmarkHelpers::cleanup_test_data(&namespace, "concurrent_key", 50).await.unwrap();
    });
}

fn benchmark_data_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let (_storage, namespace) = rt.block_on(async {
        let storage = BenchmarkHelpers::create_test_storage();
        storage.initialize().await.unwrap();
        
        storage.create_namespace("data_sizes", None).await.unwrap();
        let namespace = storage.namespace("data_sizes").await.unwrap();
        
        (storage, namespace)
    });
    
    let mut group = c.benchmark_group("data_sizes");
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));
    
    // Test different data sizes
    for size in [64, 256, 1024, 4096, 16384].iter() {
        // Setup data for this size
        rt.block_on(async {
            let mut data = vec![0u8; *size];
            for i in 0..data.len() {
                data[i] = (i % 256) as u8;
            }
            namespace.set("size_test_key".to_string(), data.clone()).await.unwrap();
        });
        
        group.bench_with_input(BenchmarkId::new("get_operation", size), size, |b, &_size| {
            b.iter(|| {
                rt.block_on(async {
                    let result = namespace.get("size_test_key").await;
                    let _ = result.map(|v| v.len());
                })
            })
        });
        
        group.bench_with_input(BenchmarkId::new("set_operation", size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let mut data = vec![0u8; size];
                    for i in 0..data.len() {
                        data[i] = (i % 256) as u8;
                    }
                    let _ = namespace.set("size_test_key".to_string(), data).await;
                })
            })
        });
    }
    
    group.finish();
}

fn benchmark_realistic_workloads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let (_storage, namespace) = rt.block_on(async {
        let storage = BenchmarkHelpers::create_test_storage();
        storage.initialize().await.unwrap();
        
        storage.create_namespace("realistic_workloads", None).await.unwrap();
        let namespace = storage.namespace("realistic_workloads").await.unwrap();
        
        // Setup realistic test data
        for i in 0..100 {
            let key = format!("workload_key_{}", i);
            let mut data = vec![0u8; 1024];
            for j in 0..data.len() {
                data[j] = ((i * j) % 256) as u8;
            }
            namespace.set(key, data).await.unwrap();
        }
        
        (storage, namespace)
    });
    
    let mut group = c.benchmark_group("realistic_workloads");
    group.measurement_time(Duration::from_secs(15));
    group.warm_up_time(Duration::from_secs(3));
    
    // Mixed read/write workload
    group.bench_function("mixed_read_write", |b| {
        b.iter(|| {
            rt.block_on(async {
                for i in 0..20 {
                    let key = format!("workload_key_{}", i);
                    if i % 3 == 0 {
                        // Write operation
                        let mut data = vec![0u8; 1024];
                        for j in 0..data.len() {
                            data[j] = ((i * j + 1000) % 256) as u8;
                        }
                        let _ = namespace.set(key, data).await;
                    } else {
                        // Read operation
                        let result = namespace.get(&key).await;
                        let _ = result.map(|v| v.len());
                    }
                }
            })
        })
    });
    
    // Read-heavy workload
    group.bench_function("read_heavy", |b| {
        b.iter(|| {
            rt.block_on(async {
                for i in 0..50 {
                    let key = format!("workload_key_{}", i);
                    let result = namespace.get(&key).await;
                    let _ = result.map(|v| v.len());
                }
            })
        })
    });
    
    // Write-heavy workload
    group.bench_function("write_heavy", |b| {
        b.iter(|| {
            rt.block_on(async {
                for i in 0..20 {
                    let key = format!("write_heavy_key_{}", i);
                    let mut data = vec![0u8; 1024];
                    for j in 0..data.len() {
                        data[j] = ((i * j + 2000) % 256) as u8;
                    }
                    let _ = namespace.set(key, data).await;
                }
            })
        })
    });
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_single_operations,
    benchmark_bulk_operations,
    benchmark_concurrent_operations,
    benchmark_data_sizes,
    benchmark_realistic_workloads
);

criterion_main!(benches);