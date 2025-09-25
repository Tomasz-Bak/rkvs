use rkvs::{StorageManager, namespace::Namespace};
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Performance metrics for benchmark results
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub operations_per_second: f64,
    pub throughput_mbps: f64,
    pub avg_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub error_rate: f64,
    pub total_operations: usize,
    pub total_errors: usize,
}

/// Benchmark helper functions for different operations
pub struct BenchmarkHelpers;

impl BenchmarkHelpers {
    /// Create a test storage manager with default configuration
    pub fn create_test_storage() -> StorageManager {
        use rkvs::{StorageManagerBuilder, StorageConfig};
        
        StorageManagerBuilder::new()
            .with_config(StorageConfig::default())
            .build()
    }

    /// Setup test data in a namespace
    pub async fn setup_test_data(
        namespace: &Namespace,
        key_prefix: &str,
        count: usize,
        data_size: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for i in 0..count {
            let key = format!("{}_{}", key_prefix, i);
            let mut data = vec![0u8; data_size];
            for j in 0..data.len() {
                data[j] = ((i * j) % 256) as u8;
            }
            namespace.set(key, data).await?;
        }
        Ok(())
    }

    /// Cleanup test data from a namespace
    pub async fn cleanup_test_data(
        namespace: &Namespace,
        key_prefix: &str,
        count: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for i in 0..count {
            let key = format!("{}_{}", key_prefix, i);
            let _ = namespace.delete(&key).await;
        }
        Ok(())
    }
}