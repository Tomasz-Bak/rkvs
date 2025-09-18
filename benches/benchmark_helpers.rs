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

    /// Run a realistic benchmark with proper warmup and anti-optimization measures
    pub async fn run_realistic_benchmark<F, Fut>(
        operation: F,
        duration: Duration,
        warmup_duration: Duration,
    ) -> PerformanceMetrics
    where
        F: Fn() -> Fut + Send + Sync + Clone,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        // Warmup phase
        let warmup_start = Instant::now();
        while warmup_start.elapsed() < warmup_duration {
            let _ = operation().await;
        }
        
        // Actual benchmark phase
        let start = Instant::now();
        let mut latencies = Vec::new();
        let mut operations = 0;
        let mut errors = 0;
        
        // Use atomic counter to prevent compiler optimizations
        let operation_counter = AtomicUsize::new(0);
        let error_counter = AtomicUsize::new(0);
        let mut last_yield = 0;

        while start.elapsed() < duration {
            let op_start = Instant::now();
            match operation().await {
                Ok(_) => {
                    operation_counter.fetch_add(1, Ordering::Relaxed);
                    operations += 1;
                    latencies.push(op_start.elapsed().as_secs_f64() * 1000.0);
                }
                Err(_) => {
                    error_counter.fetch_add(1, Ordering::Relaxed);
                    errors += 1;
                }
            }
            
            // Yield periodically to prevent unrealistic optimization
            if operations - last_yield > 100 {
                tokio::task::yield_now().await;
                last_yield = operations;
            }
        }

        let total_duration = start.elapsed().as_secs_f64();
        Self::calculate_metrics_realistic(operations, errors, latencies, 0, total_duration)
    }

    /// Run concurrent benchmark with multiple threads
    pub async fn run_concurrent_benchmark<F, Fut>(
        operation: F,
        concurrency: usize,
        duration: Duration,
    ) -> PerformanceMetrics
    where
        F: Fn() -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
    {
        let start = Instant::now();
        let mut handles = Vec::new();
        let operation_counter = Arc::new(AtomicUsize::new(0));
        let error_counter = Arc::new(AtomicUsize::new(0));
        let latencies = Arc::new(std::sync::Mutex::new(Vec::new()));

        // Spawn concurrent tasks
        for _ in 0..concurrency {
            let operation = operation.clone();
            let counter = operation_counter.clone();
            let error_counter = error_counter.clone();
            let latencies = latencies.clone();
            
            let handle = tokio::spawn(async move {
                let mut local_operations = 0;
                let mut local_errors = 0;
                let mut local_latencies = Vec::new();

                while start.elapsed() < duration {
                    let op_start = Instant::now();
                    match operation().await {
                        Ok(_) => {
                            counter.fetch_add(1, Ordering::Relaxed);
                            local_operations += 1;
                            local_latencies.push(op_start.elapsed().as_secs_f64() * 1000.0);
                        }
                        Err(_) => {
                            error_counter.fetch_add(1, Ordering::Relaxed);
                            local_errors += 1;
                        }
                    }
                    
                    // Yield periodically
                    if local_operations % 50 == 0 {
                        tokio::task::yield_now().await;
                    }
                }

                // Store latencies
                if let Ok(mut latencies) = latencies.lock() {
                    latencies.extend(local_latencies);
                }
            });
            
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            let _ = handle.await;
        }

        let total_operations = operation_counter.load(Ordering::Relaxed);
        let total_errors = error_counter.load(Ordering::Relaxed);
        let all_latencies = latencies.lock().unwrap().clone();
        let total_duration = start.elapsed().as_secs_f64();

        Self::calculate_metrics_realistic(total_operations, total_errors, all_latencies, 0, total_duration)
    }

    /// Calculate performance metrics from raw data with realistic timing
    fn calculate_metrics_realistic(
        operations: usize,
        errors: usize,
        mut latencies: Vec<f64>,
        data_size: usize,
        total_duration_seconds: f64,
    ) -> PerformanceMetrics {
        let ops_per_second = if total_duration_seconds > 0.0 {
            operations as f64 / total_duration_seconds
        } else {
            0.0
        };
        
        let error_rate = if operations + errors > 0 {
            errors as f64 / (operations + errors) as f64
        } else {
            0.0
        };
        
        let throughput_mbps = if data_size > 0 {
            (ops_per_second * data_size as f64) / (1024.0 * 1024.0)
        } else {
            0.0
        };

        // Calculate percentiles
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let len = latencies.len();
        
        let p50 = if len > 0 { latencies[len * 50 / 100] } else { 0.0 };
        let p95 = if len > 0 { latencies[len * 95 / 100] } else { 0.0 };
        let p99 = if len > 0 { latencies[len * 99 / 100] } else { 0.0 };
        
        let avg_latency = if len > 0 {
            latencies.iter().sum::<f64>() / len as f64
        } else {
            0.0
        };

        PerformanceMetrics {
            operations_per_second: ops_per_second,
            throughput_mbps,
            avg_latency_ms: avg_latency,
            p50_latency_ms: p50,
            p95_latency_ms: p95,
            p99_latency_ms: p99,
            error_rate,
            total_operations: operations,
            total_errors: errors,
        }
    }

    /// Print detailed performance metrics with additional analysis
    pub fn print_detailed_metrics(operation_name: &str, metrics: &PerformanceMetrics) {
        println!("=== {} Detailed Performance Analysis ===", operation_name);
        println!("Operations per second: {:.2}", metrics.operations_per_second);
        println!("Throughput: {:.2} MB/s", metrics.throughput_mbps);
        println!();
        println!("Latency Analysis:");
        println!("  Average: {:.3} ms", metrics.avg_latency_ms);
        println!("  P50 (Median): {:.3} ms", metrics.p50_latency_ms);
        println!("  P95: {:.3} ms", metrics.p95_latency_ms);
        println!("  P99: {:.3} ms", metrics.p99_latency_ms);
        println!();
        println!("Reliability:");
        println!("  Error rate: {:.2}%", metrics.error_rate * 100.0);
        println!("  Success rate: {:.2}%", (1.0 - metrics.error_rate) * 100.0);
        println!();
        println!("Volume:");
        println!("  Total operations: {}", metrics.total_operations);
        println!("  Total errors: {}", metrics.total_errors);
        println!();
        
        // Performance assessment
        if metrics.operations_per_second > 100000.0 {
            println!("Performance: EXCELLENT (>100K OPS)");
        } else if metrics.operations_per_second > 10000.0 {
            println!("Performance: GOOD (>10K OPS)");
        } else if metrics.operations_per_second > 1000.0 {
            println!("Performance: FAIR (>1K OPS)");
        } else {
            println!("Performance: POOR (<1K OPS)");
        }
        
        if metrics.p95_latency_ms < 1.0 {
            println!("Latency: EXCELLENT (P95 < 1ms)");
        } else if metrics.p95_latency_ms < 10.0 {
            println!("Latency: GOOD (P95 < 10ms)");
        } else if metrics.p95_latency_ms < 100.0 {
            println!("Latency: FAIR (P95 < 100ms)");
        } else {
            println!("Latency: POOR (P95 > 100ms)");
        }
        println!();
    }
}