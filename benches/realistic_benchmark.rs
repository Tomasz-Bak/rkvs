use std::time::Duration;
use std::sync::atomic::{AtomicUsize, Ordering};
use clap::Parser;
mod benchmark_helpers;
use benchmark_helpers::{BenchmarkHelpers, PerformanceMetrics};

/// RKVS Realistic Performance Benchmark
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Test duration in milliseconds
    #[arg(long, default_value_t = 1000)]
    test_time: u64,
    
    /// Warmup duration in milliseconds
    #[arg(long, default_value_t = 200)]
    warmup_time: u64,
    
    /// Run only specific test (single, mixed, concurrency, data_sizes, consume, all)
    #[arg(short, long, default_value = "all")]
    test: String,
    
    /// Number of concurrent threads for concurrency test
    #[arg(short, long, default_value_t = 10)]
    concurrency: usize,
    
    /// Number of items per batch operation (get_multiple, set_multiple, delete_multiple, consume_multiple)
    #[arg(long, default_value_t = 5)]
    batch_size: usize,
    
    /// Verbose output
    #[arg(short, long)]
    verbose: bool,
}

/// Realistic benchmark with detailed performance analysis
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    println!("RKVS Realistic Performance Analysis");
    println!("===================================\n");
    println!("This benchmark provides detailed performance metrics with");
    println!("anti-optimization measures for accurate measurements.\n");
    
    if args.verbose {
        println!("Configuration:");
        println!("  Test duration: {} ms", args.test_time);
        println!("  Warmup duration: {} ms", args.warmup_time);
        println!("  Test type: {}", args.test);
        println!("  Concurrency: {} threads", args.concurrency);
        println!("  Batch size: {} items", args.batch_size);
        println!();
    }

    let test_duration = Duration::from_millis(args.test_time);
    let warmup_duration = Duration::from_millis(args.warmup_time);

    // Create a test storage
    let storage = BenchmarkHelpers::create_test_storage();
    storage.initialize().await?;
    
    // Test 1: Single operation benchmark
    if args.test == "all" || args.test == "single" {
        println!("Test 1: Single Get Operation Analysis");
        println!("-------------------------------------");
        
        let ns_hash = storage.create_namespace("single_test", None).await?;
        let namespace = storage.namespace(ns_hash).await?;
        
        // Setup data with varying content to prevent optimization
        let mut data = vec![0u8; 1024];
        for i in 0..data.len() {
            data[i] = (i % 256) as u8;
        }
        namespace.set("test_key".to_string(), data.clone()).await?;
        
        // Create operation that forces actual work
        let namespace_clone = namespace.clone();
        let operation = move || {
            let ns = namespace_clone.clone();
            async move {
                let result = ns.get("test_key").await;
                match result {
                    Some(value) => {
                        // Force computation on the value
                        let checksum = value.iter().fold(0u64, |acc, &b| acc.wrapping_add(b as u64));
                        // Use atomic to prevent optimization
                        let _ = AtomicUsize::new(checksum as usize).load(Ordering::Relaxed);
                    }
                    None => return Err("Key not found".into()),
                }
                Ok(())
            }
        };
        
        let metrics = BenchmarkHelpers::run_realistic_benchmark(
            operation,
            test_duration,
            warmup_duration,
        ).await;
        
        BenchmarkHelpers::print_detailed_metrics("Single Get Operation", &metrics);
    }
    
    // Test 2: Mixed workload benchmark
    if args.test == "all" || args.test == "mixed" {
        println!("Test 2: Mixed Read/Write Workload Analysis");
        println!("------------------------------------------");
        
        let ns_hash2 = storage.create_namespace("mixed_test", None).await?;
        let namespace2 = storage.namespace(ns_hash2).await?;
        
        // Setup initial data
        for i in 0..10 {
            let key = format!("key_{}", i);
            let mut value = vec![0u8; 512];
            for j in 0..value.len() {
                value[j] = ((i * j) % 256) as u8;
            }
            namespace2.set(key, value).await?;
        }
        
        // Create mixed operation
        let namespace_clone2 = namespace2.clone();
        let operation = move || {
            let ns = namespace_clone2.clone();
            async move {
                for i in 0..5 {
                    let key = format!("key_{}", i);
                    if i % 2 == 0 {
                        // Read operation
                        let result = ns.get(&key).await;
                        if let Some(value) = result {
                            let _ = value.len();
                        }
                    } else {
                        // Write operation
                        let mut value = vec![0u8; 512];
                        for j in 0..value.len() {
                            value[j] = ((i * j + 1000) % 256) as u8;
                        }
                        ns.set(key, value).await?;
                    }
                }
                Ok(())
            }
        };
        
        let metrics = BenchmarkHelpers::run_realistic_benchmark(
            operation,
            test_duration,
            warmup_duration,
        ).await;
        
        BenchmarkHelpers::print_detailed_metrics("Mixed Read/Write Workload", &metrics);
    }
    
    // Test 3: High concurrency benchmark
    if args.test == "all" || args.test == "concurrency" {
        println!("Test 3: High Concurrency Analysis");
        println!("---------------------------------");
        
        let ns_hash3 = storage.create_namespace("concurrency_test", None).await?;
        let namespace3 = storage.namespace(ns_hash3).await?;
        
        // Setup test data
        for i in 0..20 {
            let key = format!("concurrent_key_{}", i);
            let mut value = vec![0u8; 256];
            for j in 0..value.len() {
                value[j] = ((i * j) % 256) as u8;
            }
            namespace3.set(key, value).await?;
        }
        
        // Create concurrent operation
        let namespace_clone3 = namespace3.clone();
        let operation = move || {
            let ns = namespace_clone3.clone();
            async move {
                for i in 0..3 {
                    let key = format!("concurrent_key_{}", i);
                    let result = ns.get(&key).await;
                    if let Some(value) = result {
                        let _ = value.len();
                    }
                }
                Ok(())
            }
        };
        
        let metrics = BenchmarkHelpers::run_concurrent_benchmark(
            operation,
            args.concurrency,
            test_duration,
        ).await;
        
        BenchmarkHelpers::print_detailed_metrics(&format!("High Concurrency ({} threads)", args.concurrency), &metrics);
    }
    
    // Test 4: Different data sizes impact
    if args.test == "all" || args.test == "data_sizes" {
        println!("Test 4: Data Size Impact Analysis");
        println!("---------------------------------");
        
        let data_sizes = vec![64, 256, 1024, 4096, 16384]; // 64B to 16KB
        
        for size in data_sizes {
            let ns_hash = storage.create_namespace(&format!("size_test_{}", size), None).await?;
            let namespace = storage.namespace(ns_hash).await?;
            
            // Create data with varying content
            let mut data = vec![0u8; size];
            for i in 0..data.len() {
                data[i] = (i % 256) as u8;
            }
            namespace.set("size_test_key".to_string(), data.clone()).await?;
            
            // Create operation
            let namespace_clone = namespace.clone();
            let operation = move || {
                let ns = namespace_clone.clone();
                async move {
                    let result = ns.get("size_test_key").await;
                    if let Some(value) = result {
                        let _ = value.len();
                    }
                    Ok(())
                }
            };
            
            let metrics = BenchmarkHelpers::run_realistic_benchmark(
                operation,
                test_duration,
                warmup_duration,
            ).await;
            
            println!("Data size: {} bytes", size);
            println!("  OPS: {:.0}", metrics.operations_per_second);
            println!("  Avg latency: {:.3} ms", metrics.avg_latency_ms);
            println!("  P95 latency: {:.3} ms", metrics.p95_latency_ms);
            println!();
        }
    }
    
    // Test 5: Bulk operations benchmark
    if args.test == "all" || args.test == "bulk" {
        println!("Test 5: Bulk Operations Analysis ({} items per operation)", args.batch_size);
        println!("--------------------------------------------------------");
        
        let ns_hash = storage.create_namespace("bulk_test", None).await?;
        let namespace = storage.namespace(ns_hash).await?;
        
        // Test get_multiple operations
        println!("Test 5a: Get Multiple Operations Analysis");
        println!("----------------------------------------");
        
        // Setup fresh data for get_multiple test
        let total_keys = args.batch_size * 10; // Ensure we have enough keys
        for i in 0..total_keys {
            let key = format!("bulk_get_key_{}", i);
            let mut data = vec![0u8; 512];
            for j in 0..data.len() {
                data[j] = ((i * j) % 256) as u8;
            }
            namespace.set(key, data).await?;
        }
        
        let namespace_clone = namespace.clone();
        let batch_size = args.batch_size;
        let get_multiple_operation = move || {
            let ns = namespace_clone.clone();
            async move {
                // Get batch_size keys at a time
                let keys = (0..batch_size)
                    .map(|i| format!("bulk_get_key_{}", i))
                    .collect();
                let result = ns.get_multiple(keys).await;
                if let Some(values) = result.data {
                    let _ = values.len();
                }
                Ok(())
            }
        };
        
        let metrics = BenchmarkHelpers::run_realistic_benchmark(
            get_multiple_operation,
            test_duration,
            warmup_duration,
        ).await;
        
        BenchmarkHelpers::print_detailed_metrics(&format!("Get Multiple Operations ({} items)", args.batch_size), &metrics);
        
        // Test set_multiple operations
        println!("Test 5b: Set Multiple Operations Analysis");
        println!("----------------------------------------");
        
        let namespace_clone2 = namespace.clone();
        let batch_size = args.batch_size;
        let set_multiple_operation = move || {
            let ns = namespace_clone2.clone();
            async move {
                // Set batch_size key-value pairs at a time
                let items = (0..batch_size)
                    .map(|i| {
                        let key = format!("bulk_set_key_{}", i);
                        let mut data = vec![0u8; 512];
                        for j in 0..data.len() {
                            data[j] = ((i * j) % 256) as u8;
                        }
                        (key, data)
                    })
                    .collect();
                let result = ns.set_multiple(items).await;
                if result.is_success() {
                    let _ = result.total_processed;
                }
                Ok(())
            }
        };
        
        let metrics = BenchmarkHelpers::run_realistic_benchmark(
            set_multiple_operation,
            test_duration,
            warmup_duration,
        ).await;
        
        BenchmarkHelpers::print_detailed_metrics(&format!("Set Multiple Operations ({} items)", args.batch_size), &metrics);
        
        // Test delete_multiple operations
        println!("Test 5c: Delete Multiple Operations Analysis");
        println!("-------------------------------------------");
        
        // Setup fresh data for delete_multiple test
        for i in 0..total_keys {
            let key = format!("bulk_delete_key_{}", i);
            let mut data = vec![0u8; 512];
            for j in 0..data.len() {
                data[j] = ((i * j) % 256) as u8;
            }
            namespace.set(key, data).await?;
        }
        
        let namespace_clone3 = namespace.clone();
        let batch_size = args.batch_size;
        let delete_multiple_operation = move || {
            let ns = namespace_clone3.clone();
            async move {
                // Delete batch_size keys at a time
                let keys = (0..batch_size)
                    .map(|i| format!("bulk_delete_key_{}", i))
                    .collect();
                let result = ns.delete_multiple(keys).await;
                if result.is_success() {
                    let _ = result.total_processed;
                }
                Ok(())
            }
        };
        
        let metrics = BenchmarkHelpers::run_realistic_benchmark(
            delete_multiple_operation,
            test_duration,
            warmup_duration,
        ).await;
        
        BenchmarkHelpers::print_detailed_metrics(&format!("Delete Multiple Operations ({} items)", args.batch_size), &metrics);
        
        // Test consume_multiple operations
        println!("Test 5d: Consume Multiple Operations Analysis");
        println!("---------------------------------------------");
        
        // Setup fresh data for consume_multiple test
        for i in 0..total_keys {
            let key = format!("bulk_consume_key_{}", i);
            let mut data = vec![0u8; 512];
            for j in 0..data.len() {
                data[j] = ((i * j) % 256) as u8;
            }
            namespace.set(key, data).await?;
        }
        
        let namespace_clone4 = namespace.clone();
        let batch_size = args.batch_size;
        let consume_multiple_operation = move || {
            let ns = namespace_clone4.clone();
            async move {
                // Consume batch_size keys at a time
                let keys = (0..batch_size)
                    .map(|i| format!("bulk_consume_key_{}", i))
                    .collect();
                let result = ns.consume_multiple(keys).await;
                if let Some(values) = result.data {
                    let _ = values.len();
                }
                Ok(())
            }
        };
        
        let metrics = BenchmarkHelpers::run_realistic_benchmark(
            consume_multiple_operation,
            test_duration,
            warmup_duration,
        ).await;
        
        BenchmarkHelpers::print_detailed_metrics(&format!("Consume Multiple Operations ({} items)", args.batch_size), &metrics);
    }
    
    println!("Benchmark completed successfully!");
    println!("\nNote: These results include:");
    println!("- Proper warmup periods");
    println!("- Anti-optimization measures");
    println!("- Actual data processing");
    println!("- Realistic timing measurements");
    println!("- Detailed performance analysis");
    
    Ok(())
}
