use futures::future::join_all;
use rand::prelude::*;
use rkvs::{NamespaceConfig, NamespaceSnapshot, StorageManager, namespace::Namespace};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const TOTAL_OPERATIONS: usize = 5_000_000;
const VALUE_SIZE: usize = 256; // 256 bytes

fn main() {
    // 1. Setup Tokio runtime
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        // 2. Setup StorageManager
        let storage = StorageManager::builder().build().await.unwrap();
        storage.initialize(None).await.unwrap();

        let mut all_results = Vec::new();

        // Define the parameters for the benchmark runs
        let shard_counts = [1, 5, 10, 15];
        let concurrency_levels = [8, 16, 32, 64, 128];
        let workloads = [
            ("80-20 Read-Heavy", 0.8),
            ("50-50 Balanced", 0.5),
            ("20-80 Write-Heavy", 0.2),
        ];

        // Create a single 100k key snapshot to be reused
        println!("Preparing a 100k-key namespace snapshot...");
        let (snapshot_100k, random_keys) = prepare_snapshot(&storage, 100_000).await;
        println!("Snapshot preparation complete.");

        for &shard_count in &shard_counts {
            println!("\n==================================================");
            println!("  RUNNING BENCHMARKS FOR {} SHARD(S)", shard_count);
            println!("==================================================");

            // Create a namespace instance with the correct sharding for this run
            let ns = Arc::new(Namespace::from_snapshot(snapshot_100k.clone()));
            if shard_count > 1 {
                ns.resize_shards(shard_count).await.unwrap();
            }
            for &concurrency in &concurrency_levels {
                for &(workload_name, read_ratio) in &workloads {
                    let scenario_name = format!(
                        "{} Workload @ {} Concurrency ({} Shard(s))",
                        workload_name, concurrency, shard_count
                    );

                    run_mixed_workload_benchmark(
                        &scenario_name,
                        ns.clone(),
                        &random_keys,
                        TOTAL_OPERATIONS,
                        concurrency,
                        read_ratio,
                        &mut all_results,
                    )
                    .await;
                }
            }
        }

        // Save results to JSON
        let output_dir = "assets/benchmarks";
        std::fs::create_dir_all(output_dir).unwrap();
        let results_json = serde_json::to_string_pretty(&all_results).unwrap();
        let output_path = std::path::Path::new(output_dir).join("concurrent_bench_results.json");
        std::fs::write(&output_path, results_json).unwrap();
        println!(
            "\n✅ Concurrent benchmark results saved to {}",
            output_path.display()
        );
    });
}

/// Prepares a snapshot with a given number of keys.
async fn prepare_snapshot(
    storage: &Arc<StorageManager>,
    key_count: usize,
) -> (NamespaceSnapshot, Vec<String>) {
    let mut rng = StdRng::from_entropy();
    let random_keys: Vec<String> = (0..key_count)
        .map(|_| format!("key_{}", rng.r#gen::<u64>()))
        .collect();

    let temp_ns_name = "snapshot_builder_concurrent";
    let ns_config = NamespaceConfig::default();
    // Always create the base snapshot with a single shard for consistency.
    ns_config.set_shard_count(1);
    storage
        .create_namespace(temp_ns_name, Some(ns_config))
        .await
        .unwrap();
    let ns = storage.namespace(temp_ns_name).await.unwrap();

    for key in &random_keys {
        let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
        ns.set(key, value).await.unwrap();
    }

    let snapshot = ns.create_snapshot().await;
    storage.delete_namespace(temp_ns_name).await.unwrap();

    (snapshot, random_keys)
}

/// Runs a benchmark for a mixed read/write workload.
async fn run_mixed_workload_benchmark(
    scenario_name: &str,
    ns: Arc<Namespace>,
    keys_to_use: &[String],
    total_iterations: usize,
    concurrency_level: usize,
    read_ratio: f64,
    results: &mut Vec<ScenarioResult>,
) {
    println!("\n>>> Running Benchmark: '{}'", scenario_name);

    let mut all_durations = Vec::with_capacity(total_iterations);
    let num_read_ops = (concurrency_level as f64 * read_ratio).round() as usize;
    let num_write_ops = concurrency_level - num_read_ops;

    let total_batches = total_iterations / concurrency_level;

    let overall_start_time = Instant::now();

    for _ in 0..total_batches {
        let mut handles = Vec::with_capacity(concurrency_level);

        // Spawn read tasks
        for _ in 0..num_read_ops {
            // Generate the random key on the current thread before spawning.
            let key = keys_to_use[thread_rng().r#gen_range(0..keys_to_use.len())].clone();
            let ns_clone = ns.clone();
            handles.push(tokio::spawn(async move {
                let start = Instant::now();
                let _ = ns_clone.get(&key).await;
                start.elapsed()
            }));
        }

        // Spawn write tasks (updates)
        for _ in 0..num_write_ops {
            // Generate the random key on the current thread before spawning.
            let mut rng = thread_rng();
            let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
            let key = keys_to_use[rng.r#gen_range(0..keys_to_use.len())].clone();
            let ns_clone = ns.clone();
            handles.push(tokio::spawn(async move {
                // Create a new RNG that is local to this new task.
                let start = Instant::now();
                let _ = ns_clone.set(&key, value).await;
                start.elapsed()
            }));
        }

        let durations: Vec<Duration> = join_all(handles)
            .await
            .into_iter()
            .map(|res| res.unwrap())
            .collect();
        all_durations.extend(durations);
    }

    let total_duration_for_ops = overall_start_time.elapsed();
    let ops_per_sec = total_iterations as f64 / total_duration_for_ops.as_secs_f64();

    let mut durations = all_durations;
    print_stats(scenario_name, &mut durations, ops_per_sec, results);
}

/// Calculates and prints latency statistics for a set of measurements.
fn print_stats(
    scenario_name: &str,
    durations: &mut Vec<Duration>,
    ops_per_sec: f64,
    results: &mut Vec<ScenarioResult>,
) {
    if durations.is_empty() {
        println!("No measurements recorded for '{}'.", scenario_name);
        return;
    }

    durations.sort();

    let total_duration: Duration = durations.iter().sum();
    let avg_duration = total_duration / durations.len() as u32;
    let min_duration = durations.first().unwrap();
    let max_duration = durations.last().unwrap();
    let p99_index = (durations.len() as f64 * 0.99) as usize;
    let p99_duration = durations[p99_index.min(durations.len() - 1)];

    println!("--- Results for '{}' ---", scenario_name);
    println!("Min Latency:      {:?}", min_duration);
    println!("Avg Latency:      {:?}", avg_duration);
    println!("p99 Latency:      {:?}", p99_duration);
    println!("Max Latency:      {:?}", max_duration);
    println!("Ops/Sec:          {:.2}", ops_per_sec);
    println!("------------------------------------");

    // Store results for JSON output
    results.push(ScenarioResult {
        scenario_name: scenario_name.to_string(),
        min_latency_ns: min_duration.as_nanos(),
        avg_latency_ns: avg_duration.as_nanos(),
        p99_latency_ns: p99_duration.as_nanos(),
        max_latency_ns: max_duration.as_nanos(),
        ops_per_sec,
    });
}

#[derive(Serialize)]
struct ScenarioResult {
    scenario_name: String,
    min_latency_ns: u128,
    avg_latency_ns: u128,
    p99_latency_ns: u128,
    max_latency_ns: u128,
    ops_per_sec: f64,
}
