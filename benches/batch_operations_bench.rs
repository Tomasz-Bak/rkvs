use rkvs::{namespace::Namespace, BatchMode, NamespaceConfig, NamespaceSnapshot, StorageManager};
use serde::Serialize;
use std::sync::Arc;
use rand::prelude::*;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const ITERATIONS_PER_SCENARIO: usize = 100_000; // Number of batch operations to run
const VALUE_SIZE: usize = 256;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let storage = StorageManager::builder().build().await.unwrap();
        storage.initialize(None).await.unwrap();

        let mut all_results = Vec::new();

        run_all_scenarios_for_config(&storage, &mut all_results).await;

        let output_dir = "assets/benchmarks";
        std::fs::create_dir_all(output_dir).unwrap();
        let results_json = serde_json::to_string_pretty(&all_results).unwrap();
        let output_path = std::path::Path::new(output_dir).join("batch_operations_bench_results.json");
        std::fs::write(&output_path, results_json).unwrap();
        println!("\n✅ Batch operations benchmark results saved to {}", output_path.display());
    });
}

/// Helper to run the full set of benchmarks for a given shard configuration.
async fn run_all_scenarios_for_config(
    storage: &Arc<StorageManager>,
    results: &mut Vec<ScenarioResult>) {
    // Prepare snapshots and keys
    println!("Preparing random keys and namespace snapshots for different data scales...");
    let mut rng = StdRng::from_entropy();
    let random_keys: Vec<String> = (0..100_000)
        .map(|_| format!("key_{}", rng.r#gen::<u64>()))
        .collect();

    let temp_ns_name = "snapshot_builder_batch_ops";
    let ns_config = NamespaceConfig::default();
    ns_config.set_shard_count(1);
    storage.create_namespace(temp_ns_name, Some(ns_config)).await.unwrap();
    let ns = storage.namespace(temp_ns_name).await.unwrap();

    for i in 0..100_000 {
        let key = &random_keys[i];
        let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
        ns.set(&key, value).await.unwrap();
    }
    let snapshot_100k = ns.create_snapshot().await;
    println!("  - Snapshot created for 100,000 keys.");
    storage.delete_namespace(temp_ns_name).await.unwrap();
    println!("Snapshot preparation complete.");

    let modes = [BatchMode::AllOrNothing, BatchMode::BestEffort];
    let batch_sizes = [50, 100, 200];

    for &batch_size in &batch_sizes {
        println!("\n==================================================");
        println!("  RUNNING BATCH BENCHMARKS (1 SHARD, BATCH SIZE {})", batch_size);
        println!("==================================================");

        for &mode in &modes {
            let mode_str = if mode == BatchMode::AllOrNothing { "AllOrNothing" } else { "BestEffort" };

            run_set_multiple_benchmark(
                &format!("Batch Set (Size {}) ({}) on 100k-key Namespace", batch_size, mode_str),
                snapshot_100k.clone(),
                ITERATIONS_PER_SCENARIO,
                batch_size,
                mode,
                results,
            ).await;

            run_get_multiple_benchmark(
                &format!("Batch Get (Size {}) ({}) on 100k-key Namespace", batch_size, mode_str),
                snapshot_100k.clone(),
                &random_keys,
                ITERATIONS_PER_SCENARIO,
                batch_size,
                mode,
                results,
            ).await;

            run_delete_multiple_benchmark(
                &format!("Batch Delete (Size {}) ({}) on 100k-key Namespace", batch_size, mode_str),
                snapshot_100k.clone(),
                &random_keys,
                ITERATIONS_PER_SCENARIO,
                batch_size,
                mode,
                results,
            ).await;
        }
    }
}

// --- Benchmark Implementations ---

/// Measures batch `set_multiple` operations.
async fn run_set_multiple_benchmark(
    scenario_name: &str,
    template_snapshot: NamespaceSnapshot,
    iterations: usize,
    batch_size: usize,
    mode: BatchMode,
    results: &mut Vec<ScenarioResult>,
) {
    println!("\n>>> Running Benchmark: '{}'", scenario_name);
    let ns = Arc::new(Namespace::from_snapshot(template_snapshot));
    let mut durations = Vec::with_capacity(iterations);
    let mut rng = StdRng::from_entropy();

    for _ in 0..iterations {
        let batch: Vec<(String, Vec<u8>)> = (0..batch_size)
            .map(|_| {
                let key = format!("new_key_{}", rng.r#gen::<u64>());
                let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
                (key, value)
            })
            .collect();

        let start = Instant::now();
        let result = ns.set_multiple(batch.clone(), mode).await;
        let duration = start.elapsed();

        if result.errors.is_some() {
            panic!("Batch set operation failed: {:?}", result.errors);
        }

        durations.push(duration);

        // Cleanup outside of timing
        let keys_to_delete: Vec<String> = batch.into_iter().map(|(k, _)| k).collect();
        ns.delete_multiple(keys_to_delete, BatchMode::BestEffort).await;
    }

    print_stats(scenario_name, &mut durations, batch_size, results);
}

/// Measures batch `get_multiple` operations.
async fn run_get_multiple_benchmark(
    scenario_name: &str,
    template_snapshot: NamespaceSnapshot,
    keys_to_use: &[String],
    iterations: usize,
    batch_size: usize,
    mode: BatchMode,
    results: &mut Vec<ScenarioResult>,
) {
    println!("\n>>> Running Benchmark: '{}'", scenario_name);
    let ns = Arc::new(Namespace::from_snapshot(template_snapshot));
    let mut durations = Vec::with_capacity(iterations);
    let mut key_idx = 0;

    for _ in 0..iterations {
        let batch: Vec<String> = (0..batch_size)
            .map(|_| {
                let key = keys_to_use[key_idx % keys_to_use.len()].clone();
                key_idx += 1;
                key
            })
            .collect();

        let start = Instant::now();
        let result = ns.get_multiple(batch, mode).await;
        let duration = start.elapsed();

        if result.errors.is_some() {
            panic!("Batch get operation failed: {:?}", result.errors);
        }

        durations.push(duration);
    }

    print_stats(scenario_name, &mut durations, batch_size, results);
}

/// Measures batch `delete_multiple` operations.
async fn run_delete_multiple_benchmark(
    scenario_name: &str,
    template_snapshot: NamespaceSnapshot,
    keys_to_use: &[String],
    iterations: usize,
    batch_size: usize,
    mode: BatchMode,
    results: &mut Vec<ScenarioResult>,
) {
    println!("\n>>> Running Benchmark: '{}'", scenario_name);
    let ns = Arc::new(Namespace::from_snapshot(template_snapshot));
    let mut durations = Vec::with_capacity(iterations);
    let mut key_idx = 0;

    for _ in 0..iterations {
        let batch: Vec<String> = (0..batch_size)
            .map(|_| {
                let key = keys_to_use[key_idx % keys_to_use.len()].clone();
                key_idx += 1;
                key
            })
            .collect();

        let start = Instant::now();
        let result = ns.delete_multiple(batch.clone(), mode).await;
        let duration = start.elapsed();

        if result.errors.is_some() {
            panic!("Batch delete operation failed: {:?}", result.errors);
        }

        durations.push(duration);

        // Restore the deleted keys to maintain DB size
        let mut rng = StdRng::from_entropy();
        let items_to_restore: Vec<(String, Vec<u8>)> = batch.into_iter()
            .map(|key| {
                let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
                (key, value)
            })
            .collect();
        ns.set_multiple(items_to_restore, BatchMode::BestEffort).await;
    }

    print_stats(scenario_name, &mut durations, batch_size, results);
}

/// Calculates and prints latency statistics for a set of measurements.
fn print_stats(
    scenario_name: &str,
    durations: &mut Vec<Duration>,
    batch_size: usize,
    results: &mut Vec<ScenarioResult>,
) {
    if durations.is_empty() {
        println!("No measurements recorded for '{}'.", scenario_name);
        return;
    }

    durations.sort();

    // Per-operation (i.e., per-batch) stats
    let total_op_duration: Duration = durations.iter().sum();
    let avg_op_duration = total_op_duration / durations.len() as u32;
    let min_op_duration = *durations.first().unwrap();
    let max_op_duration = *durations.last().unwrap();
    let p99_index = (durations.len() as f64 * 0.99) as usize;
    let p99_op_duration = durations[p99_index.min(durations.len() - 1)];

    // Per-item stats (derived from per-op stats)
    let avg_item_duration = avg_op_duration / batch_size as u32;
    let min_item_duration = min_op_duration / batch_size as u32;
    let max_item_duration = max_op_duration / batch_size as u32;
    let p99_item_duration = p99_op_duration / batch_size as u32;

    println!("--- Results for '{}' ---", scenario_name);
    println!("Min Latency (per item):      {:?}", min_item_duration);
    println!("Avg Latency (per item):      {:?}", avg_item_duration);
    println!("p99 Latency (per item):      {:?}", p99_item_duration);
    println!("Max Latency (per item):      {:?}", max_item_duration);
    println!("Avg Latency (per op):        {:?}", avg_op_duration);
    println!("------------------------------------");

    results.push(ScenarioResult {
        scenario_name: scenario_name.to_string(),
        min_latency_per_item_ns: min_item_duration.as_nanos(),
        avg_latency_per_item_ns: avg_item_duration.as_nanos(),
        p99_latency_per_item_ns: p99_item_duration.as_nanos(),
        max_latency_per_item_ns: max_item_duration.as_nanos(),
        min_latency_per_op_ns: min_op_duration.as_nanos(),
        avg_latency_per_op_ns: avg_op_duration.as_nanos(),
        p99_latency_per_op_ns: p99_op_duration.as_nanos(),
        max_latency_per_op_ns: max_op_duration.as_nanos(),
    });
}

#[derive(Serialize)]
struct ScenarioResult {
    scenario_name: String,
    min_latency_per_item_ns: u128,
    avg_latency_per_item_ns: u128,
    p99_latency_per_item_ns: u128,
    max_latency_per_item_ns: u128,
    min_latency_per_op_ns: u128,
    avg_latency_per_op_ns: u128,
    p99_latency_per_op_ns: u128,
    max_latency_per_op_ns: u128,
}