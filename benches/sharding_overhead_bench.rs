use rkvs::{namespace::Namespace, NamespaceConfig, NamespaceSnapshot, StorageManager};
use serde::Serialize;
use std::sync::Arc;
use rand::prelude::*;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const ITERATIONS_PER_SCENARIO: usize = 5_000_000; // Use a large number of iterations for a stable average
const VALUE_SIZE: usize = 256;
const KEY_COUNT: usize = 100_000;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let storage = StorageManager::builder().build().await.unwrap();
        storage.initialize(None).await.unwrap();

        let mut all_results = Vec::new();

        println!("Preparing a {}-key namespace snapshot...", KEY_COUNT);
        let (snapshot, random_keys) = prepare_snapshot(&storage, KEY_COUNT).await;
        println!("Snapshot preparation complete.");

        let shard_counts = [1, 2, 4, 8, 16, 32, 64, 128];

        for &shard_count in &shard_counts {
            println!("\n==================================================");
            println!("  RUNNING SHARDING OVERHEAD BENCHMARK FOR {} SHARD(S)", shard_count);
            println!("==================================================");

            let ns = Arc::new(Namespace::from_snapshot(snapshot.clone()));
            if shard_count > 1 {
                ns.resize_shards(shard_count).await.unwrap();
            }

            let shard_key_counts = ns.get_shard_key_counts().await;
            println!("    - Shard Distribution: {:?}", shard_key_counts);

            let total_keys_from_shards: usize = shard_key_counts.iter().sum();
            let num_shards = shard_key_counts.len();
            let avg_deviation_percent = if num_shards > 0 && total_keys_from_shards > 0 {
                let perfect_dist = total_keys_from_shards as f64 / num_shards as f64;
                let total_deviation: f64 = shard_key_counts
                    .iter()
                    .map(|&count| (count as f64 - perfect_dist).abs())
                    .sum();
                let avg_deviation = total_deviation / num_shards as f64;
                (avg_deviation / perfect_dist) * 100.0
            } else {
                panic!(
                    "Benchmark misconfiguration: Cannot calculate deviation for a namespace with {} shards and {} total keys.",
                    num_shards, total_keys_from_shards
                )
            };

            let scenario_name = format!(
                "Sequential Get on {}k-key Namespace ({} Shard(s))",
                KEY_COUNT / 1_000,
                shard_count
            );

            let set_scenario_name = format!(
                "Sequential Set (Update) on {}k-key Namespace ({} Shard(s))",
                KEY_COUNT / 1_000,
                shard_count
            );

            // Run Get benchmark
            run_get_benchmark(
                &scenario_name,
                ns.clone(),
                &random_keys,
                ITERATIONS_PER_SCENARIO,
                avg_deviation_percent,
                &mut all_results,
            ).await;

            // Run Set (Update) benchmark
            // We re-create the namespace from the snapshot to ensure a clean state for the write benchmark,
            // although in this sequential case it's not strictly necessary. It's good practice.
            let ns_for_set = Arc::new(Namespace::from_snapshot(snapshot.clone()));
            if shard_count > 1 {
                ns_for_set.resize_shards(shard_count).await.unwrap();
            }
            run_set_update_benchmark(
                &set_scenario_name,
                ns_for_set,
                &random_keys,
                ITERATIONS_PER_SCENARIO,
                avg_deviation_percent,
                &mut all_results,
            ).await;
        }

        // Save results to JSON
        let output_dir = "assets/benchmarks";
        std::fs::create_dir_all(output_dir).unwrap();
        let results_json = serde_json::to_string_pretty(&all_results).unwrap();
        let output_path = std::path::Path::new(output_dir).join("sharding_overhead_bench_results.json");
        std::fs::write(&output_path, results_json).unwrap();
        println!("\n✅ Sharding overhead benchmark results saved to {}", output_path.display());
    });
}

/// Prepares a snapshot with a given number of keys.
async fn prepare_snapshot(storage: &Arc<StorageManager>, key_count: usize) -> (NamespaceSnapshot, Vec<String>) {
    let mut rng = StdRng::from_entropy();
    let random_keys: Vec<String> = (0..key_count)
        .map(|_| format!("key_{}", rng.r#gen::<u64>()))
        .collect();

    let temp_ns_name = "snapshot_builder_overhead";
    let ns_config = NamespaceConfig::default();
    ns_config.set_shard_count(1); // Always create with 1 shard, then resize
    storage.create_namespace(temp_ns_name, Some(ns_config)).await.unwrap();
    let ns = storage.namespace(temp_ns_name).await.unwrap();

    for key in &random_keys {
        let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
        ns.set(key, value).await.unwrap();
    }

    let snapshot = ns.create_snapshot().await;
    storage.delete_namespace(temp_ns_name).await.unwrap();

    (snapshot, random_keys)
}

/// Measures sequential `get` operations.
async fn run_get_benchmark(
    scenario_name: &str,
    ns: Arc<Namespace>,
    keys_to_use: &[String],
    iterations: usize,
    avg_deviation_percent: f64,
    results: &mut Vec<ScenarioResult>,
) {
    println!(
        "\n>>> Running Benchmark: '{}' ({} iterations) <<<",
        scenario_name, iterations
    );

    let mut durations = Vec::with_capacity(iterations);
    let mut key_iter = keys_to_use.iter().cycle();

    for _ in 0..iterations {
        let key = key_iter.next().unwrap();
        let start = Instant::now();
        if ns.get(key).await.is_none() {
            panic!("Get operation failed to find a key that should exist.");
        }
        let duration = start.elapsed();
        durations.push(duration);
    }
    print_stats(scenario_name, &mut durations, avg_deviation_percent, results);
}

/// Measures sequential `set` operations for existing keys (updates).
async fn run_set_update_benchmark(
    scenario_name: &str,
    ns: Arc<Namespace>,
    keys_to_use: &[String],
    iterations: usize,
    avg_deviation_percent: f64,
    results: &mut Vec<ScenarioResult>,
) {
    println!(
        "\n>>> Running Benchmark: '{}' ({} iterations) <<<",
        scenario_name, iterations
    );

    let mut durations = Vec::with_capacity(iterations);
    let mut key_iter = keys_to_use.iter().cycle();
    let mut rng = StdRng::from_entropy();

    for _ in 0..iterations {
        let key = key_iter.next().unwrap();
        let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();

        let start = Instant::now();
        let old_value = ns.set(key, value.clone()).await.unwrap();
        if old_value.is_none() {
            panic!("Set (update) operation did not return an old value for an existing key.");
        }
        let duration = start.elapsed();

        durations.push(duration);
    }

    print_stats(scenario_name, &mut durations, avg_deviation_percent, results);
}

/// Calculates and prints latency statistics for a set of measurements.
fn print_stats(
    scenario_name: &str,
    durations: &mut Vec<Duration>,
    avg_deviation_percent: f64,
    results: &mut Vec<ScenarioResult>) {
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
    println!("------------------------------------");

    results.push(ScenarioResult {
        scenario_name: scenario_name.to_string(),
        min_latency_ns: min_duration.as_nanos(),
        avg_latency_ns: avg_duration.as_nanos(),
        p99_latency_ns: p99_duration.as_nanos(),
        max_latency_ns: max_duration.as_nanos(),
        avg_deviation_percent,
    });
}

#[derive(Serialize)]
struct ScenarioResult {
    scenario_name: String,
    min_latency_ns: u128,
    avg_latency_ns: u128,
    p99_latency_ns: u128,
    max_latency_ns: u128,
    avg_deviation_percent: f64,
}
