use rand::prelude::*;
use rkvs::{NamespaceConfig, NamespaceSnapshot, StorageManager, namespace::Namespace};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const ITERATIONS_PER_SCENARIO: usize = 5_000_000;
const VALUE_SIZE: usize = 256; // 256 bytes

fn main() {
    // 1. Setup Tokio runtime
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        // 2. Setup StorageManager
        let storage = StorageManager::builder().build().await.unwrap();
        storage.initialize(None).await.unwrap();

        let mut all_results = Vec::new();

        // Run benchmarks for a single-shard configuration
        run_all_scenarios_for_config(&storage, &mut all_results).await;

        // Save results to JSON
        let output_dir = "assets/benchmarks";
        std::fs::create_dir_all(output_dir).unwrap();
        let results_json = serde_json::to_string_pretty(&all_results).unwrap();
        let output_path = std::path::Path::new(output_dir).join("operations_bench_results.json");
        std::fs::write(&output_path, results_json).unwrap();
        println!(
            "\n✅ Sequential operations benchmark results saved to {}",
            output_path.display()
        );
    });
}

/// Helper to run the full set of benchmarks for a given shard configuration.
async fn run_all_scenarios_for_config(
    storage: &Arc<StorageManager>,
    results: &mut Vec<ScenarioResult>,
) {
    println!("\n==================================================");
    println!("  RUNNING SEQUENTIAL BENCHMARKS (1 SHARD)");
    println!("==================================================");

    {
        // 3. Create snapshots for different data sizes
        println!("Preparing random keys and namespace snapshots for different data scales...");
        let mut rng = StdRng::from_entropy();
        let random_keys: Vec<String> = (0..1_000_000)
            .map(|_| format!("key_{}", rng.r#gen::<u64>()))
            .collect();
        println!("  - Generated 1,000,000 random keys.");

        // Create a temporary namespace to build the snapshots
        let temp_ns_name = "snapshot_builder_ops";
        let ns_config = NamespaceConfig::default();
        ns_config.set_shard_count(1); // Hardcode to 1 shard for this benchmark
        storage
            .create_namespace(temp_ns_name, Some(ns_config))
            .await
            .unwrap();
        let ns = storage.namespace(temp_ns_name).await.unwrap();

        // Snapshot 1: 1k keys
        for i in 0..1_000 {
            let key = &random_keys[i];
            let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
            ns.set(&key, value).await.unwrap();
        }
        let snapshot_1k = ns.create_snapshot().await;
        println!("  - Snapshot created for 1,000 keys.");
        let counts_1k = ns.get_shard_key_counts().await;
        println!("    - Shard Distribution (1k keys): {:?}", counts_1k);

        // Snapshot 2: 10k keys
        for i in 1_000..10_000 {
            let key = &random_keys[i];
            let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
            ns.set(&key, value).await.unwrap();
        }
        let snapshot_10k = ns.create_snapshot().await;
        println!("  - Snapshot created for 10,000 keys.");
        let counts_10k = ns.get_shard_key_counts().await;
        println!("    - Shard Distribution (10k keys): {:?}", counts_10k);

        // Snapshot 3: 100k keys
        for i in 10_000..100_000 {
            let key = &random_keys[i];
            let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
            ns.set(&key, value).await.unwrap();
        }
        let snapshot_100k = ns.create_snapshot().await;
        println!("  - Snapshot created for 100,000 keys.");
        let counts_100k = ns.get_shard_key_counts().await;
        println!("    - Shard Distribution (100k keys): {:?}", counts_100k);

        // Snapshot 4: 1M keys
        for i in 100_000..1_000_000 {
            let key = &random_keys[i];
            let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
            ns.set(&key, value).await.unwrap();
        }
        let snapshot_1m = ns.create_snapshot().await;
        println!("  - Snapshot created for 1,000,000 keys.");
        let counts_1m = ns.get_shard_key_counts().await;
        println!("    - Shard Distribution (1M keys): {:?}", counts_1m);

        // Clean up the temporary namespace
        storage.delete_namespace(temp_ns_name).await.unwrap();
        println!("Snapshot preparation complete.");

        // 4. Define and run benchmarks for each scenario
        struct Bench<'a> {
            name: &'a str,
            snapshot: NamespaceSnapshot,
            keys: &'a [String],
        }

        let scenarios = [
            Bench {
                name: "1k-key Namespace",
                snapshot: snapshot_1k,
                keys: &random_keys[0..1_000],
            },
            Bench {
                name: "10k-key Namespace",
                snapshot: snapshot_10k,
                keys: &random_keys[0..10_000],
            },
            Bench {
                name: "100k-key Namespace",
                snapshot: snapshot_100k,
                keys: &random_keys[0..100_000],
            },
            Bench {
                name: "1M-key Namespace",
                snapshot: snapshot_1m,
                keys: &random_keys,
            },
        ];

        for bench in &scenarios {
            run_set_insert_benchmark_sequential(
                &format!("Sequential Set (Insert) on {}", bench.name),
                bench.snapshot.clone(),
                ITERATIONS_PER_SCENARIO,
                results,
            )
            .await;

            run_set_update_benchmark_sequential(
                &format!("Sequential Set (Update) on {}", bench.name),
                bench.snapshot.clone(),
                bench.keys,
                ITERATIONS_PER_SCENARIO,
                results,
            )
            .await;

            run_update_benchmark_sequential(
                &format!("Sequential Update on {}", bench.name),
                bench.snapshot.clone(),
                bench.keys,
                ITERATIONS_PER_SCENARIO,
                results,
            )
            .await;

            run_get_benchmark_sequential(
                &format!("Sequential Get on {}", bench.name),
                bench.snapshot.clone(),
                bench.keys,
                ITERATIONS_PER_SCENARIO,
                results,
            )
            .await;

            run_exists_benchmark_sequential(
                &format!("Sequential Exists on {}", bench.name),
                bench.snapshot.clone(),
                bench.keys,
                ITERATIONS_PER_SCENARIO,
                results,
            )
            .await;

            run_delete_benchmark_sequential(
                &format!("Sequential Delete on {}", bench.name),
                bench.snapshot.clone(),
                bench.keys,
                ITERATIONS_PER_SCENARIO,
                results,
            )
            .await;

            run_consume_benchmark_sequential(
                &format!("Sequential Consume on {}", bench.name),
                bench.snapshot.clone(),
                bench.keys,
                ITERATIONS_PER_SCENARIO,
                results,
            )
            .await;
        }
    }
}

// --- Benchmark Implementations ---

/// Measures sequential `set` operations for **new keys (inserts)**.
async fn run_set_insert_benchmark_sequential(
    scenario_name: &str,
    template_snapshot: NamespaceSnapshot,
    iterations: usize,
    results: &mut Vec<ScenarioResult>,
) {
    println!(
        "\n>>> Running Benchmark: '{}' ({} iterations) <<<",
        scenario_name, iterations
    );

    let mut durations = Vec::with_capacity(iterations);

    // Create a single namespace from the snapshot to be used for all sequential iterations.
    let ns = Arc::new(Namespace::from_snapshot(template_snapshot));

    let mut rng = StdRng::from_entropy();
    for _ in 0..iterations {
        // Generate a new random key for each iteration to ensure it's always an insert.
        let key = format!("key_{}", rng.r#gen::<u64>());
        let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();

        let start = Instant::now();
        let old_value = ns.set(&key, value.clone()).await.unwrap();
        let duration = start.elapsed();

        // Clean up the key to keep the namespace size consistent for the next iteration.
        // This happens outside the timed block.
        if old_value.is_some() {
            panic!("Set (insert) operation returned an old value, but key should be new.");
        }
        if !ns.delete(&key).await {
            panic!("Failed to delete a key that was just inserted.");
        }
        durations.push(duration);
    }

    print_stats(scenario_name, &mut durations, results);
}

/// Measures sequential `set` operations for **existing keys (updates)**.
async fn run_set_update_benchmark_sequential(
    scenario_name: &str,
    template_snapshot: NamespaceSnapshot,
    keys_to_use: &[String],
    iterations: usize,
    results: &mut Vec<ScenarioResult>,
) {
    println!(
        "\n>>> Running Benchmark: '{}' ({} iterations) <<<",
        scenario_name, iterations
    );

    let ns = Arc::new(Namespace::from_snapshot(template_snapshot));
    let mut durations = Vec::with_capacity(iterations);

    let mut key_iter = keys_to_use.iter().cycle();
    let mut rng = StdRng::from_entropy();
    for _ in 0..iterations {
        let key = key_iter.next().unwrap();
        let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();

        let start = Instant::now();
        let old_value = ns.set(&key, value.clone()).await.unwrap();
        if old_value.is_none() {
            panic!("Set (update) operation did not return an old value for an existing key.");
        }
        let duration = start.elapsed();

        durations.push(duration);
    }
    print_stats(scenario_name, &mut durations, results);
}

/// Measures sequential `update` operations (which require the key to exist).
async fn run_update_benchmark_sequential(
    scenario_name: &str,
    template_snapshot: NamespaceSnapshot,
    keys_to_use: &[String],
    iterations: usize,
    results: &mut Vec<ScenarioResult>,
) {
    println!(
        "\n>>> Running Benchmark: '{}' ({} iterations) <<<",
        scenario_name, iterations
    );

    let ns = Arc::new(Namespace::from_snapshot(template_snapshot));
    let mut durations = Vec::with_capacity(iterations);

    let mut key_iter = keys_to_use.iter().cycle();
    let mut rng = StdRng::from_entropy();
    for _ in 0..iterations {
        let key = key_iter.next().unwrap();
        let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();

        let start = Instant::now();
        ns.update(&key, value.clone()).await.unwrap();
        let duration = start.elapsed();
        durations.push(duration);
    }
    print_stats(scenario_name, &mut durations, results);
}

/// Measures sequential `get` operations.
async fn run_get_benchmark_sequential(
    scenario_name: &str,
    template_snapshot: NamespaceSnapshot,
    keys_to_use: &[String],
    iterations: usize,
    results: &mut Vec<ScenarioResult>,
) {
    println!(
        "\n>>> Running Benchmark: '{}' ({} iterations) <<<",
        scenario_name, iterations
    );

    let ns = Arc::new(Namespace::from_snapshot(template_snapshot));
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
    print_stats(scenario_name, &mut durations, results);
}

/// Measures sequential `exists` operations.
async fn run_exists_benchmark_sequential(
    scenario_name: &str,
    template_snapshot: NamespaceSnapshot,
    keys_to_use: &[String],
    iterations: usize,
    results: &mut Vec<ScenarioResult>,
) {
    println!(
        "\n>>> Running Benchmark: '{}' ({} iterations) <<<",
        scenario_name, iterations
    );

    let ns = Arc::new(Namespace::from_snapshot(template_snapshot));
    let mut durations = Vec::with_capacity(iterations);

    let mut key_iter = keys_to_use.iter().cycle();
    for _ in 0..iterations {
        let key = key_iter.next().unwrap();
        let start = Instant::now();
        if !ns.exists(key).await {
            panic!("Exists operation failed to find a key that should exist.");
        }
        let duration = start.elapsed();

        durations.push(duration);
    }
    print_stats(scenario_name, &mut durations, results);
}

/// Measures sequential `delete` operations.
async fn run_delete_benchmark_sequential(
    scenario_name: &str,
    template_snapshot: NamespaceSnapshot,
    keys_to_use: &[String],
    iterations: usize,
    results: &mut Vec<ScenarioResult>,
) {
    println!(
        "\n>>> Running Benchmark: '{}' ({} iterations) <<<",
        scenario_name, iterations
    );

    let ns = Arc::new(Namespace::from_snapshot(template_snapshot));
    let mut durations = Vec::with_capacity(iterations);

    let mut key_iter = keys_to_use.iter().cycle();
    let mut rng = StdRng::from_entropy();
    for _ in 0..iterations {
        let key = key_iter.next().unwrap();

        let value_to_restore: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();

        let start = Instant::now();
        let deleted = ns.delete(key).await;
        let duration = start.elapsed();
        durations.push(duration);

        if !deleted {
            panic!("Failed to delete a key that should have existed: {}", key);
        }
        ns.set(key, value_to_restore.clone()).await.unwrap();
    }
    print_stats(scenario_name, &mut durations, results);
}

/// Measures sequential `consume` operations.
async fn run_consume_benchmark_sequential(
    scenario_name: &str,
    template_snapshot: NamespaceSnapshot,
    keys_to_use: &[String],
    iterations: usize,
    results: &mut Vec<ScenarioResult>,
) {
    println!(
        "\n>>> Running Benchmark: '{}' ({} iterations) <<<",
        scenario_name, iterations
    );

    let ns = Arc::new(Namespace::from_snapshot(template_snapshot));
    let mut durations = Vec::with_capacity(iterations);

    let mut key_iter = keys_to_use.iter().cycle();
    for _ in 0..iterations {
        let key = key_iter.next().unwrap();

        let start = Instant::now();
        let consumed_value = ns.consume(key).await;
        let duration = start.elapsed();
        durations.push(duration);

        match consumed_value {
            Some(value) => ns.set(key, (*value).clone()).await.unwrap(),
            _ => panic!("Failed to consume a key that should have existed: {}", key),
        };
    }
    print_stats(scenario_name, &mut durations, results);
}

/// Calculates and prints latency statistics for a set of measurements.
fn print_stats(
    scenario_name: &str,
    durations: &mut Vec<Duration>,
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
    println!("------------------------------------");

    // Store results for JSON output
    results.push(ScenarioResult {
        scenario_name: scenario_name.to_string(),
        min_latency_ns: min_duration.as_nanos(),
        avg_latency_ns: avg_duration.as_nanos(),
        p99_latency_ns: p99_duration.as_nanos(),
        max_latency_ns: max_duration.as_nanos(),
    });
}

#[derive(Serialize)]
struct ScenarioResult {
    scenario_name: String,
    min_latency_ns: u128,
    avg_latency_ns: u128,
    p99_latency_ns: u128,
    max_latency_ns: u128,
}
