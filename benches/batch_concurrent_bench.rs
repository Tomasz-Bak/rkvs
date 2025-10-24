use futures::future::join_all;
use rand::prelude::*;
use rkvs::{BatchMode, NamespaceConfig, NamespaceSnapshot, StorageManager, namespace::Namespace};
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

const TOTAL_BATCH_OPERATIONS: usize = 10_000; // Number of batch ops to run per scenario
const VALUE_SIZE: usize = 256;
const KEY_COUNT: usize = 100_000;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let storage = StorageManager::builder().build().await.unwrap();
        storage.initialize(None).await.unwrap();

        let mut all_results = Vec::new();

        println!("Preparing a {}-key namespace snapshot...", KEY_COUNT);
        let (snapshot_100k, random_keys) = prepare_snapshot(&storage, KEY_COUNT).await;
        println!("Snapshot preparation complete.");

        let shard_counts = [1, 5, 10, 15];
        let concurrency_levels = [8, 16, 32, 64];
        let batch_sizes = [50, 100, 200];
        let modes = [BatchMode::AllOrNothing, BatchMode::BestEffort];

        for &shard_count in &shard_counts {
            println!("\n==================================================");
            println!(
                "  RUNNING CONCURRENT BATCH BENCHMARKS FOR {} SHARD(S)",
                shard_count
            );
            println!("==================================================");

            let ns = Arc::new(Namespace::from_snapshot(snapshot_100k.clone()));
            if shard_count > 1 {
                ns.resize_shards(shard_count).await.unwrap();
            }

            for &concurrency in &concurrency_levels {
                for &batch_size in &batch_sizes {
                    for &mode in &modes {
                        let mode_str = if mode == BatchMode::AllOrNothing {
                            "AllOrNothing"
                        } else {
                            "BestEffort"
                        };

                        // --- Batch Set ---
                        let scenario_name = format!(
                            "Concurrent Batch Set (Size {}) ({}) @ {} Concurrency ({} Shard(s))",
                            batch_size, mode_str, concurrency, shard_count
                        );
                        run_concurrent_set_benchmark(
                            &scenario_name,
                            ns.clone(),
                            TOTAL_BATCH_OPERATIONS,
                            concurrency,
                            batch_size,
                            mode,
                            &mut all_results,
                        )
                        .await;

                        // --- Batch Get ---
                        let scenario_name = format!(
                            "Concurrent Batch Get (Size {}) ({}) @ {} Concurrency ({} Shard(s))",
                            batch_size, mode_str, concurrency, shard_count
                        );
                        run_concurrent_get_benchmark(
                            &scenario_name,
                            ns.clone(),
                            &random_keys,
                            TOTAL_BATCH_OPERATIONS,
                            concurrency,
                            batch_size,
                            mode,
                            &mut all_results,
                        )
                        .await;

                        // --- Batch Delete ---
                        let scenario_name = format!(
                            "Concurrent Batch Delete (Size {}) ({}) @ {} Concurrency ({} Shard(s))",
                            batch_size, mode_str, concurrency, shard_count
                        );
                        run_concurrent_delete_benchmark(
                            &scenario_name,
                            ns.clone(),
                            &random_keys,
                            TOTAL_BATCH_OPERATIONS,
                            concurrency,
                            batch_size,
                            mode,
                            &mut all_results,
                        )
                        .await;
                    }
                }
            }
        }

        let output_dir = "assets/benchmarks";
        std::fs::create_dir_all(output_dir).unwrap();
        let results_json = serde_json::to_string_pretty(&all_results).unwrap();
        let output_path =
            std::path::Path::new(output_dir).join("batch_concurrent_bench_results.json");
        std::fs::write(&output_path, results_json).unwrap();
        println!(
            "\n✅ Concurrent batch benchmark results saved to {}",
            output_path.display()
        );
    });
}

async fn prepare_snapshot(
    storage: &Arc<StorageManager>,
    key_count: usize,
) -> (NamespaceSnapshot, Vec<String>) {
    let mut rng = StdRng::from_entropy();
    let random_keys: Vec<String> = (0..key_count)
        .map(|_| format!("key_{}", rng.r#gen::<u64>()))
        .collect();

    let temp_ns_name = "snapshot_builder_batch_concurrent";
    let ns_config = NamespaceConfig::default();
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

async fn run_concurrent_set_benchmark(
    scenario_name: &str,
    ns: Arc<Namespace>,
    total_iterations: usize,
    concurrency_level: usize,
    batch_size: usize,
    mode: BatchMode,
    results: &mut Vec<ScenarioResult>,
) {
    println!("\n>>> Running Benchmark: '{}'", scenario_name);
    let total_batches = total_iterations / concurrency_level;
    let mut all_durations = Vec::with_capacity(total_iterations);

    for _ in 0..total_batches {
        let mut handles = Vec::with_capacity(concurrency_level);
        for _ in 0..concurrency_level {
            let ns_clone = ns.clone();
            let mut rng = thread_rng();
            let batch: Vec<(String, Vec<u8>)> = (0..batch_size)
                .map(|_| {
                    let key = format!("new_key_{}", rng.r#gen::<u64>());
                    let value: Vec<u8> = (0..VALUE_SIZE).map(|_| rng.r#gen()).collect();
                    (key, value)
                })
                .collect();
            let keys_to_delete: Vec<String> = batch.clone().into_iter().map(|(k, _)| k).collect();

            handles.push(tokio::spawn(async move {
                let start = Instant::now();
                let result = ns_clone.set_multiple(batch.clone(), mode).await;
                let duration = start.elapsed();

                if result.errors.is_some() {
                    panic!("Batch set failed: {:?}", result.errors);
                }

                // Cleanup outside of timing
                ns_clone
                    .delete_multiple(keys_to_delete, BatchMode::BestEffort)
                    .await;

                duration
            }));
        }
        let durations: Vec<Duration> = join_all(handles)
            .await
            .into_iter()
            .map(|res| res.unwrap())
            .collect();
        all_durations.extend(durations);
    }
    print_stats(scenario_name, &mut all_durations, batch_size, results);
}

async fn run_concurrent_get_benchmark(
    scenario_name: &str,
    ns: Arc<Namespace>,
    keys_to_use: &[String],
    total_iterations: usize,
    concurrency_level: usize,
    batch_size: usize,
    mode: BatchMode,
    results: &mut Vec<ScenarioResult>,
) {
    println!("\n>>> Running Benchmark: '{}'", scenario_name);
    let total_batches = total_iterations / concurrency_level;
    let mut all_durations = Vec::with_capacity(total_iterations);

    for i in 0..total_batches {
        let mut handles = Vec::with_capacity(concurrency_level);
        for j in 0..concurrency_level {
            let ns_clone = ns.clone();
            let start_index = (i * concurrency_level + j) * batch_size;
            let batch: Vec<String> = (0..batch_size)
                .map(|k| keys_to_use[(start_index + k) % keys_to_use.len()].clone())
                .collect();

            handles.push(tokio::spawn(async move {
                let start = Instant::now();
                let result = ns_clone.get_multiple(batch, mode).await;
                if result.errors.is_some() {
                    panic!("Batch get failed: {:?}", result.errors);
                }
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
    print_stats(scenario_name, &mut all_durations, batch_size, results);
}

async fn run_concurrent_delete_benchmark(
    scenario_name: &str,
    ns: Arc<Namespace>,
    keys_to_use: &[String],
    total_iterations: usize,
    concurrency_level: usize,
    batch_size: usize,
    mode: BatchMode,
    results: &mut Vec<ScenarioResult>,
) {
    println!("\n>>> Running Benchmark: '{}'", scenario_name);
    let total_batches = total_iterations / concurrency_level;
    let mut all_durations = Vec::with_capacity(total_iterations);

    for i in 0..total_batches {
        let mut handles = Vec::with_capacity(concurrency_level);
        for j in 0..concurrency_level {
            let ns_clone = ns.clone();
            let start_index = (i * concurrency_level + j) * batch_size;
            let batch: Vec<String> = (0..batch_size)
                .map(|k| keys_to_use[(start_index + k) % keys_to_use.len()].clone())
                .collect();
            let mut rng = thread_rng();
            let items_to_restore: Vec<(String, Vec<u8>)> = batch
                .clone()
                .into_iter()
                .map(|key| (key, (0..VALUE_SIZE).map(|_| rng.r#gen()).collect()))
                .collect();

            handles.push(tokio::spawn(async move {
                let start = Instant::now();
                let result = ns_clone.delete_multiple(batch, mode).await;
                if result.errors.is_some() {
                    panic!("Batch delete failed: {:?}", result.errors);
                }
                let duration = start.elapsed();

                // Restore keys
                ns_clone
                    .set_multiple(items_to_restore, BatchMode::BestEffort)
                    .await;

                duration
            }));
        }
        let durations: Vec<Duration> = join_all(handles)
            .await
            .into_iter()
            .map(|res| res.unwrap())
            .collect();
        all_durations.extend(durations);
    }
    print_stats(scenario_name, &mut all_durations, batch_size, results);
}

fn print_stats(
    scenario_name: &str,
    durations: &mut Vec<Duration>,
    batch_size: usize,
    results: &mut Vec<ScenarioResult>,
) {
    if durations.is_empty() {
        return;
    }
    durations.sort();

    let total_op_duration: Duration = durations.iter().sum();
    let avg_op_duration = total_op_duration / durations.len() as u32;
    let avg_item_duration = avg_op_duration / batch_size as u32;

    println!("--- Results for '{}' ---", scenario_name);
    println!("Avg Latency (per item):      {:?}", avg_item_duration);
    println!("Avg Latency (per op):        {:?}", avg_op_duration);
    println!("------------------------------------");

    results.push(ScenarioResult {
        scenario_name: scenario_name.to_string(),
        avg_latency_per_item_ns: avg_item_duration.as_nanos(),
        avg_latency_per_op_ns: avg_op_duration.as_nanos(),
    });
}

#[derive(Serialize)]
struct ScenarioResult {
    scenario_name: String,
    avg_latency_per_item_ns: u128,
    avg_latency_per_op_ns: u128,
}
