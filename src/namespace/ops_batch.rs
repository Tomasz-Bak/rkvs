//! Contains all batch operations for the `Namespace`.
use super::{BatchMode, Namespace};
use crate::types::{BatchError, BatchResult};
use std::collections::HashMap;
use std::time::Instant;

impl Namespace {
    /// Returns a list of all keys in the namespace.
    pub async fn list_keys(&self) -> Vec<String> {
        let shards_guard = self.timeout_read_lock(&self.shards, "list_keys").await.unwrap(); // Should not fail
        let mut all_keys = Vec::with_capacity(self.metadata.key_count());

        for shard in shards_guard.iter() {
            if let Ok(data) = self.timeout_read_lock(shard, "list_keys shard").await {
                for key in data.all_keys() {
                    all_keys.push(String::from_utf8_lossy(&key).to_string());
                }
            }
        }

        all_keys
    }

    /// Sets multiple key-value pairs in a single batch operation.
    pub async fn set_multiple(
        &self,
        items: Vec<(String, Vec<u8>)>,
        mode: BatchMode,
    ) -> BatchResult<Vec<(String, Option<Vec<u8>>)>> {
        let byte_items = items.into_iter().map(|(k, v)| (k.into_bytes(), v)).collect();
        let result = self.set_multiple_bytes(byte_items, mode).await;

        let data = result.data.map(|op_results| {
            op_results
                .into_iter()
                .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v))
                .collect()
        });

        BatchResult {
            data,
            total_processed: result.total_processed,
            duration: result.duration,
            errors: result.errors,
        }
    }

    /// Sets multiple key-value pairs using binary keys in a single batch operation.
    pub async fn set_multiple_bytes(
        &self,
        items: Vec<(Vec<u8>, Vec<u8>)>,
        mode: BatchMode,
    ) -> BatchResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_items = items.len();

        // Group items by shard
        let shards_guard = self
            .timeout_read_lock(&self.shards, "set_multiple_bytes")
            .await
            .unwrap(); // Should not fail
        let shard_count = shards_guard.len();
        let mut items_by_shard: HashMap<usize, Vec<(usize, Vec<u8>, Vec<u8>)>> =
            HashMap::with_capacity(shard_count);

        let max_value_size = self.config.max_value_size();
        for (index, (key_bytes, value)) in items.into_iter().enumerate() {
            // Basic validation that doesn't require locks
            if value.len() > max_value_size {
                errors.push(BatchError {
                    key: String::from_utf8_lossy(&key_bytes).to_string(),
                    operation: "set".to_string(),
                    error_message: format!("Value size {} exceeds maximum {}", value.len(), max_value_size),
                    index,
                });
                continue;
            }
            let shard_idx = super::helpers::jump_consistent_hash(&key_bytes, shard_count);
            items_by_shard
                .entry(shard_idx)
                .or_default()
                .push((index, key_bytes, value));
        }

        let mut operation_results = Vec::with_capacity(total_items);

        match mode {
            BatchMode::AllOrNothing => {
                // If basic validation already failed, abort.
                if !errors.is_empty() {
                    return BatchResult {
                        data: None,
                        total_processed: total_items,
                        duration: start.elapsed(),
                        errors: Some(errors),
                    };
                }

                let mut shard_indices: Vec<usize> = items_by_shard.keys().copied().collect();
                shard_indices.sort_unstable();

                let mut locked_shards = Vec::with_capacity(shard_indices.len());
                for shard_idx in &shard_indices {
                    match self
                        .timeout_write_lock(
                            &shards_guard[*shard_idx],
                            &format!("set_multiple shard {}", shard_idx),
                        )
                        .await
                    {
                        Ok(guard) => locked_shards.push((*shard_idx, guard)),
                        Err(_) => {
                            return BatchResult {
                                data: None,
                                total_processed: total_items,
                                duration: start.elapsed(),
                                errors: Some(vec![BatchError {
                                    key: "batch".to_string(),
                                    operation: "set_multiple".to_string(),
                                    error_message: format!(
                                        "Failed to acquire lock on shard {} within timeout",
                                        shard_idx
                                    ),
                                    index: 0,
                                }]),
                            }
                        }
                    }
                }

                // With all shards locked, perform final validation and prepare changes.
                let mut total_size_increase: isize = 0;
                let mut new_keys_count: isize = 0;
                for (shard_idx, shard_guard) in &locked_shards {
                    if let Some(shard_items) = items_by_shard.get(shard_idx) {
                        for (_, key_bytes, value) in shard_items {
                            if let Some(existing) = shard_guard.get_value(key_bytes) {
                                total_size_increase += value.len() as isize - existing.len() as isize;
                            } else {
                                new_keys_count += 1;
                                total_size_increase += value.len() as isize;
                            }
                        }
                    }
                }

                // Validate max_keys limit with the accurate new_keys_count
                let max_keys = self.config.max_keys();
                if self.metadata.key_count() + new_keys_count as usize > max_keys {
                    return BatchResult {
                        data: None,
                        total_processed: total_items,
                        duration: start.elapsed(),
                        errors: Some(vec![BatchError {
                            key: "batch".to_string(),
                            operation: "set_multiple".to_string(),
                            error_message: format!("Key count limit {} would be exceeded", max_keys),
                            index: 0,
                        }]),
                    };
                }

                // Execute all writes
                for (shard_idx, mut shard_guard) in locked_shards {
                    if let Some(shard_items) = items_by_shard.get(&shard_idx) {
                        for (_, key, value) in shard_items {
                            let old_value = shard_guard.set_value(key.clone(), value.clone());
                            operation_results.push((key.clone(), old_value.map(|v| (*v).clone())));
                        }
                    }
                }

                self.metadata.update_key_count(new_keys_count);
                self.metadata.update_total_size(total_size_increase);
            }
            BatchMode::BestEffort => {
                let mut total_size_increase: isize = 0;
                let mut new_keys_count: isize = 0;

                for (shard_idx, shard_items) in items_by_shard {
                    let mut data = match self
                        .timeout_write_lock(
                            &shards_guard[shard_idx],
                            &format!("set_multiple shard {}", shard_idx),
                        )
                        .await
                    {
                        Ok(guard) => guard,
                        Err(_) => continue, // Skip this shard on timeout in BestEffort mode
                    };

                    for (_, key, value) in shard_items {
                        let old_value = data.set_value(key.clone(), value.clone());
                        operation_results.push((key.clone(), old_value.as_ref().map(|v| (**v).clone())));
                        if let Some(old) = &old_value {
                            total_size_increase += value.len() as isize - old.len() as isize;
                        } else {
                            new_keys_count += 1;
                            total_size_increase += value.len() as isize;
                        }
                    }
                }
                self.metadata.update_key_count(new_keys_count);
                self.metadata.update_total_size(total_size_increase);
            }
        }

        BatchResult {
            data: Some(operation_results),
            total_processed: total_items,
            duration: start.elapsed(),
            errors: if errors.is_empty() { None } else { Some(errors) },
        }
    }

    /// Gets multiple key-value pairs in a single batch operation.
    pub async fn get_multiple(
        &self,
        keys: Vec<String>,
        mode: BatchMode,
    ) -> BatchResult<Vec<(String, Vec<u8>)>> {
        let byte_keys = keys.into_iter().map(|k| k.into_bytes()).collect();
        let result = self.get_multiple_bytes(byte_keys, mode).await;

        let data = result.data.map(|items| {
            items
                .into_iter()
                .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v))
                .collect()
        });

        BatchResult {
            data,
            total_processed: result.total_processed,
            duration: result.duration,
            errors: result.errors,
        }
    }

    /// Gets multiple key-value pairs using binary keys in a single batch operation.
    pub async fn get_multiple_bytes(
        &self,
        keys: Vec<Vec<u8>>,
        mode: BatchMode,
    ) -> BatchResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let start = Instant::now();
        let mut errors = Vec::new();
        let total_keys = keys.len();

        let shards_guard = self
            .timeout_read_lock(&self.shards, "get_multiple_bytes")
            .await
            .unwrap(); // Should not fail
        let shard_count = shards_guard.len();

        // Group keys by shard
        let mut keys_by_shard: HashMap<usize, Vec<(usize, Vec<u8>)>> =
            HashMap::with_capacity(shard_count);
        for (index, key_bytes) in keys.into_iter().enumerate() {
            let shard_idx = super::helpers::jump_consistent_hash(&key_bytes, shard_count);
            keys_by_shard
                .entry(shard_idx)
                .or_default()
                .push((index, key_bytes));
        }

        let mut items = Vec::with_capacity(total_keys);

        match mode {
            BatchMode::AllOrNothing => {
                // Validate all keys exist before returning any data
                for (shard_idx, shard_keys) in &keys_by_shard {
                    let data = match self
                        .timeout_read_lock(
                            &shards_guard[*shard_idx],
                            &format!("get_multiple shard {}", shard_idx),
                        )
                        .await
                    {
                        Ok(guard) => guard,
                        Err(e) => {
                            // On timeout, the entire AllOrNothing operation fails.
                            return BatchResult {
                                data: None,
                                total_processed: total_keys,
                                duration: start.elapsed(),
                                errors: Some(vec![BatchError {
                                    key: "batch".to_string(),
                                    operation: "get".to_string(),
                                    error_message: e.to_string(),
                                    index: 0,
                                }]),
                            };
                        }
                    };
                    for (index, key_bytes) in shard_keys {
                        if !data.has_key(key_bytes) {
                            errors.push(BatchError {
                                key: String::from_utf8_lossy(key_bytes).to_string(),
                                operation: "get".to_string(),
                                error_message: "Key not found".to_string(),
                                index: *index,
                            });
                        }
                    }
                }

                if !errors.is_empty() {
                    return BatchResult {
                        data: None,
                        total_processed: total_keys,
                        duration: start.elapsed(),
                        errors: Some(errors),
                    };
                }

                // If all keys exist, retrieve them
                for (shard_idx, shard_keys) in keys_by_shard {
                    let data = match self
                        .timeout_read_lock(
                            &shards_guard[shard_idx],
                            &format!("get_multiple shard {}", shard_idx),
                        )
                        .await
                    {
                        Ok(guard) => guard,
                        Err(e) => {
                            // This should be unreachable if validation passed, but we handle it for safety.
                            return BatchResult {
                                data: None,
                                total_processed: total_keys,
                                duration: start.elapsed(),
                                errors: Some(vec![BatchError {
                                    key: "batch".to_string(),
                                    operation: "get".to_string(),
                                    error_message: e.to_string(),
                                    index: 0,
                                }]),
                            };
                        }
                    };
                    for (_, key_bytes) in shard_keys {
                        if let Some(value) = data.get_value(&key_bytes) {
                            items.push((key_bytes, (*value).clone()));
                        }
                    }
                }
            }
            BatchMode::BestEffort => {
                for (shard_idx, shard_keys) in keys_by_shard {
                    let data = match self
                        .timeout_read_lock(
                            &shards_guard[shard_idx],
                            &format!("get_multiple shard {}", shard_idx),
                        )
                        .await
                    {
                        Ok(guard) => guard,
                        Err(e) => {
                            // If the shard lock times out, report an error for all keys in that shard.
                            for (index, key_bytes) in shard_keys {
                                errors.push(BatchError {
                                    key: String::from_utf8_lossy(&key_bytes).to_string(),
                                    operation: "get".to_string(),
                                    error_message: e.to_string(),
                                    index,
                                });
                            }
                            continue;
                        }
                    };
                    for (index, key_bytes) in shard_keys {
                        if let Some(value) = data.get_value(&key_bytes) {
                            items.push((key_bytes, (*value).clone()));
                        } else {
                            errors.push(BatchError {
                                key: String::from_utf8_lossy(&key_bytes).to_string(),
                                operation: "get".to_string(),
                                error_message: "Key not found".to_string(),
                                index,
                            });
                        }
                    }
                }
            }
        }

        BatchResult {
            data: Some(items),
            total_processed: total_keys,
            duration: start.elapsed(),
            errors: if errors.is_empty() { None } else { Some(errors) },
        }
    }

    /// Deletes multiple keys in a single batch operation.
    pub async fn delete_multiple(&self, keys: Vec<String>, mode: BatchMode) -> BatchResult<()> {
        let byte_keys = keys.into_iter().map(|k| k.into_bytes()).collect();
        self.delete_multiple_bytes(byte_keys, mode).await
    }

    /// Deletes multiple keys using binary keys in a single batch operation.
    pub async fn delete_multiple_bytes(&self, keys: Vec<Vec<u8>>, mode: BatchMode) -> BatchResult<()> {
        let result = self.consume_multiple_bytes(keys, mode).await;

        // We don't need the consumed data, just the status of the operation.
        BatchResult {
            data: result.data.map(|_| ()),
            total_processed: result.total_processed,
            duration: result.duration,
            errors: result.errors,
        }
    }

    /// Consumes multiple key-value pairs in a single batch operation.
    pub async fn consume_multiple(
        &self,
        keys: Vec<String>,
        mode: BatchMode,
    ) -> BatchResult<Vec<(String, Vec<u8>)>> {
        let start = Instant::now();
        let byte_keys: Vec<Vec<u8>> = keys.into_iter().map(|k| k.into_bytes()).collect();
        let result = self.consume_multiple_bytes(byte_keys, mode).await;

        let data = result.data.map(|items| {
            items
                .into_iter()
                .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), v))
                .collect()
        });

        BatchResult {
            data,
            total_processed: result.total_processed,
            duration: start.elapsed(),
            errors: result.errors,
        }
    }

    /// Consumes multiple key-value pairs using binary keys in a single batch operation.
    pub async fn consume_multiple_bytes(
        &self,
        keys: Vec<Vec<u8>>,
        mode: BatchMode,
    ) -> BatchResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let start = Instant::now();
        let total_keys = keys.len();

        let shards_guard = self
            .timeout_read_lock(&self.shards, "consume_multiple_bytes")
            .await
            .unwrap(); // Should not fail
        let shard_count = shards_guard.len();

        // Group keys by shard
        let mut keys_by_shard: HashMap<usize, Vec<(usize, Vec<u8>)>> =
            HashMap::with_capacity(shard_count);
        for (index, key) in keys.into_iter().enumerate() {
            let shard_idx = super::helpers::jump_consistent_hash(&key, shard_count);
            keys_by_shard
                .entry(shard_idx)
                .or_default()
                .push((index, key));
        }

        let mut errors = Vec::new();
        let mut items = Vec::with_capacity(total_keys);

        match mode {
            BatchMode::AllOrNothing => {
                let mut shard_indices: Vec<usize> = keys_by_shard.keys().copied().collect();
                shard_indices.sort_unstable();

                let mut locked_shards = Vec::with_capacity(shard_indices.len());
                for shard_idx in &shard_indices {
                    match self
                        .timeout_write_lock(
                            &shards_guard[*shard_idx],
                            &format!("consume_multiple shard {}", shard_idx),
                        )
                        .await
                    {
                        Ok(guard) => locked_shards.push((*shard_idx, guard)),
                        Err(_) => {
                            return BatchResult {
                                data: None,
                                total_processed: total_keys,
                                duration: start.elapsed(),
                                errors: Some(vec![BatchError {
                                    key: "batch".to_string(),
                                    operation: "consume".to_string(),
                                    error_message: format!(
                                        "Failed to acquire lock on shard {} within timeout",
                                        shard_idx
                                    ),
                                    index: 0,
                                }]),
                            }
                        }
                    }
                }

                // Validate all keys exist
                for (shard_idx, shard_guard) in &locked_shards {
                    if let Some(shard_keys) = keys_by_shard.get(shard_idx) {
                        for (index, key) in shard_keys {
                            if !shard_guard.has_key(key) {
                                return BatchResult {
                                    data: None,
                                    total_processed: total_keys,
                                    duration: start.elapsed(),
                                    errors: Some(vec![BatchError {
                                        key: String::from_utf8_lossy(key).to_string(),
                                        operation: "delete".to_string(),
                                        error_message: "Key not found".to_string(),
                                        index: *index,
                                    }]),
                                };
                            }
                        }
                    }
                }

                // All keys exist, proceed with consumption
                let mut consumed_keys_count: isize = 0;
                let mut total_size_decrease: isize = 0;

                for (shard_idx, shard_guard) in locked_shards.iter_mut() {
                    if let Some(shard_keys) = keys_by_shard.get(shard_idx) {
                        for (_, key) in shard_keys {
                            if let Some(value) = shard_guard.delete_value(key) {
                                consumed_keys_count += 1;
                                total_size_decrease += value.len() as isize;
                                items.push((key.clone(), (*value).clone()));
                            }
                        }
                    }
                }

                self.metadata.update_key_count(-consumed_keys_count);
                self.metadata.update_total_size(-total_size_decrease);
            }
            BatchMode::BestEffort => {
                let mut consumed_keys_count: isize = 0;
                let mut total_size_decrease: isize = 0;

                for (shard_idx, shard_keys) in keys_by_shard {
                    let mut data = match self
                        .timeout_write_lock(
                            &shards_guard[shard_idx],
                            &format!("consume_multiple shard {}", shard_idx),
                        )
                        .await
                    {
                        Ok(guard) => guard,
                        Err(_) => continue, // Skip this shard on timeout
                    };
                    for (index, key) in shard_keys {
                        if let Some(value) = data.delete_value(&key) {
                            consumed_keys_count += 1;
                            total_size_decrease += value.len() as isize;
                            items.push((key, (*value).clone()));
                        } else {
                            errors.push(BatchError {
                                key: String::from_utf8_lossy(&key).to_string(),
                                operation: "consume".to_string(),
                                error_message: "Key not found".to_string(),
                                index,
                            });
                        }
                    }
                }
                self.metadata.update_key_count(-consumed_keys_count);
                self.metadata.update_total_size(-total_size_decrease);
            }
        }

        BatchResult {
            data: Some(items),
            total_processed: total_keys,
            duration: start.elapsed(),
            errors: if errors.is_empty() { None } else { Some(errors) },
        }
    }
}