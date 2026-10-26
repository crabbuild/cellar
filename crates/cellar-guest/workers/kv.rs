// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::Limits;
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::{
    native::clone as clone_value,
    sdk::{Context, ListOptions, StoredClone},
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashSet;

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct Input {
    keys: Vec<String>,
    entries: Vec<(String, String)>,
    start: Option<String>,
    end: Option<String>,
    start_after: Option<String>,
    prefix: Option<String>,
    limit: Option<usize>,
    reverse: bool,
}

fn key(key: &str) -> Result<()> {
    ensure!(
        !key.is_empty() && key.len() <= 2048 && !key.starts_with("__sys_"),
        "invalid storage key"
    );
    Ok(())
}

fn encoded(value: StoredClone, maximum: usize) -> Result<(String, usize)> {
    ensure!(
        !value.stored_stubs,
        "stored RPC targets require materialization"
    );
    let bytes = clone_value::encode_with_limits(&value.value, limits(maximum))?;
    let length = bytes.len();
    Ok((STANDARD.encode(bytes), length))
}

fn limits(bytes: usize) -> clone_value::Limits {
    clone_value::Limits {
        bytes,
        objects: 100_000,
        values: 200_000,
        depth: 64,
    }
}

pub(super) fn execute(
    context: &Context,
    name: &str,
    input: Value,
    options: &Limits,
) -> Result<Value> {
    let allowed: &[&str] = match name {
        "workers.kv.get" | "workers.kv.delete" => &["keys"],
        "workers.kv.put" => &["entries"],
        "workers.kv.list" => &["start", "end", "start_after", "prefix", "limit", "reverse"],
        "workers.kv.delete_all" => &[],
        _ => anyhow::bail!(crate::failure::Validation),
    };
    ensure!(
        input
            .as_object()
            .is_some_and(|input| input.keys().all(|key| allowed.contains(&key.as_str()))),
        crate::failure::Validation
    );
    if matches!(name, "workers.kv.get" | "workers.kv.delete") {
        ensure!(input["keys"].is_array(), crate::failure::Validation);
    }
    if name == "workers.kv.put" {
        ensure!(input["entries"].is_array(), crate::failure::Validation);
    }
    let input: Input = serde_json::from_value(input).context(crate::failure::Validation)?;
    let entries = (|| -> Result<_> {
        ensure!(input.keys.len().saturating_add(input.entries.len()) <= options.max_batch_operations, "storage batch limit");
        let mut seen = HashSet::new();
        for k in input.keys.iter().chain(input.entries.iter().map(|(key, _)| key)) {
            key(k)?;
            ensure!(seen.insert(k), "duplicate storage key");
        }
        for bound in [&input.start, &input.end, &input.start_after, &input.prefix].into_iter().flatten() {
            ensure!(bound.len() <= 2048 && !bound.starts_with("__sys_"), "invalid storage bound");
        }
        ensure!(input.start.is_none() || input.start_after.is_none(), "conflicting storage bounds");
        ensure!(input.limit.is_none_or(|n| n > 0 && n <= options.max_batch_operations), "storage list limit");
        let mut total = 0_usize;
        input.entries.iter().map(|(key, value)| {
            let bytes = STANDARD.decode(value)?;
            total = total.saturating_add(bytes.len());
            ensure!(total <= options.max_body_bytes, "storage values exceed batch byte limit");
            let value = clone_value::decode_with_limits(&bytes, limits(options.max_body_bytes))?;
            let marker: Vec<u16> = "__cellar_runtime_capability__".encode_utf16().collect();
            ensure!(!value.objects.iter().any(|object| matches!(object, clone_value::Object::Object(properties)
                if properties.iter().any(|(key, _)| matches!(key, clone_value::Value::String(key) if key == &marker)))),
                "runtime capabilities require materialization");
            Ok((key.clone(), StoredClone { value, stored_stubs: false }))
        }).collect::<Result<Vec<_>>>()
    })().context(crate::failure::Validation)?;
    let storage = context.storage()?;
    match name {
        "workers.kv.get" => {
            let result = storage.transaction_sync(|storage| {
                let mut bytes = 0_usize;
                let mut rows = Vec::new();
                for key in &input.keys {
                    if let Some(value) = storage.get_clone(key)? {
                        let (value, length) = encoded(value, options.max_body_bytes)?;
                        bytes = bytes.saturating_add(length);
                        ensure!(
                            bytes <= options.max_body_bytes,
                            "storage result exceeds limit"
                        );
                        rows.push((key.clone(), value));
                    }
                }
                Ok(rows)
            })?;
            Ok(json!(result))
        }
        "workers.kv.put" => {
            storage.put_many_stored_clone(&entries)?;
            Ok(Value::Null)
        }
        "workers.kv.delete" => Ok(json!(storage.delete_many(&input.keys)?)),
        "workers.kv.list" => {
            let mut bytes = 0_usize;
            let maximum = input.limit.unwrap_or(options.max_batch_operations);
            let mut list = ListOptions {
                start: input.start,
                end: input.end,
                start_after: input.start_after,
                prefix: input.prefix,
                limit: None,
                reverse: input.reverse,
            };
            let mut cursor = storage.list_cursor(&list)?;
            let mut result = Vec::new();
            while result.len() < maximum {
                let Some(row) = cursor.next() else {
                    break;
                };
                let (key, value) = row?;
                if key.starts_with("__sys_") {
                    // Jump over the reserved range once instead of scanning an
                    // unbounded number of host-private entries or charging them
                    // against the application's visible result limit.
                    if list.reverse {
                        list.end = Some(
                            list.end
                                .as_deref()
                                .map_or("__sys_", |end| end.min("__sys_"))
                                .to_owned(),
                        );
                    } else {
                        list.start = Some("__sys`".to_owned());
                        list.start_after = None;
                    }
                    drop(cursor);
                    cursor = storage.list_cursor(&list)?;
                    continue;
                }
                let (value, length) =
                    encoded(StoredClone::try_from(value)?, options.max_body_bytes)?;
                bytes = bytes.saturating_add(length);
                ensure!(
                    bytes <= options.max_body_bytes,
                    "storage list exceeds limit"
                );
                result.push((key, value));
            }
            Ok(json!(result))
        }
        "workers.kv.delete_all" => {
            storage.delete_all()?;
            Ok(Value::Null)
        }
        _ => anyhow::bail!("unsupported storage operation"),
    }
}
