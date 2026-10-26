// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Portable file operation inputs and provider-result validation. Paths use
//! Linux container semantics; credentials and host paths never cross this API.
use crate::{Limits, Operation};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::{
    containers::{FileMetadata, FilePath, MAX_FILE_ENTRIES},
    Context,
};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Read {
    path: String,
    max_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Write {
    path: String,
    data: String,
    mode: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Stat {
    path: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct List {
    path: String,
    max_bytes: usize,
    max_entries: usize,
}

pub(crate) fn validate(operation: Operation, input: &Value, limits: &Limits) -> Result<()> {
    let path = match operation {
        Operation::ReadFile => {
            let value: Read = serde_json::from_value(input.clone())?;
            ensure!(
                value.max_bytes <= limits.max_body_bytes,
                "file byte limit exceeds host limit"
            );
            value.path
        }
        Operation::WriteFile => {
            let value: Write = serde_json::from_value(input.clone())?;
            ensure!(
                value.path != "/" && value.mode <= 0o777,
                "invalid file destination or mode"
            );
            ensure!(
                value.data.len() <= limits.max_body_bytes.div_ceil(3).saturating_mul(4),
                "file data exceeds host limit"
            );
            ensure!(
                STANDARD.decode(value.data)?.len() <= limits.max_body_bytes,
                "file data exceeds host limit"
            );
            value.path
        }
        Operation::StatFile => serde_json::from_value::<Stat>(input.clone())?.path,
        Operation::ListFiles => {
            let value: List = serde_json::from_value(input.clone())?;
            ensure!(
                value.max_bytes <= limits.max_body_bytes
                    && (1..=MAX_FILE_ENTRIES).contains(&value.max_entries),
                "file listing limits exceed host limit"
            );
            value.path
        }
        _ => anyhow::bail!("unsupported file operation"),
    };
    FilePath::new(path)?;
    Ok(())
}

pub(crate) async fn execute(
    context: &Context,
    operation: Operation,
    input: Value,
) -> Result<Value> {
    let container = context.container()?;
    match operation {
        Operation::ReadFile => {
            let value: Read = serde_json::from_value(input)?;
            Ok(json!(STANDARD.encode(
                container
                    .read_file(context, FilePath::new(value.path)?, value.max_bytes)
                    .await?
            )))
        }
        Operation::WriteFile => {
            let value: Write = serde_json::from_value(input)?;
            container
                .write_file(
                    context,
                    FilePath::new(value.path)?,
                    STANDARD.decode(value.data)?,
                    value.mode,
                )
                .await?;
            Ok(Value::Null)
        }
        Operation::StatFile => {
            let value: Stat = serde_json::from_value(input)?;
            Ok(serde_json::to_value(
                container
                    .stat_file(context, FilePath::new(value.path)?)
                    .await?,
            )?)
        }
        Operation::ListFiles => {
            let value: List = serde_json::from_value(input)?;
            Ok(serde_json::to_value(
                container
                    .list_files(
                        context,
                        FilePath::new(value.path)?,
                        value.max_bytes,
                        value.max_entries,
                    )
                    .await?,
            )?)
        }
        _ => anyhow::bail!("unsupported file operation"),
    }
}

pub(crate) fn validate_metadata(value: &Value, limit: usize) -> Result<()> {
    ensure!(
        value.get("modified_ms").is_some() && value.get("link_target").is_some(),
        "file metadata must explicitly represent nullable fields"
    );
    let metadata: FileMetadata = serde_json::from_value(value.clone())?;
    ensure!(
        metadata.size <= 9_007_199_254_740_991
            && metadata
                .modified_ms
                .is_none_or(|value| value.unsigned_abs() <= 9_007_199_254_740_991),
        "file metadata exceeds portable integer range"
    );
    ensure!(
        metadata.mode <= 0o777
            && !metadata.name.is_empty()
            && metadata.name.len() <= 4096
            && !metadata.name.contains('\0')
            && metadata
                .link_target
                .as_ref()
                .is_none_or(|value| value.len() <= 4096 && !value.contains('\0')),
        "invalid file metadata"
    );
    ensure!(
        serde_json::to_vec(value)?.len() <= limit,
        "file metadata exceeds result limit"
    );
    Ok(())
}

pub(crate) fn validate_listing(value: &Value, limit: usize, max_entries: usize) -> Result<()> {
    let entries = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("file list must be an array"))?;
    ensure!(
        entries.len() <= max_entries,
        "file list exceeds requested entry limit"
    );
    let mut names = std::collections::HashSet::new();
    for entry in entries {
        validate_metadata(entry, limit)?;
        let name = entry["name"].as_str().unwrap();
        ensure!(
            !name.is_empty()
                && !matches!(name, "." | "..")
                && !name.contains('/')
                && names.insert(name),
            "invalid or duplicate listing name"
        );
    }
    ensure!(
        serde_json::to_vec(value)?.len() <= limit,
        "file list exceeds result limit"
    );
    Ok(())
}

#[cfg(test)]
#[path = "files_tests.rs"]
mod tests;
