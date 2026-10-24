// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded byte inventories of the actual retained proof, never a replacement.
use anyhow::{ensure, Context, Result};
use cellar::{bucket::Bucket, sdk::ExecutionDomain};
use serde_json::Value;
use std::path::Path;

pub(super) async fn root(domain: &ExecutionDomain, bucket: &Bucket) -> Result<Value> {
    domain
        .run(async {
            let (bytes, _) = bucket
                .get("nodes/recovery-owner.json")
                .await?
                .context("original recovery root is missing")?;
            Ok::<_, anyhow::Error>(serde_json::from_slice(&bytes)?)
        })
        .await?
}

pub(super) async fn bucket(
    domain: &ExecutionDomain,
    bucket: &Bucket,
    scope: &str,
) -> Result<Vec<(String, Vec<u8>)>> {
    domain
        .run(async {
            let mut objects = bucket.list(&format!("cells/{scope}/ltx/")).await?;
            objects.extend(bucket.list("log/recovery-owner/").await?);
            ensure!(
                objects.len() <= 100,
                "recovery proof inventory exceeds its bound"
            );
            objects.sort_by(|a, b| a.location.cmp(&b.location));
            let mut total = 0usize;
            let mut rows = Vec::new();
            for object in objects {
                let key = object.location.to_string();
                let (bytes, _) = bucket
                    .get(&key)
                    .await?
                    .context("retained replica vanished")?;
                total = total
                    .checked_add(bytes.len())
                    .context("proof inventory overflow")?;
                ensure!(total <= 32 << 20, "recovery proof bytes exceed their bound");
                rows.push((key, bytes.to_vec()));
            }
            Ok::<_, anyhow::Error>(rows)
        })
        .await?
}

pub(super) fn follower(directory: &Path, root: &Value) -> Result<Vec<(String, Vec<u8>)>> {
    let lineage = root["lineage"].as_str().context("proof lineage missing")?;
    let generation = root["generation"]
        .as_str()
        .context("proof generation missing")?;
    ensure!(
        lineage.len() == 32
            && generation.len() == 64
            && lineage
                .bytes()
                .chain(generation.bytes())
                .all(|c| c.is_ascii_hexdigit()),
        "proof identity is not a canonical path component"
    );
    let path = directory
        .join("witness/peerlog/recovery-owner")
        .join(lineage)
        .join(generation);
    let entries = std::fs::read_dir(path)?;
    let mut rows = Vec::new();
    let mut total = 0usize;
    for entry in entries {
        let entry = entry?;
        ensure!(
            rows.len() < 100 && entry.file_type()?.is_file(),
            "invalid follower proof inventory"
        );
        total = total
            .checked_add(usize::try_from(entry.metadata()?.len())?)
            .context("follower proof overflow")?;
        ensure!(total <= 32 << 20, "follower proof bytes exceed their bound");
        rows.push((
            entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("invalid proof filename"))?,
            std::fs::read(entry.path())?,
        ));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    ensure!(
        rows.iter().any(|(name, _)| name.ends_with(".batch")),
        "no actual committed follower batch"
    );
    Ok(rows)
}
