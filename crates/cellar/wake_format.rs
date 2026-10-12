// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Initialize a fresh Cellar fleet's durable wake format. No Celld import.
use crate::bucket::Bucket;
use anyhow::ensure;

const KEY: &str = "wake/format.json";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FormatRecord {
    system: String,
    format: u8,
}

fn decode(bytes: &[u8]) -> anyhow::Result<()> {
    let record: FormatRecord = serde_json::from_slice(bytes)?;
    ensure!(
        record.system == "cellar" && record.format == 2,
        "unsupported wake format; start Cellar with fresh fleet storage"
    );
    Ok(())
}

/// Concurrent starters can initialize the same fresh fleet. A confirmed
/// Cellar marker also resolves a lost conditional-write response.
pub async fn ensure_ready(bucket: &Bucket) -> anyhow::Result<()> {
    let result = initialize(bucket).await;
    if result.is_err() {
        if let Some((bytes, _)) = bucket.get(KEY).await? {
            decode(&bytes)?;
            return Ok(());
        }
    }
    result
}

async fn initialize(bucket: &Bucket) -> anyhow::Result<()> {
    if let Some((bytes, _)) = bucket.get(KEY).await? {
        return decode(&bytes);
    }
    for prefix in ["cells/", "nodes/", "wake/entries/", "wake/retired/"] {
        ensure!(bucket.objects_page(prefix, None, 1).await?.objects.is_empty(),
            "uninitialized fleet contains state under {prefix}; Cellar requires a hard cutover to fresh storage");
    }
    bucket
        .put_cas(
            KEY,
            serde_json::to_vec(&FormatRecord {
                system: "cellar".into(),
                format: 2,
            })?,
            None,
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fresh_fleet_initialization_is_concurrent_and_reopenable() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("store.sqlite");
        let bucket = Bucket::open_dev(&path)?;
        let results = futures_util::future::join_all((0..8).map(|_| ensure_ready(&bucket))).await;
        for result in results {
            result?;
        }
        let reopened = Bucket::open_dev(&path)?;
        ensure_ready(&reopened).await?;
        let marker = reopened.get(KEY).await?.unwrap().0;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&marker)?["system"],
            "cellar"
        );
        Ok(())
    }

    #[tokio::test]
    async fn cutover_refuses_unmarked_state_and_previous_markers() -> anyhow::Result<()> {
        let directory = tempfile::tempdir()?;
        let bucket = Bucket::open_dev(&directory.path().join("state.sqlite"))?;
        bucket
            .put("cells/Account:old/owner.json", b"{}".to_vec())
            .await?;
        assert!(ensure_ready(&bucket).await.is_err());
        assert!(bucket.get(KEY).await?.is_none());
        let old = Bucket::open_dev(&directory.path().join("old.sqlite"))?;
        old.put(KEY, br#"{"format":2,"migrating":false}"#.to_vec())
            .await?;
        assert!(ensure_ready(&old).await.is_err());
        Ok(())
    }
}
