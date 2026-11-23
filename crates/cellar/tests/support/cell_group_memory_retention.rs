// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Synthetic terminal media, never admission or recovery proof.

use anyhow::{Context as _, Result};
use futures_util::TryStreamExt as _;
use object_store::{memory::InMemory, ObjectStore as _};
use std::path::PathBuf;

/// Copy only concrete synthetic memory. External stores cannot enter this API.
/// Call after fixture cleanup; this diagnostic is not a transactional closure.
pub async fn retain(store: &InMemory, phase: &'static str) -> Result<Option<PathBuf>> {
    let mut objects = store.list(None).try_collect::<Vec<_>>().await?;
    if objects.is_empty() {
        return Ok(None);
    }
    objects.sort_by(|a, b| a.location.cmp(&b.location));
    let root = tempfile::Builder::new()
        .prefix("cellar-memory-")
        .tempdir()?
        .keep();
    // Keep even a partial export if a diagnostic write fails.
    eprintln!("retained synthetic fixture memory: {}", root.display());
    let mut manifest = Vec::new();
    for (ordinal, object) in objects.into_iter().enumerate() {
        let response = store.get(&object.location).await?;
        let metadata = response.meta.clone();
        let bytes = response.bytes().await?;
        // Opaque object keys are metadata, never filesystem paths.
        let file = format!("{ordinal:06}.bin");
        std::fs::write(root.join(&file), &bytes)?;
        manifest.push(serde_json::json!({
            "object": object.location.as_ref(), "file": file,
            "bytes": bytes.len(), "e_tag": metadata.e_tag, "version": metadata.version,
        }));
    }
    std::fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "format": 1, "phase": phase, "transactional": false, "objects": manifest,
        }))?,
    )?;
    eprintln!(
        "MEMORY_RETENTION {}",
        serde_json::json!({"phase": phase, "objects": manifest.len(), "complete": true})
    );
    Ok(Some(root))
}

/// Preserve terminal bytes without replacing the original operation failure.
pub async fn preserve_result(
    store: &InMemory,
    phase: &'static str,
    result: Result<()>,
) -> Result<()> {
    match (result, retain(store, phase).await) {
        (result, Ok(_)) => result,
        (Ok(()), Err(error)) => Err(error.context("retaining synthetic fixture memory")),
        (Err(error), Err(retention)) => {
            Err(error.context(format!("retaining synthetic fixture memory: {retention:#}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn terminal_memory_preserves_opaque_bytes_and_original_failure() -> Result<()> {
        let store = super::super::fault::Store::default();
        let key = object_store::path::Path::from("private/key-metadata");
        let bytes = bytes::Bytes::from_static(b"\0\xffpayload");
        store.inner.put(&key, bytes.clone().into()).await?;
        store
            .fail_read
            .store(true, std::sync::atomic::Ordering::Release);
        store
            .deny_group_archive_writes
            .store(true, std::sync::atomic::Ordering::Release);
        let root = retain(&store.inner, "retention-control")
            .await?
            .context("nonempty memory snapshot")?;
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("manifest.json"))?)?;
        let entries = manifest["objects"].as_array().context("object manifest")?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["object"], key.as_ref());
        assert_eq!(entries[0]["file"], "000000.bin");
        assert_eq!(std::fs::read(root.join("000000.bin"))?, bytes.as_ref());
        assert!(!root.join("private").exists());
        let failure = preserve_result(
            &store.inner,
            "failure-control",
            Err(anyhow::anyhow!("original ambiguous outcome")),
        )
        .await
        .err()
        .context("original failure must survive retention")?;
        assert_eq!(failure.to_string(), "original ambiguous outcome");
        Ok(())
    }
}
