// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persist exact test intents before conditional archive mutations.
use super::{Cluster, Result};
use sha2::{Digest, Sha256};

pub(crate) async fn persist_archive_intent<T: serde::Serialize + serde::de::DeserializeOwned>(
    cluster: &Cluster,
    intent: &T,
) -> Result<T> {
    let bytes = serde_json::to_vec(intent)?;
    let name = hex::encode(Sha256::digest(&bytes));
    let directory = cluster._root.path().join("archive-intents");
    let loaded = cluster
        .domain
        .blocking(move || -> Result<Vec<u8>> {
            use std::io::Write;
            std::fs::create_dir_all(&directory)?;
            let path = directory.join(name);
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::File::open(&directory)?.sync_all()?;
            Ok(std::fs::read(path)?)
        })?
        .await??;
    Ok(serde_json::from_slice(&loaded)?)
}
