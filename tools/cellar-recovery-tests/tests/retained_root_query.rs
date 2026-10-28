// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Offline diagnosis of a retained qualification worker. This is not a native
//! recovery oracle: only the committed marker selects the root, and all work
//! runs on private copies so failed-trial evidence remains untouched.
#![allow(clippy::disallowed_methods)]

#[path = "support/query_io.rs"]
mod query_io;

use anyhow::{ensure, Context as _, Result};
use cellar::{
    application::consensus::{verification::*, ImageCommitment},
    sdk::ExecutionDomain,
};
use cellar_logic::consensus::*;
use cellar_ltx::{
    generation::ReadServices,
    internal::{open_extent_image, CacheLimits, NodeExtentStore},
    stream_snapshot::ScratchPool,
    DirectFileSystem, FileSystem,
};
use sha2::{Digest as _, Sha256};
use std::{path::Path, sync::Arc, time::Instant};

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    std::fs::create_dir(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            ensure!(kind.is_file(), "refuse links in retained worker");
            match DirectFileSystem.clone_read_write(&entry.path(), &target)? {
                Some(file) => drop(file),
                None => {
                    std::fs::copy(entry.path(), target)?;
                }
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires an offline qualification worker via CELLAR_RETAINED_QUERY_WORKER"]
async fn retained_marker_payload_query() -> Result<()> {
    let source = std::path::PathBuf::from(std::env::var("CELLAR_RETAINED_QUERY_WORKER")?);
    ensure!(source.is_absolute(), "retained worker must be absolute");
    let parent = source.parent().context("worker parent")?.to_owned();
    let copies = tokio::task::spawn_blocking(move || -> Result<_> {
        // Refuse a live worker and retain both source locks until its private
        // copies are complete. Taking these existing locks changes no content.
        let _log = DirectFileSystem.lock_file(&source.join("raft/CELLAR_LOCK"))?;
        let _inventory =
            DirectFileSystem.lock_file(&source.join("raft-cells/extent-inventory/extents.lock"))?;
        let copy = tempfile::Builder::new()
            .prefix("query-diagnostic-")
            .tempdir_in(parent)?;
        copy_tree(&source.join("raft"), &copy.path().join("raft"))?;
        copy_tree(
            &source.join("raft-cells/extent-inventory"),
            &copy.path().join("extents"),
        )?;
        std::fs::create_dir(copy.path().join("cache"))?;
        Ok(copy)
    })
    .await??;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                copies.path().join("raft"),
                NodeId::new([4; 16])?,
                DiskLineage::new([13; 16])?,
                ProcessGeneration::new([250; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let result = async {
        let scope = GroupScope {
            cluster: ClusterId::new([81; 16])?,
            namespace: NamespaceId::new([82; 16])?,
            cell: "Counter:full-capacity".into(),
            group: GroupId::new([83; 16])?,
            incarnation: IncarnationId::new([84; 16])?,
        };
        let applied = log.group(&scope)?.applied()?;
        let marker = applied.image.context("committed image")?;
        let ImageCommitment::Extents(reference) = marker.commitment else {
            anyhow::bail!("retained marker must select extents");
        };
        cellar::note!(
            "[retained-query] marker={} index={} bytes={}",
            marker.name,
            applied.position.index.get(),
            marker.length
        );
        tokio::task::spawn_blocking(move || -> Result<_> {
            let io = Arc::new(query_io::ObservedFiles::default());
            let store = NodeExtentStore::open(
                copies.path().join("extents"),
                io.clone(),
                Default::default(),
            )?;
            let view = store.open_root(reference)?;
            // Native admission re-establishes this proof before serving too.
            // Keep its cost separate from the query and retain the same owner.
            let started = Instant::now();
            for index in 0..view.manifest().geometry().extent_count() {
                store.verify_extent(view.manifest().extent(index).context("extent")?)?;
            }
            cellar::note!("[retained-query] closure_verify_seconds={:.6}", started.elapsed().as_secs_f64());
            let before = io.capture();
            let image = view.image(
                copies.path().join("cache"),
                ScratchPool::default(),
                CacheLimits::default(),
            )?;
            let started = Instant::now();
            let db = open_extent_image(
                image,
                ReadServices {
                    now_unix_millis: Arc::new(|| 1_700_000_000_000),
                    fill_random: Arc::new(|bytes| bytes.fill(1)),
                },
            )?;
            let (counter, bytes): (u64, u64) = db.connection().query_row(
                "SELECT (SELECT value FROM counter),coalesce(sum(length(value)),0) FROM items",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            ensure!(counter == 9 && bytes > 9_000_000_000, "retained fixture state differs");
            let last: u64 = db.connection().query_row(
                "SELECT id FROM items ORDER BY id DESC LIMIT 1", [], |row| row.get(0),
            )?;
            for id in [1, last / 2 + 1, last] {
                let payload: Vec<u8> = db.connection().query_row(
                    "SELECT value FROM items WHERE id=?1", [id], |row| row.get(0),
                )?;
                ensure!(payload.len() == 1048576, "payload size");
                cellar::note!("[retained-query] sample_id={id} sha256={}", hex::encode(Sha256::digest(payload)));
            }
            let seconds = started.elapsed().as_secs_f64();
            cellar::note!("[retained-query] seconds={seconds:.6} counter={counter} payload_bytes={bytes} io={}", io.capture().delta(before));
            drop(db);
            drop(view);
            drop(store);
            drop(copies);
            Ok(seconds)
        }).await?
    }.await;
    domain.run(log.close()).await??;
    ensure!(
        result? < 60.0,
        "payload query exceeded the unchanged native 60-second budget"
    );
    Ok(())
}
