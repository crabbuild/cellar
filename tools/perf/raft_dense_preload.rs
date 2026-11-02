// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Initial-image lab setup using the full-capacity qualification procedure.
//! Permanent member claims and complete image checks precede all runtimes and
//! client acknowledgements. This fixture does not qualify ordinary ingestion.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Result};
use cellar::sdk::{
    consensus::{
        verification::{Catalog, CommittedImage, DurableApply, GroupWrite, ReplicaLog},
        *,
    },
    ExecutionDomain,
};
use cellar_logic::consensus::OperationId;
use cellar_ltx::{
    apply::ImageState,
    internal::{
        ExtentId, ExtentManifest, Geometry, ImageBinding, NodeExtentStore, SealedImageRoot,
        EXTENT_BYTES,
    },
    DirectFileSystem, FileSystem, TXID,
};
use object_store::ObjectStore;
use raft::{prelude::Entry, Storage as _};
use sha2::{Digest, Sha256};
use std::{
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
};

#[cfg(test)]
#[path = "raft_dense_preload_tests.rs"]
mod tests;

fn template(path: &Path, cell: usize, mib: usize) -> Result<ImageState> {
    ensure!(!path.exists(), "preload template already exists");
    let db = rusqlite::Connection::open(path)?;
    db.execute_batch("PRAGMA page_size=4096;PRAGMA journal_mode=DELETE;PRAGMA synchronous=OFF;PRAGMA cache_size=-2048;BEGIN IMMEDIATE;")?;
    crate::reference_workload::apply(
        &db,
        crate::reference_workload::Change::Initialize { database_bytes: 0 },
    )?;
    for block in (0..mib).step_by(4) {
        crate::reference_workload::apply(
            &db,
            crate::reference_workload::Change::Populate {
                seed: cell as u64,
                block,
                blocks: (mib - block).min(4),
            },
        )?;
    }
    db.execute_batch("COMMIT")?;
    drop(db);
    let mut file = std::fs::File::open(path)?;
    file.sync_all()?;
    let length = file.metadata()?.len();
    let mut digest = Sha256::new();
    let mut bytes = vec![0; 1024 * 1024];
    loop {
        let count = file.read(&mut bytes)?;
        if count == 0 {
            break;
        }
        digest.update(&bytes[..count]);
    }
    Ok(ImageState {
        epoch: 1,
        txid: TXID(1),
        checksum: 0,
        page_size: 4096,
        length,
        digest: digest.finalize().into(),
    })
}

struct Installed {
    marker: CommittedImage,
    _store: NodeExtentStore,
    _image: SealedImageRoot,
}

fn install(
    directory: PathBuf,
    template: PathBuf,
    state: ImageState,
    history: [u8; 32],
    schema: [u8; 32],
) -> Result<Installed> {
    let filesystem: Arc<dyn FileSystem> = Arc::new(DirectFileSystem);
    let store = NodeExtentStore::open_serialized(
        directory.join("raft-cells/extent-inventory"),
        filesystem.clone(),
        Default::default(),
    )?;
    let geometry = Geometry::new(state.page_size, state.length)?;
    let mut source = filesystem.open(&template)?;
    ensure!(
        source.file_len()? == state.length,
        "preload image length changed"
    );
    let mut buffer = vec![0; EXTENT_BYTES as usize];
    let mut objects = Vec::new();
    let mut digest = Sha256::new();
    for index in 0..geometry.extent_count() {
        let length = geometry
            .extent_length(index)
            .ok_or_else(|| anyhow::anyhow!("preload geometry"))? as usize;
        let bytes = &mut buffer[..length];
        source.read_exact_into_at(u64::from(index) * EXTENT_BYTES, bytes)?;
        digest.update(&*bytes);
        let id = ExtentId::of(bytes)?;
        store.put_extent(id, |offset, count| {
            bytes
                .get(offset as usize..offset as usize + count)
                .map(<[u8]>::to_vec)
                .ok_or_else(|| std::io::Error::other("preload extent range"))
        })?;
        objects.push(id);
    }
    ensure!(
        <[u8; 32]>::from(digest.finalize()) == state.digest,
        "preload image digest changed"
    );
    let manifest = ExtentManifest::new(
        ImageBinding {
            history,
            schema,
            epoch: state.epoch,
            txid: state.txid.0,
            checksum: state.checksum,
        },
        geometry,
        &objects,
    )?;
    let image = store.seal_image_root(manifest, &mut buffer)?;
    let reference = image.reference();
    let marker = CommittedImage {
        name: format!("{}.root", hex::encode(reference.root().bytes())),
        commitment: ImageCommitment::Extents(reference),
        length: state.length,
        ltx_epoch: state.epoch,
        txid: state.txid.0,
        checksum: state.checksum,
        page_size: state.page_size,
    };
    Ok(Installed {
        marker,
        _store: store,
        _image: image,
    })
}

pub(super) async fn run(
    store: Arc<dyn ObjectStore>,
    schema: [u8; 32],
    total: usize,
    quota: u64,
) -> Result<()> {
    let mib: usize = std::env::var("RAFT_DENSE_PRELOAD_MIB")?.parse()?;
    ensure!((1..=1024).contains(&mib), "invalid preload size");
    ensure!((1..=32).contains(&total), "invalid preload cell count");
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let worker = domain.clone();
    let result = domain.run(async move {
        let mut config = NamespaceConfig::new(ClusterId::new([1; 16])?, NamespaceId::new([2; 16])?, NamespaceMode::Raft, schema);
        config.image_storage = RaftImageStorage::Extents;
        let namespace = Catalog::new(store, CatalogOptions { prefix: "perf/multi-raft".into(), ..Default::default() })?.open_namespace(config).await?;
        let mut logs = Vec::new();
        for node in 1..=3u8 {
            let directory = PathBuf::from(format!("/replicas/node{node}/node"));
            ensure!(!directory.join("raft").exists(), "preload refuses retained voter data");
            let process = ProcessGeneration::new(std::fs::read(format!("/config/process{node}"))?.try_into().map_err(|_| anyhow::anyhow!("preload process identity"))?)?;
            let log = ReplicaLog::open(ReplicaLogOptions::new(directory.join("raft"), NodeId::new([node; 16])?, DiskLineage::new([node + 10; 16])?, process, LogOpenMode::Create), Arc::new(DirectFileSystem)).await?;
            logs.push((directory, log));
        }
        let prepared = async {
            for cell in 0..total {
                let path = PathBuf::from(format!("/replicas/template-{cell}.sqlite"));
                let create = path.clone();
                let state = worker.blocking(move || template(&create, cell, mib))?.await??;
                ensure!(state.length <= quota, "preload exceeds image quota");
                let birth = namespace.resolve_birth(BirthProposal { scope: crate::raft_disaster::scope(cell, "Workload", 0)?, voters: (1..=3u8).map(|node| Ok(InitialMember { node: NodeId::new([node; 16])?, member: MemberId::new(u64::from(node))? })).collect::<Result<_>>()?, learners: vec![] }).await?;
                for (node, (directory, log)) in logs.iter().enumerate() {
                    let permit = namespace.bootstrap_member(&birth, log).await?;
                    let group = log.group(birth.scope())?;
                    let mut write = GroupWrite::new(&group);
                    write.entries.push(Entry { index: 2, term: 2, ..Default::default() });
                    let mut hard = group.initial_state()?.hard_state;
                    hard.term = 2; hard.vote = 0; hard.commit = 2;
                    write.hard_state = Some(hard);
                    let directory = directory.clone(); let path = path.clone(); let history = birth.digest();
                    let image = worker.blocking(move || install(directory, path, state, history, schema))?.await??;
                    write.applied = Some(DurableApply { position: LogPosition { index: LogIndex::new(2), term: Term::new(2) }, image: Some(image.marker.clone()) });
                    let persisted = log.persist(OperationId::new(cell as u64 + 1), vec![write]).await;
                    worker.blocking(move || drop(image))?.await?;
                    drop(permit);
                    persisted?;
                    cellar::note!("dense_preloaded cell={} node={} payload_mib={} image_bytes={} sha256={}", cell, node+1, mib, state.length, hex::encode(state.digest));
                }
                worker.blocking(move || std::fs::remove_file(path))?.await??;
            }
            Ok::<_, anyhow::Error>(())
        }.await;
        for (_, log) in logs { log.close().await?; }
        prepared
    }).await;
    domain.shutdown().await;
    result?
}
