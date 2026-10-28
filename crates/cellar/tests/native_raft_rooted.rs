// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real engine markers, runtime tickets and SQLite extent images. This owning
//! boundary does not qualify the unfinished SDK writer or remote-root transport.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_rooted_archive.rs"]
mod archive;
#[path = "support/raft_rooted_download.rs"]
mod archive_download;
#[path = "support/raft_catalog_fault.rs"]
#[allow(dead_code)]
mod archive_faults;
#[path = "support/raft_rooted_archive_head.rs"]
mod archive_heads;
#[path = "support/raft_rooted_archive_retry.rs"]
mod archive_retry;
#[path = "support/raft_rooted_execution.rs"]
mod execution;
#[path = "support/raft_storage_fault.rs"]
mod fault;
#[path = "support/raft_ltx_fixture.rs"]
#[allow(dead_code)]
mod images;
#[path = "support/raft_rooted_publication.rs"]
mod publication;
#[path = "support/raft_rooted_install.rs"]
mod snapshot_installs;
#[path = "support/raft_rooted_snapshot.rs"]
mod snapshots;
#[path = "support/raft_rooted_stream.rs"]
mod streams;

use anyhow::{bail, Result};
use cellar::{
    application::consensus::{
        verification::*, CellUpdate, EnvelopeLimits, ImageCommitment, LtxImage, LtxUpdate,
    },
    sdk::ExecutionDomain,
};
use cellar_logic::consensus::*;
use cellar_ltx::{
    generation::ReadServices,
    internal::{
        open_extent_image, CacheLimits, ExtentId, ExtentManifest, ExtentRootReference, Geometry,
        ImageBinding, NodeExtentStore, EXTENT_BYTES,
    },
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem, FileSystem,
};
use std::{path::Path, sync::Arc, time::Duration};

fn scope() -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:7".into(),
        group: GroupId::new([7; 16]).unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    }
}

fn manifest_schema(bytes: &[u8], txid: u64, schema: [u8; 32]) -> Result<ExtentManifest> {
    Ok(ExtentManifest::new(
        ImageBinding {
            history: [7; 32],
            schema,
            epoch: 1,
            txid,
            checksum: 0,
        },
        Geometry::new(if bytes.is_empty() { 0 } else { 512 }, bytes.len() as u64)?,
        &bytes
            .chunks(EXTENT_BYTES as usize)
            .map(ExtentId::of)
            .collect::<Result<Vec<_>, _>>()?,
    )?)
}

fn image_position(manifest: &ExtentManifest) -> Result<LtxImage> {
    let binding = manifest.binding();
    Ok(LtxImage {
        epoch: binding.epoch,
        txid: binding.txid,
        checksum: binding.checksum,
        commitment: ImageCommitment::Extents(ExtentRootReference::new(
            manifest.root(),
            manifest.encode()?.len() as u32,
        )?),
    })
}

fn update(before: &[u8], after: &[u8], base_txid: u64) -> Result<CellUpdate> {
    update_schema(before, after, base_txid, [9; 32])
}

fn update_schema(
    before: &[u8],
    after: &[u8],
    base_txid: u64,
    schema: [u8; 32],
) -> Result<CellUpdate> {
    let cut = images::Cut::new(before, after, base_txid, false, true);
    Ok(CellUpdate {
        scope: scope(),
        application_digest: schema,
        proposal: ProposalId::new([base_txid as u8 + 1; 16])?,
        base: image_position(&manifest_schema(before, base_txid, schema)?)?,
        result: image_position(&manifest_schema(after, base_txid + 1, schema)?)?,
        files: vec![LtxUpdate {
            min_txid: base_txid + 1,
            max_txid: base_txid + 1,
            page_count: cut.pages.len() as u64,
            decoded_bytes: cut.pages.len() as u64 * 512,
            bytes: cut.bytes,
        }],
    })
}

struct Host {
    domain: ExecutionDomain,
    log: ReplicaLog,
    runtime: RaftRuntime,
    events: RuntimeEvents,
    store: NodeExtentStore,
    applier: RootImageApplier,
    gate: Arc<fault::Gate>,
    buffer: Vec<u8>,
}

impl Host {
    async fn open(root: &Path, resume: bool, process: u8) -> Result<Self> {
        Self::open_schema(root, resume, process, [9; 32]).await
    }

    async fn open_schema(root: &Path, resume: bool, process: u8, schema: [u8; 32]) -> Result<Self> {
        Self::open_history(root, resume, process, schema, [7; 32]).await
    }

    async fn open_history(
        root: &Path,
        resume: bool,
        process: u8,
        schema: [u8; 32],
        history: [u8; 32],
    ) -> Result<Self> {
        Self::open_members(root, resume, process, schema, history, 1, vec![]).await
    }

    async fn open_members(
        root: &Path,
        resume: bool,
        process: u8,
        schema: [u8; 32],
        history: [u8; 32],
        member: u64,
        learners: Vec<MemberId>,
    ) -> Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let gate = fault::Gate::new(None);
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    root.join("raft"),
                    NodeId::new([1; 16])?,
                    DiskLineage::new([101; 16])?,
                    ProcessGeneration::new([process; 16])?,
                    if resume {
                        LogOpenMode::Resume
                    } else {
                        LogOpenMode::Create
                    },
                ),
                Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
            ))
            .await??;
        if !resume {
            log.create_group(GroupBootstrap {
                scope: scope(),
                member: MemberId::new(member)?,
                birth_digest: history,
                voters: vec![MemberId::new(1)?],
                learners,
            })
            .await?;
        }
        let store = NodeExtentStore::open(
            root.join("extents"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?;
        let mut buffer = vec![0; EXTENT_BYTES as usize];
        let applier = RootImageApplier::recover(
            log.group(&scope())?,
            store.clone(),
            schema,
            EnvelopeLimits::default(),
            64 * 1024 * 1024,
            &mut buffer,
        )?;
        let (runtime, events) = RaftRuntime::start(
            log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                election_seed: Some([11; 32]),
                ..Default::default()
            },
        )
        .await?;
        Ok(Self {
            domain,
            log,
            runtime,
            events,
            store,
            applier,
            gate,
            buffer,
        })
    }

    async fn elect(&mut self) -> Result<()> {
        self.runtime.campaign(&scope()).await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                while let Ok(event) = self.events.try_recv() {
                    if let RuntimeEventKind::Failed { error, .. } = event.kind {
                        return Err(error.into());
                    }
                }
                let status = self.runtime.status(&scope()).await?;
                if status.protocol_ready && !status.awaiting_ready && !status.awaiting_apply {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?
    }

    async fn next_apply(&mut self) -> Result<(ApplyTicket, raft::prelude::Entry)> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match self
                    .events
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("events closed"))?
                    .kind
                {
                    RuntimeEventKind::Apply { ticket, entry } => return Ok((ticket, entry)),
                    RuntimeEventKind::Failed { error, .. } => return Err(error.into()),
                    _ => (),
                }
            }
        })
        .await?
    }

    async fn applied(&mut self, expected: &ApplyTicket) -> Result<RootPublishedImage> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match self
                    .events
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("events closed"))?
                    .kind
                {
                    RuntimeEventKind::Applied { ticket, .. } if ticket == *expected => {
                        return Ok(self.applier.publish(&ticket, &mut self.buffer)?)
                    }
                    RuntimeEventKind::Failed { error, .. } => return Err(error.into()),
                    _ => (),
                }
            }
        })
        .await?
    }

    async fn propose(
        &mut self,
        update: &CellUpdate,
    ) -> Result<(ApplyTicket, raft::prelude::Entry)> {
        self.runtime
            .propose(&scope(), vec![], update.encode(EnvelopeLimits::default())?)
            .await?;
        self.next_apply().await
    }

    async fn commit(&mut self, update: &CellUpdate) -> Result<RootPublishedImage> {
        let (ticket, entry) = self.propose(update).await?;
        let complete = self
            .applier
            .prepare(ticket.clone(), &entry, &mut self.buffer)?;
        self.runtime.complete_apply(complete).await?;
        self.applied(&ticket).await
    }

    async fn close(self) -> Result<()> {
        let runtime = self.runtime.close().await;
        let log = self.log.close().await;
        self.domain.shutdown().await;
        runtime?;
        log?;
        Ok(())
    }
}

fn read(root: &RootPublishedImage, directory: &Path) -> Result<u64> {
    DirectFileSystem.create_dir_all(directory)?;
    let image = root.image().view().image(
        directory.into(),
        ScratchPool::new(ScratchLimits {
            max_bytes: 2 * EXTENT_BYTES,
            max_files: 2,
        })?,
        CacheLimits {
            max_cached_bytes: EXTENT_BYTES,
            max_cached_extents: 1,
        },
    )?;
    let db = open_extent_image(
        image,
        ReadServices {
            now_unix_millis: Arc::new(|| 1_700_000_000_000),
            fill_random: Arc::new(|bytes| bytes.fill(1)),
        },
    )?;
    assert_eq!(
        db.connection()
            .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?,
        "ok"
    );
    Ok(db
        .connection()
        .query_row("SELECT value FROM counter", [], |row| row.get(0))?)
}

fn initial() -> Result<Vec<u8>> {
    images::image(None, "CREATE TABLE counter(value);INSERT INTO counter VALUES(7);CREATE TABLE padding(value);INSERT INTO padding VALUES(zeroblob(5242880))")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extent_publication_waits_for_the_exact_engine_marker_and_sync() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut host = Host::open(directory.path(), false, 1).await?;
    host.elect().await?;
    let bytes = initial()?;
    let (ticket, entry) = host.propose(&update(&[], &bytes, 0)?).await?;
    let completion = host
        .applier
        .prepare(ticket.clone(), &entry, &mut host.buffer)?;
    assert!(host.applier.current().is_none());
    assert!(matches!(
        host.applier.publish(&ticket, &mut host.buffer),
        Err(ImageApplyError::Unpublished)
    ));
    let release = host.gate.arm(fault::HOLD_SYNC);
    host.runtime.complete_apply(completion).await?;
    let entered = tokio::time::timeout(Duration::from_secs(5), host.gate.entered.notified()).await;
    let marker_before = host.log.group(&scope())?.applied()?.image;
    let prematurely_published = host.applier.publish(&ticket, &mut host.buffer);
    drop(release);
    entered?;
    assert!(marker_before.is_none());
    assert!(matches!(
        prematurely_published,
        Err(ImageApplyError::Unpublished)
    ));
    let published = host.applied(&ticket).await?;
    assert_eq!(published.position(), ticket.position());
    assert_eq!(
        published.marker(),
        host.log.group(&scope())?.applied()?.image.unwrap()
    );
    assert_eq!(
        host.applier.queryable()?.unwrap().marker(),
        published.marker()
    );
    assert_eq!(read(&published, &directory.path().join("query"))?, 7);
    assert!(
        matches!(
            host.store.reclaim(8),
            Err(cellar_ltx::internal::StoreError::Unreconciled)
        ),
        "a single group reconciled the node's complete reference set"
    );
    drop(published);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_selects_the_engine_root_and_replays_a_prepared_orphan_with_a_new_ticket(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut host = Host::open(directory.path(), false, 1).await?;
    host.elect().await?;
    let first = initial()?;
    drop(host.commit(&update(&[], &first, 0)?).await?);
    let after = images::image(Some(&first), "UPDATE counter SET value=8")?;
    let change = update(&first, &after, 1)?;
    let (ticket, entry) = host.propose(&change).await?;
    let orphan = host
        .applier
        .prepare(ticket.clone(), &entry, &mut host.buffer)?;
    drop(orphan);
    assert_eq!(
        read(
            &host.applier.current().unwrap(),
            &directory.path().join("before")
        )?,
        7
    );
    host.close().await?;
    let mut resumed = Host::open(directory.path(), true, 2).await?;
    assert_eq!(
        read(
            &resumed.applier.current().unwrap(),
            &directory.path().join("old")
        )?,
        7
    );
    let (replay, entry) = resumed.next_apply().await?;
    assert_eq!(replay.position(), ticket.position());
    assert_ne!(replay, ticket);
    let complete = resumed
        .applier
        .prepare(replay.clone(), &entry, &mut resumed.buffer)?;
    assert!(resumed
        .applier
        .publish(&ticket, &mut resumed.buffer)
        .is_err());
    resumed.runtime.complete_apply(complete).await?;
    let published = resumed.applied(&replay).await?;
    assert_eq!(read(&published, &directory.path().join("new"))?, 8);
    assert_eq!(published.marker().commitment, change.result.commitment);
    drop(published);
    resumed.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unrelated_entry_or_schema_never_acquires_a_native_root_completion() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut host = Host::open(directory.path(), false, 1).await?;
    host.elect().await?;
    let first = initial()?;
    let (ticket, entry) = host.propose(&update(&[], &first, 0)?).await?;
    let before = host.store.usage()?;
    let mut unrelated = entry.clone();
    unrelated.data = vec![0; 1024].into();
    assert!(host
        .applier
        .prepare(ticket.clone(), &unrelated, &mut host.buffer)
        .is_err());
    let group = host.log.group(&scope())?;
    let mut wrong = RootImageApplier::recover(
        group,
        host.store.clone(),
        [8; 32],
        EnvelopeLimits::default(),
        64 * 1024 * 1024,
        &mut host.buffer,
    )?;
    assert!(wrong
        .prepare(ticket.clone(), &entry, &mut host.buffer)
        .is_err());
    assert_eq!(host.store.usage()?, before);
    let complete = host
        .applier
        .prepare(ticket.clone(), &entry, &mut host.buffer)?;
    host.runtime.complete_apply(complete).await?;
    drop(host.applied(&ticket).await?);
    drop(wrong);
    host.close().await
}

#[test]
fn commitment_kinds_cannot_alias_or_change_inside_a_data_transition() -> Result<()> {
    let bytes = initial()?;
    let mut change = update(&[], &bytes, 0)?;
    let encoded = change.encode(EnvelopeLimits::default())?;
    assert_eq!(
        CellUpdate::decode(&encoded, EnvelopeLimits::default())?,
        change
    );
    let ImageCommitment::Extents(reference) = change.result.commitment else {
        bail!("root fixture");
    };
    change.result.commitment = ImageCommitment::Sha256(reference.root().bytes());
    assert!(
        change.encode(EnvelopeLimits::default()).is_err(),
        "root bytes were relabeled as a flat digest"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_extent_queries_keep_schema_version_sql_guards_and_output_bounds() -> Result<()>
{
    use cellar::sdk::consensus::RaftClass;
    let directory = tempfile::tempdir()?;
    let class = RaftClass::new("Counter", 1, [30; 32])?
        .query("Read", [31; 32], |context, _| {
            let value: u64 =
                context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Write", [32; 32], |context, _| {
            let value: u64 =
                context.query_row("UPDATE counter SET value=9 RETURNING value", [], |row| {
                    row.get(0)
                })?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Private", [33; 32], |context, _| {
            let value: u64 =
                context.query_row("SELECT value FROM __sys_hidden_test", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?;
    let schema = class.schema_digest();
    let mut host = Host::open_schema(directory.path(), false, 1, schema).await?;
    host.elect().await?;
    let bytes = images::image(None, "CREATE TABLE counter(value);INSERT INTO counter VALUES(7);CREATE TABLE __sys_hidden_test(value);INSERT INTO __sys_hidden_test VALUES(123)")?;
    let published = host.commit(&update_schema(&[], &bytes, 0, schema)?).await?;
    let cache = directory.path().join("registered-query-cache");
    DirectFileSystem.create_dir_all(&cache)?;
    let options = |max_result_bytes| -> Result<RootQueryOptions> {
        let execution = ExecutionLimits {
            max_result_bytes,
            ..Default::default()
        };
        Ok(RootQueryOptions {
            services: ReadServices {
                now_unix_millis: Arc::new(|| 1_700_000_000_000),
                fill_random: Arc::new(|bytes| bytes.fill(1)),
            },
            execution,
            cache_directory: cache.clone(),
            scratch: ScratchPool::new(ScratchLimits {
                max_bytes: 2 * EXTENT_BYTES,
                max_files: 2,
            })?,
            cache: CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
        })
    };
    let contract = class.contract("Read").unwrap();
    let output = query_registered_rooted(&published, &class, &contract, &[], options(1024)?)?;
    assert_eq!(output.result, 7_u64.to_be_bytes());
    assert_eq!(output.version.position, published.position());
    assert_eq!(&output.version.scope, published.scope());
    assert!(matches!(
        query_registered_rooted(&published, &class, &contract, &[], options(1)?),
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert!(query_registered_rooted(
        &published,
        &class,
        &class.contract("Write").unwrap(),
        &[],
        options(1024)?
    )
    .is_err());
    assert!(query_registered_rooted(
        &published,
        &class,
        &class.contract("Private").unwrap(),
        &[],
        options(1024)?
    )
    .is_err());
    let wrong =
        RaftClass::new("Counter", 1, [99; 32])?.query("Read", [31; 32], |_, _| Ok(vec![]))?;
    assert!(matches!(
        query_registered_rooted(
            &published,
            &wrong,
            &wrong.contract("Read").unwrap(),
            &[],
            options(1024)?
        ),
        Err(ExecutionError::Registration(_))
    ));
    assert_eq!(
        query_registered_rooted(&published, &class, &contract, &[], options(1024)?)?.result,
        7_u64.to_be_bytes()
    );
    drop(published);
    host.close().await
}
