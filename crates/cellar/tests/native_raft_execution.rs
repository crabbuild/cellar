//! Command/apply integration uses real SQLite bytes, engines and RawNodes.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_storage_fault.rs"]
mod fault;
#[allow(dead_code)]
#[path = "support/raft_ltx_fixture.rs"]
mod images;

use anyhow::{bail, Result};
use cellar::application::consensus::verification::*;
use cellar::application::consensus::{CellUpdate, EnvelopeLimits, LtxImage, LtxUpdate};
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::*;
use cellar_ltx::generation::{GenerationLimits, GenerationStore};
use cellar_ltx::DirectFileSystem;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn scope() -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:7".into(),
        group: GroupId::new([7; 16]).unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    }
}

fn payload(cut: &images::Cut, proposal: u8) -> Result<Vec<u8>> {
    let position = |state: cellar_ltx::apply::ImageState| LtxImage {
        epoch: state.epoch,
        txid: state.txid.0,
        checksum: state.checksum,
        image_digest: state.digest,
    };
    Ok(CellUpdate {
        scope: scope(),
        application_digest: [9; 32],
        proposal: ProposalId::new([proposal; 16])?,
        base: position(cut.base),
        result: position(cut.result),
        files: vec![LtxUpdate {
            min_txid: cut.base.txid.0 + 1,
            max_txid: cut.result.txid.0,
            page_count: cut.pages.len() as u64,
            decoded_bytes: cut.pages.len() as u64 * 512,
            bytes: cut.bytes.clone(),
        }],
    }
    .encode(EnvelopeLimits::default())?)
}

struct Host {
    domain: ExecutionDomain,
    log: ReplicaLog,
    runtime: RaftRuntime,
    events: RuntimeEvents,
    applier: ImageApplier,
    gate: Arc<fault::Gate>,
}

impl Host {
    async fn open(root: &Path, recover: bool) -> Result<Self> {
        Self::open_process(root, recover, if recover { 2 } else { 1 }, None).await
    }

    async fn open_process(
        root: &Path,
        recover: bool,
        process: u8,
        signal: Option<std::path::PathBuf>,
    ) -> Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let gate = fault::Gate::new(signal);
        let options = ReplicaLogOptions::new(
            root.join("raft"),
            NodeId::new([1; 16])?,
            DiskLineage::new([101; 16])?,
            ProcessGeneration::new([process; 16])?,
            if recover {
                LogOpenMode::Resume
            } else {
                LogOpenMode::Create
            },
        );
        let log = domain
            .run(ReplicaLog::open(
                options,
                Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
            ))
            .await??;
        if !recover {
            log.create_group(GroupBootstrap {
                scope: scope(),
                member: MemberId::new(1)?,
                birth_digest: [7; 32],
                voters: vec![MemberId::new(1)?],
                learners: vec![],
            })
            .await?;
        }
        let images = GenerationStore::open(
            root.join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let applier = ImageApplier::recover(
            log.group(&scope())?,
            images,
            [9; 32],
            EnvelopeLimits::default(),
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
            applier,
            gate,
        })
    }

    async fn elect(&mut self) -> Result<()> {
        self.runtime.campaign(&scope()).await?;
        for _ in 0..3000 {
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
        bail!("single-voter leader did not establish its barrier")
    }

    async fn next_apply(&mut self) -> Result<(ApplyTicket, raft::prelude::Entry)> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match self
                    .events
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("runtime event stream closed"))?
                    .kind
                {
                    RuntimeEventKind::Apply { ticket, entry } => return Ok((ticket, entry)),
                    RuntimeEventKind::Failed { error, .. } => {
                        return Err(anyhow::Error::from(error))
                    }
                    _ => (),
                }
            }
        })
        .await?
    }

    async fn applied(&mut self, expected: &ApplyTicket) -> Result<PublishedImage> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match self
                    .events
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("runtime event stream closed"))?
                    .kind
                {
                    RuntimeEventKind::Applied { ticket, .. } if ticket == *expected => {
                        return Ok(self.applier.publish(&ticket)?)
                    }
                    RuntimeEventKind::Failed { error, .. } => {
                        return Err(anyhow::Error::from(error))
                    }
                    _ => (),
                }
            }
        })
        .await?
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

fn initial() -> Result<Vec<u8>> {
    images::image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)",
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn image_publication_waits_for_engine_marker() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let expected = initial()?;
    let cut = images::Cut::new(&[], &expected, 0, false, true);
    host.runtime
        .propose(&scope(), vec![], payload(&cut, 1)?)
        .await?;
    let (ticket, entry) = host.next_apply().await?;
    let completion = host
        .applier
        .prepare(ticket.clone(), &entry, images::id(1))?;
    assert!(host.applier.current().is_none());
    assert!(matches!(
        host.applier.publish(&ticket),
        Err(ImageApplyError::Unpublished)
    ));
    assert!(matches!(
        host.applier.prepare(ticket.clone(), &entry, images::id(2)),
        Err(ImageApplyError::Busy)
    ));
    let release = host.gate.arm(fault::HOLD_SYNC);
    host.runtime.complete_apply(completion).await?;
    tokio::time::timeout(Duration::from_secs(5), host.gate.entered.notified()).await?;
    assert!(host.log.group(&scope())?.applied()?.image.is_none());
    assert!(host.applier.current().is_none());
    assert!(matches!(
        host.applier.publish(&ticket),
        Err(ImageApplyError::Unpublished)
    ));
    drop(release);
    let published = host.applied(&ticket).await?;
    assert_eq!(published.position(), ticket.position());
    assert_eq!(images::read(&published.pin())?, expected);
    let query = images::query(published.pin())?;
    assert_eq!(
        query
            .connection()
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))?,
        7
    );
    drop(query);
    drop(published);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_recovery_follows_engine_marker() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let first = initial()?;
    let birth = images::Cut::new(&[], &first, 0, false, true);
    host.runtime
        .propose(&scope(), vec![], payload(&birth, 1)?)
        .await?;
    let (ticket, entry) = host.next_apply().await?;
    let completion = host
        .applier
        .prepare(ticket.clone(), &entry, images::id(1))?;
    host.runtime.complete_apply(completion).await?;
    drop(host.applied(&ticket).await?);
    let next = images::image(Some(&first), "UPDATE counter SET value=8")?;
    let cut = images::Cut::new(&first, &next, 1, false, true);
    host.runtime
        .propose(&scope(), vec![], payload(&cut, 2)?)
        .await?;
    let (ticket, entry) = host.next_apply().await?;
    let orphan = host
        .applier
        .prepare(ticket.clone(), &entry, images::id(2))?;
    drop(orphan);
    assert_eq!(images::read(&host.applier.current().unwrap().pin())?, first);
    host.close().await?;
    let mut recovered = Host::open(root.path(), true).await?;
    assert_eq!(
        images::read(&recovered.applier.current().unwrap().pin())?,
        first
    );
    let (replay, entry) = recovered.next_apply().await?;
    assert_eq!(replay.position(), ticket.position());
    let completion = recovered
        .applier
        .prepare(replay.clone(), &entry, images::id(3))?;
    recovered.runtime.complete_apply(completion).await?;
    let published = recovered.applied(&replay).await?;
    assert_eq!(images::read(&published.pin())?, next);
    drop(published);
    recovered.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_entry_and_schema_identity_are_bound() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let expected = initial()?;
    let cut = images::Cut::new(&[], &expected, 0, false, true);
    let mut update = CellUpdate::decode(&payload(&cut, 1)?, EnvelopeLimits::default())?;
    update.application_digest = [8; 32];
    host.runtime
        .propose(&scope(), vec![], update.encode(EnvelopeLimits::default())?)
        .await?;
    let (ticket, entry) = host.next_apply().await?;
    let mut changed = entry.clone();
    changed.context = vec![1].into();
    assert!(matches!(
        host.applier
            .prepare(ticket.clone(), &changed, images::id(1)),
        Err(ImageApplyError::Invalid("apply ticket/entry identity"))
    ));
    assert!(matches!(
        host.applier.prepare(ticket, &entry, images::id(1)),
        Err(ImageApplyError::Invalid("group/schema identity"))
    ));
    assert!(host.applier.current().is_none());
    assert!(host.log.group(&scope())?.applied()?.image.is_none());
    host.close().await
}

const AFTER_IMAGE: u8 = 100;
const AFTER_PUBLICATION: u8 = 101;

fn pause_child(root: &Path) -> Result<()> {
    use std::io::Write;
    let mut file = std::fs::File::create(root.join("reached"))?;
    file.write_all(b"reached")?;
    file.sync_all()?;
    loop {
        std::thread::park();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_marker_crash_cut() -> Result<()> {
    for mode in [
        AFTER_IMAGE,
        fault::CRASH_BEFORE_SYNC,
        fault::CRASH_AFTER_SYNC,
        AFTER_PUBLICATION,
    ] {
        let root = tempfile::tempdir()?;
        let mut host = Host::open(root.path(), false).await?;
        host.elect().await?;
        let first = initial()?;
        let birth = images::Cut::new(&[], &first, 0, false, true);
        host.runtime
            .propose(&scope(), vec![], payload(&birth, 1)?)
            .await?;
        let (ticket, entry) = host.next_apply().await?;
        let completion = host
            .applier
            .prepare(ticket.clone(), &entry, images::id(1))?;
        host.runtime.complete_apply(completion).await?;
        drop(host.applied(&ticket).await?);
        host.close().await?;
        let next = images::image(Some(&first), "UPDATE counter SET value=8")?;
        std::fs::write(root.path().join("next.sqlite"), &next)?;

        struct Child(std::process::Child);

        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let log = std::fs::File::create(root.path().join("child.log"))?;
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args(["--ignored", "--exact", "apply_crash_process", "--nocapture"])
                .env("CELLAR_APPLY_CRASH_ROOT", root.path())
                .env("CELLAR_APPLY_CRASH_MODE", mode.to_string())
                .stdout(std::process::Stdio::from(log.try_clone()?))
                .stderr(std::process::Stdio::from(log))
                .spawn()?,
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !root.path().join("reached").exists() {
            if child.0.try_wait()?.is_some() || std::time::Instant::now() > deadline {
                bail!(
                    "missed apply cut {mode}: {}",
                    std::fs::read_to_string(root.path().join("child.log"))?
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        child.0.kill()?;
        let _ = child.0.wait()?;
        let recovered = Host::open_process(root.path(), true, 4, None).await?;
        let head = recovered.applier.current().unwrap();
        let txid = head.pin().state().txid.0;
        match mode {
            AFTER_IMAGE => assert_eq!(txid, 1),
            fault::CRASH_BEFORE_SYNC => assert!(txid == 1 || txid == 2),
            _ => assert_eq!(txid, 2),
        }
        assert_eq!(
            images::read(&head.pin())?,
            if txid == 1 { first } else { next }
        );
        let marker = recovered.log.group(&scope())?.applied()?;
        assert_eq!(marker.position, head.position());
        assert_eq!(marker.image.unwrap().digest, head.pin().state().digest);
        drop(head);
        recovered.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "child entry point invoked and killed by apply_marker_crash_cut"]
async fn apply_crash_process() -> Result<()> {
    let root = std::path::PathBuf::from(std::env::var("CELLAR_APPLY_CRASH_ROOT")?);
    let mode: u8 = std::env::var("CELLAR_APPLY_CRASH_MODE")?.parse()?;
    let mut host = Host::open_process(&root, true, 3, Some(root.join("reached"))).await?;
    host.elect().await?;
    let first = images::read(&host.applier.current().unwrap().pin())?;
    let next = std::fs::read(root.join("next.sqlite"))?;
    let cut = images::Cut::new(&first, &next, 1, false, true);
    host.runtime
        .propose(&scope(), vec![], payload(&cut, 2)?)
        .await?;
    let (ticket, entry) = host.next_apply().await?;
    let completion = host
        .applier
        .prepare(ticket.clone(), &entry, images::id(2))?;
    if mode == AFTER_IMAGE {
        pause_child(&root)?;
    }
    let _release = if mode == fault::CRASH_BEFORE_SYNC || mode == fault::CRASH_AFTER_SYNC {
        Some(host.gate.arm(mode))
    } else {
        None
    };
    host.runtime.complete_apply(completion).await?;
    drop(host.applied(&ticket).await?);
    assert_eq!(mode, AFTER_PUBLICATION);
    pause_child(&root)
}
