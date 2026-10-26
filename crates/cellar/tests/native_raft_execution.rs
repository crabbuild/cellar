// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Command/apply integration uses real SQLite bytes, engines and RawNodes.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_storage_fault.rs"]
mod fault;
#[allow(dead_code)]
#[path = "support/raft_ltx_fixture.rs"]
mod images;

use anyhow::{bail, Context as _, Result};
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
        commitment: state.digest.into(),
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
    images: GenerationStore,
    gate: Arc<fault::Gate>,
    application_digest: [u8; 32],
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
        Self::open_schema(root, recover, process, signal, [9; 32]).await
    }

    async fn open_schema(
        root: &Path,
        recover: bool,
        process: u8,
        signal: Option<std::path::PathBuf>,
        application_digest: [u8; 32],
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
            images.clone(),
            application_digest,
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
            images,
            gate,
            application_digest,
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
async fn committed_replay_waits_for_pinned_image_capacity() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.images = GenerationStore::open(
        root.path().join("limited-images"),
        Arc::new(DirectFileSystem),
        GenerationLimits {
            max_generations: 2,
            ..Default::default()
        },
    )?;
    host.applier = ImageApplier::recover(
        host.log.group(&scope())?,
        host.images.clone(),
        host.application_digest,
        EnvelopeLimits::default(),
    )?;
    host.elect().await?;
    let mut bytes = Vec::new();
    let mut archive_pin = None;
    for sequence in 1..=3_u8 {
        let next = if sequence == 1 {
            initial()?
        } else {
            images::image(Some(&bytes), "UPDATE counter SET value=value+1")?
        };
        let cut = images::Cut::new(&bytes, &next, u64::from(sequence - 1), false, true);
        host.runtime
            .propose(&scope(), vec![], payload(&cut, sequence)?)
            .await?;
        let (ticket, entry) = host.next_apply().await?;
        if sequence == 3 {
            // Follower/recovery apply has no local command preview to promote.
            // The old archive pin plus the public head fill the image store.
            assert!(matches!(
                host.applier
                    .prepare(ticket.clone(), &entry, images::id(sequence)),
                Err(ImageApplyError::Images(
                    cellar_ltx::generation::GenerationError::Limit
                ))
            ));
            let status = host.runtime.status(&scope()).await?;
            assert!(status.committed >= ticket.position().index);
            assert!(status.applied.index < ticket.position().index && status.awaiting_apply);
            assert!(matches!(
                host.applier.publish(&ticket),
                Err(ImageApplyError::Unpublished)
            ));
            assert_eq!(images::read(&host.applier.current().unwrap().pin())?, bytes);
            drop(archive_pin.take());
        }
        let completion = host
            .applier
            .prepare(ticket.clone(), &entry, images::id(sequence))?;
        host.runtime.complete_apply(completion).await?;
        let published = host.applied(&ticket).await?;
        assert_eq!(images::read(&published.pin())?, next);
        if sequence == 1 {
            archive_pin = Some(published.pin());
        }
        bytes = next;
    }
    assert_eq!(host.counter()?, 9);
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
        assert_eq!(
            marker.image.unwrap().commitment,
            head.pin().state().digest.into()
        );
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

impl Host {
    async fn authority(&self) -> Result<CommandAuthority> {
        for _ in 0..3000 {
            let status = self.runtime.status(&scope()).await?;
            match CommandAuthority::from_status(self.log.group(&scope())?, &status) {
                Ok(authority) => return Ok(authority),
                Err(ExecutionError::Refused(CommandFailure::Unavailable)) => (),
                Err(error) => return Err(error.into()),
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        bail!("command authority did not become current")
    }

    async fn private(&self, root: &Path, limits: ExecutionLimits) -> Result<PrivateExecutor> {
        self.private_with_clock(root, limits, Arc::new(|| 10)).await
    }

    async fn private_with_clock(
        &self,
        root: &Path,
        limits: ExecutionLimits,
        clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Result<PrivateExecutor> {
        let host = cellar_ltx::LtxHost::new(
            move || clock(),
            |_| Ok(Duration::ZERO),
            |path| async move { std::fs::read(path) },
            |job| async move {
                job();
                Ok(())
            },
        )
        .with_filesystem(Arc::new(DirectFileSystem));
        Ok(PrivateExecutor::open(
            PrivateOptions {
                directory: root.join("private"),
                host,
                sqlite_vfs: None,
                images: self.images.clone(),
                query_services: cellar_ltx::generation::ReadServices {
                    now_unix_millis: Arc::new(|| 10),
                    fill_random: Arc::new(|bytes| bytes.fill(17)),
                },
                application_digest: self.application_digest,
                limits,
            },
            self.authority().await?,
            self.applier.current(),
        )?)
    }

    async fn commit_private(
        &mut self,
        executor: &mut PrivateExecutor,
        prepared: PreparedCommand,
        image: u8,
    ) -> Result<AppliedExecution> {
        executor.mark_submitted(prepared.proposal())?;
        self.runtime
            .propose(&scope(), prepared.context(), prepared.bytes().to_vec())
            .await?;
        let (ticket, entry) = self.next_apply().await?;
        let completion = self
            .applier
            .prepare(ticket.clone(), &entry, images::id(image))?;
        self.runtime.complete_apply(completion).await?;
        let published = self.applied(&ticket).await?;
        let applied = executor.finish(published)?;
        executor.refresh_authority(self.authority().await?)?;
        Ok(applied)
    }

    fn counter(&self) -> Result<i64> {
        let query = images::query(self.applier.current().unwrap().pin())?;
        Ok(query
            .connection()
            .query_row("SELECT value FROM counter", [], |r| r.get(0))?)
    }
}

fn attempt(sequence: u64, tag: u8) -> Result<CommandAttempt> {
    Ok(CommandAttempt {
        request: CommandRequest {
            id: CommandId::new(ClientId::new([1; 16])?, sequence)?,
            operation: "Counter.Change".into(),
            contract_digest: [9; 32],
            request_digest: [sequence as u8; 32],
            durability: Durability::Quorum,
            retry_until_ms: 1000,
        },
        proposal: ProposalId::new([tag; 16])?,
        preview: images::id(tag + 64),
        inputs: cellar::storage::CommandInputs {
            now_unix_millis: 10,
            random: [17; 32],
        },
    })
}

fn prepared(outcome: StageOutcome) -> Result<PreparedCommand> {
    match outcome {
        StageOutcome::Prepared(value) => Ok(value),
        StageOutcome::Duplicate(_) => bail!("expected a new command"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registered_command_uses_exact_image_receipt_and_read_only_query() -> Result<()> {
    use cellar::sdk::consensus::command_request_digest;
    use cellar::sdk::{OperationKind, RaftClass, RegistrationError};
    use cellar_logic::raft_command::{Cancellation, CommandState};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let class = RaftClass::new("Counter", 1, [22; 32])?
        .command("Increment", [23; 32], move |context, input| {
            called.fetch_add(1, Ordering::SeqCst);
            assert_eq!(context.identity(), &scope());
            assert_eq!(context.command().sequence(), 1);
            assert_eq!(context.inputs().now_unix_millis, 10);
            assert_eq!(context.inputs().random, [17; 32]);
            let delta = i64::from_be_bytes(input.try_into()?);
            context.sql().execute_batch("CREATE TABLE IF NOT EXISTS counter(value INTEGER); INSERT INTO counter SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM counter)")?;
            context.sql().execute("UPDATE counter SET value=value+?1", [delta])?;
            let value: i64 = context.sql().query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get", [24; 32], |context, _| {
            assert_eq!(context.identity(), &scope());
            assert!(context.version().position.index.get() > 0);
            for sql in [
                "UPDATE counter SET value=999 RETURNING value",
                "PRAGMA user_version=8",
                "ATTACH DATABASE ':memory:' AS other",
                "CREATE TABLE unauthorized(value INTEGER)",
                "SELECT count(*) FROM __sys_raft_receipts",
            ] {
                assert!(context.query_row(sql, [], |row| row.get::<_, i64>(0)).is_err(), "{sql}");
            }
            let value: i64 = context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?;
    let input = 7_i64.to_be_bytes();
    let registered_attempt = |tag: u8| -> Result<CommandAttempt> {
        let mut request = attempt(1, tag)?;
        request.request.operation = "Increment".into();
        request.request.contract_digest = [23; 32];
        request.request.request_digest = command_request_digest(&input);
        Ok(request)
    };
    let root = tempfile::tempdir()?;
    let mut host = Host::open_schema(root.path(), false, 1, None, class.schema_digest()).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let mut wrong = registered_attempt(1)?;
    wrong.request.contract_digest = [25; 32];
    assert!(matches!(
        executor.stage_registered(wrong, &class, &input),
        Err(ExecutionError::Registration(
            RegistrationError::ContractMismatch
        ))
    ));
    assert!(matches!(
        executor.stage_registered(registered_attempt(1)?, &class, b"changed"),
        Err(ExecutionError::Refused(CommandFailure::CommandConflict))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(executor.status(), ExecutionStatus::Ready);
    let mut query_as_command = registered_attempt(1)?;
    query_as_command.request.operation = "Get".into();
    query_as_command.request.contract_digest = [24; 32];
    assert!(matches!(
        executor.stage_registered(query_as_command, &class, &input),
        Err(ExecutionError::Registration(
            RegistrationError::KindMismatch
        ))
    ));

    let status = host.runtime.status(&scope()).await?;
    let mut core = CommandState::new(
        ReplicaProgress::checkpoint(
            status.replica.clone(),
            status.applied.index,
            LogIndex::new(0),
        )?,
        Durability::Quorum,
    )?;
    let authority = core
        .observe_leadership(status.leadership_observation(1)?)?
        .authority
        .unwrap();
    let request = registered_attempt(1)?;
    let identity = core.admit(
        &authority,
        request.request.id,
        request.proposal,
        request.request.durability,
    )?;
    let staged = prepared(executor.stage_registered(request, &class, &input)?)?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(host.applier.current().is_none());
    assert_eq!(core.finish(&identity).unwrap_err(), Refusal::Unproven);
    // Both producer owners fence uncertainty before the proposal await. Losing
    // this caller cannot permit another callback against the private image.
    core.mark_submitted(&identity)?;
    executor.mark_submitted(staged.proposal())?;
    let accepted = host
        .runtime
        .propose(&scope(), staged.context(), staged.bytes().to_vec())
        .await?;
    core.bind_entry(&identity, accepted.position, accepted.entry_digest)?;
    assert_eq!(core.cancel(&identity)?, Cancellation::Submitted);
    assert_eq!(core.discard_completed(&identity), Err(Refusal::Unproven));
    let (ticket, entry) = host.next_apply().await?;
    assert_eq!(ticket.entry_digest(), accepted.entry_digest);
    let status = host.runtime.status(&scope()).await?;
    let mut progress = core.progress().clone();
    progress.persist(status.persisted)?;
    progress.commit(status.committed)?;
    core.observe_progress(progress)?;
    let proof = |stage| {
        OutputProof::Raft(Box::new(RaftProof {
            command: identity.clone(),
            entry_digest: ticket.entry_digest(),
            position: ticket.position(),
            stage,
        }))
    };
    core.observe_proof(&proof(ProofStage::Committed))?;
    assert_eq!(
        core.observe_proof(&proof(ProofStage::Applied)),
        Err(Refusal::Unproven)
    );
    let completion = host
        .applier
        .prepare(ticket.clone(), &entry, images::id(1))?;
    assert!(host.applier.current().is_none());
    host.runtime.complete_apply(completion).await?;
    let published = host.applied(&ticket).await?;
    let status = host.runtime.status(&scope()).await?;
    core.observe_progress(ReplicaProgress::checkpoint(
        status.replica.clone(),
        status.applied.index,
        LogIndex::new(0),
    )?)?;
    core.observe_leadership(status.leadership_observation(2)?)?;
    core.observe_proof(&proof(ProofStage::Applied))?;
    core.observe_proof(&proof(ProofStage::Queryable))?;
    assert_eq!(core.finish(&identity).unwrap_err(), Refusal::Unproven);
    let AppliedExecution::Command(receipt) = executor.finish(published.clone())? else {
        bail!("expected command receipt")
    };
    core.verify_receipt(
        &identity,
        receipt.receipt().command(),
        receipt.position(),
        receipt.receipt().minimum_durability(),
    )?;
    let permit = core.finish(&identity)?;
    assert_eq!(permit.command(), receipt.receipt().command());
    assert_eq!(permit.version().position, accepted.position);
    assert_eq!(receipt.receipt().result(), input);
    let get = class.contract("Get").unwrap();
    let services = || cellar_ltx::generation::ReadServices {
        now_unix_millis: Arc::new(|| 10),
        fill_random: Arc::new(|bytes| bytes.fill(17)),
    };
    let result = query_registered(
        &published,
        &class,
        &get,
        &[],
        services(),
        ExecutionLimits::default(),
    )?;
    assert_eq!(result.result, input);
    assert_eq!(result.version, *permit.version());
    assert_eq!(published.application_digest(), class.schema_digest());
    let incompatible = RaftClass::new("Other", 1, [22; 32])?
        .query("Get", [24; 32], |_, _| bail!("incompatible query code ran"))?;
    assert!(matches!(
        query_registered(
            &published,
            &incompatible,
            &get,
            &[],
            services(),
            ExecutionLimits::default()
        ),
        Err(ExecutionError::Registration(
            RegistrationError::ContractMismatch
        ))
    ));
    let mut wrong = get.clone();
    wrong.kind = OperationKind::Command;
    assert!(matches!(
        query_registered(
            &published,
            &class,
            &wrong,
            &[],
            services(),
            ExecutionLimits::default()
        ),
        Err(ExecutionError::Registration(
            RegistrationError::KindMismatch
        ))
    ));
    wrong = get.clone();
    wrong.digest = [25; 32];
    assert!(matches!(
        query_registered(
            &published,
            &class,
            &wrong,
            &[],
            services(),
            ExecutionLimits::default()
        ),
        Err(ExecutionError::Registration(
            RegistrationError::ContractMismatch
        ))
    ));
    executor.refresh_authority(host.authority().await?)?;
    let request = registered_attempt(2)?;
    // A matching physical image cannot be relabeled with another schema and
    // used as this class's private base. Refusal precedes reconstruction.
    let wrong_schema = ImageApplier::recover(
        host.log.group(&scope())?,
        host.images.clone(),
        [26; 32],
        EnvelopeLimits::default(),
    )?;
    assert!(matches!(
        executor.rebuild(wrong_schema.current(), host.authority().await?),
        Err(ExecutionError::Invalid(_))
    ));
    drop(wrong_schema);
    assert_eq!(executor.status(), ExecutionStatus::Ready);
    let retry = core.admit(
        &authority,
        request.request.id,
        request.proposal,
        request.request.durability,
    )?;
    let StageOutcome::Duplicate(repeated) = executor.stage_registered(request, &class, &input)?
    else {
        bail!("retry re-executed")
    };
    core.recover_receipt(
        &retry,
        repeated.receipt().command(),
        CommittedVersion {
            scope: scope(),
            position: repeated.position(),
        },
        repeated.receipt().minimum_durability(),
    )?;
    assert_eq!(core.finish(&retry)?.version(), permit.version());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(executor);
    drop(published);
    host.close().await?;

    let mut recovered =
        Host::open_schema(root.path(), true, 2, None, class.schema_digest()).await?;
    recovered.elect().await?;
    let mut executor = recovered
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let StageOutcome::Duplicate(receipt) =
        executor.stage_registered(registered_attempt(3)?, &class, &input)?
    else {
        bail!("receipt lost on restart")
    };
    assert_eq!(receipt.receipt().result(), input);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let status = recovered.runtime.status(&scope()).await?;
    let mut core = CommandState::new(
        ReplicaProgress::checkpoint(
            status.replica.clone(),
            status.applied.index,
            LogIndex::new(0),
        )?,
        Durability::Quorum,
    )?;
    let current = core
        .observe_leadership(status.leadership_observation(1)?)?
        .authority
        .unwrap();
    let retry = core.admit(
        &current,
        receipt.receipt().command(),
        ProposalId::new([3; 16])?,
        Durability::Quorum,
    )?;
    core.recover_receipt(
        &retry,
        receipt.receipt().command(),
        CommittedVersion {
            scope: scope(),
            position: receipt.position(),
        },
        receipt.receipt().minimum_durability(),
    )?;
    let reconciled = core.finish(&retry)?;
    assert_eq!(reconciled.version().position, receipt.position());
    assert!(reconciled.version().position.term < current.term);
    assert_eq!(
        query_registered(
            &recovered.applier.current().unwrap(),
            &class,
            &get,
            &[],
            services(),
            ExecutionLimits::default()
        )?
        .result,
        input
    );
    drop(executor);
    recovered.close().await
}

fn initialize(transaction: &cellar::storage::CommandTransaction<'_>) -> Result<Vec<u8>> {
    transaction
        .execute_batch("CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7)")?;
    Ok(7_i64.to_be_bytes().to_vec())
}

fn increment(transaction: &cellar::storage::CommandTransaction<'_>) -> Result<Vec<u8>> {
    transaction.execute("UPDATE counter SET value=value+1", [])?;
    let value: i64 = transaction.query_row("SELECT value FROM counter", [], |r| r.get(0))?;
    Ok(value.to_be_bytes().to_vec())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_command_receipt() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
    host.commit_private(&mut executor, first, 1).await?;
    let mut request = attempt(2, 2)?;
    request.request.durability = Durability::S3;
    let second = prepared(executor.stage(request, increment)?)?;
    let result = host.commit_private(&mut executor, second, 2).await?;
    let AppliedExecution::Command(result) = result else {
        bail!("wrong applied kind")
    };
    assert_eq!(result.receipt().result(), 8_i64.to_be_bytes());
    assert_eq!(result.receipt().minimum_durability(), Durability::S3);
    drop(result);
    drop(executor);
    host.close().await?;
    // Lose the reply, then retry against a fresh engine/runtime/private image.
    let mut host = Host::open(root.path(), true).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let duplicate = executor.stage(attempt(2, 3)?, |_| panic!("duplicate callback executed"))?;
    let StageOutcome::Duplicate(result) = duplicate else {
        bail!("retry was executed again")
    };
    assert_eq!(result.receipt().result(), 8_i64.to_be_bytes());
    assert_eq!(result.receipt().minimum_durability(), Durability::S3);
    assert_eq!(host.counter()?, 8);
    let third = prepared(executor.stage(attempt(3, 4)?, increment)?)?;
    host.commit_private(&mut executor, third, 3).await?;
    assert_eq!(host.counter()?, 9);
    drop(executor);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicting_command_digest() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
    host.commit_private(&mut executor, first, 1).await?;
    for field in 0..3 {
        let mut conflict = attempt(1, 2)?;
        match field {
            0 => conflict.request.request_digest = [88; 32],
            1 => conflict.request.contract_digest = [88; 32],
            _ => conflict.request.operation = "Other.Operation".into(),
        };
        assert!(matches!(
            executor.stage(conflict, |_| panic!("conflicting command executed")),
            Err(ExecutionError::Refused(CommandFailure::CommandConflict))
        ));
        assert_eq!(executor.status(), ExecutionStatus::Ready);
        assert_eq!(host.counter()?, 7);
    }
    drop(executor);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_proposal_rebuild() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
    host.commit_private(&mut executor, first, 1).await?;
    let lost = prepared(executor.stage(attempt(2, 2)?, increment)?)?;
    executor.mark_submitted(lost.proposal())?;
    assert_eq!(executor.status(), ExecutionStatus::Submitted);
    assert!(matches!(
        executor.stage(attempt(3, 3)?, |_| panic!("second command ran")),
        Err(ExecutionError::Busy)
    ));
    assert!(matches!(
        executor.rebuild(host.applier.current(), host.authority().await?),
        Err(ExecutionError::Uncertain)
    ));
    assert_eq!(host.counter()?, 7);
    // No reply establishes whether admission happened. Lose the process; the
    // next leader establishes its barrier before rebuilding from the marker.
    drop(lost);
    drop(executor);
    host.close().await?;
    let mut host = Host::open(root.path(), true).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let retry = prepared(executor.stage(attempt(2, 3)?, increment)?)?;
    host.commit_private(&mut executor, retry, 2).await?;
    assert_eq!(host.counter()?, 8);
    let abandoned = prepared(executor.stage(attempt(3, 4)?, increment)?)?;
    assert!(matches!(
        executor.stage(attempt(4, 5)?, increment),
        Err(ExecutionError::Busy)
    ));
    // A staged proposal known never to have been submitted can be abandoned
    // under the same authority, but its surviving writable delta cannot be reused.
    executor.rebuild(host.applier.current(), host.authority().await?)?;
    drop(abandoned);
    let retry = prepared(executor.stage(attempt(3, 5)?, increment)?)?;
    host.commit_private(&mut executor, retry, 3).await?;
    assert_eq!(host.counter()?, 9);
    drop(executor);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_term_barrier_retires_uncertain_shadow_without_reusing_it() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
    host.commit_private(&mut executor, first, 1).await?;
    let pending = prepared(executor.stage(attempt(2, 2)?, increment)?)?;
    executor.mark_submitted(pending.proposal())?;
    let original = host.runtime.status(&scope()).await?;
    assert!(matches!(
        executor.discard_unsubmitted(),
        Err(ExecutionError::Uncertain)
    ));
    assert!(matches!(
        executor.retire_after_barrier(host.applier.current().as_ref(), original.applied),
        Err(ExecutionError::Uncertain)
    ));
    host.runtime.close().await?;
    let (runtime, events) = RaftRuntime::start(
        host.log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            election_seed: Some([12; 32]),
            ..Default::default()
        },
    )
    .await?;
    host.runtime = runtime;
    host.events = events;
    host.elect().await?;
    let current = host.runtime.status(&scope()).await?;
    assert!(current.term > original.term);
    let mut wrong = current.applied;
    wrong.index = LogIndex::new(wrong.index.get() + 1);
    assert!(matches!(
        executor.retire_after_barrier(host.applier.current().as_ref(), wrong),
        Err(ExecutionError::Uncertain)
    ));
    executor.retire_after_barrier(host.applier.current().as_ref(), current.applied)?;
    assert_eq!(executor.status(), ExecutionStatus::RebuildRequired);
    executor.close()?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let retry = prepared(executor.stage(attempt(2, 3)?, increment)?)?;
    host.commit_private(&mut executor, retry, 2).await?;
    assert_eq!(host.counter()?, 8);
    executor.close()?;
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_delta_atomic_refusal() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let mut limits = ExecutionLimits::default();
    limits.envelope.max_payload_bytes = 8192;
    let mut executor = host.private(root.path(), limits).await?;
    let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
    host.commit_private(&mut executor, first, 1).await?;
    let mut state = 7_u64;
    let mut noise = vec![0; 40_000];
    for byte in &mut noise {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    assert!(executor
        .stage(attempt(2, 2)?, |transaction| {
            transaction
                .execute_batch("CREATE TABLE payload(value BLOB);UPDATE counter SET value=999")?;
            transaction.execute("INSERT INTO payload VALUES(?1)", [noise])?;
            Ok(vec![99])
        })
        .is_err());
    assert_eq!(executor.status(), ExecutionStatus::RebuildRequired);
    assert_eq!(host.counter()?, 7);
    assert!(executor.refresh_authority(host.authority().await?).is_err());
    executor.rebuild(host.applier.current(), host.authority().await?)?;
    let small = prepared(executor.stage(attempt(2, 3)?, increment)?)?;
    host.commit_private(&mut executor, small, 2).await?;
    assert_eq!(host.counter()?, 8);
    let query = images::query(host.applier.current().unwrap().pin())?;
    assert_eq!(
        query.connection().query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='payload'",
            [],
            |r| r.get::<_, i64>(0)
        )?,
        0
    );
    drop(query);
    drop(executor);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receipt_expiry_floor() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let limits = ExecutionLimits {
        max_client_identities: 1,
        max_retained_receipts: 2,
        ..Default::default()
    };
    let mut executor = host.private(root.path(), limits).await?;
    let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
    host.commit_private(&mut executor, first, 1).await?;
    assert!(matches!(
        executor.stage(attempt(3, 2)?, |_| panic!("gap executed")),
        Err(ExecutionError::Refused(CommandFailure::SequenceGap {
            next: 2
        }))
    ));
    let second = prepared(executor.stage(attempt(2, 2)?, increment)?)?;
    host.commit_private(&mut executor, second, 2).await?;
    assert!(matches!(
        executor.stage(attempt(3, 3)?, increment),
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    let inputs = cellar::storage::CommandInputs {
        now_unix_millis: 999,
        random: [1; 32],
    };
    assert!(matches!(
        executor.stage_prune(
            ClientId::new([1; 16])?,
            2,
            ProposalId::new([3; 16])?,
            images::id(67),
            inputs
        ),
        Err(ExecutionError::RetentionNotExpired)
    ));
    executor.rebuild(host.applier.current(), host.authority().await?)?;
    assert!(matches!(
        executor.stage_prune(
            ClientId::new([1; 16])?,
            2,
            ProposalId::new([4; 16])?,
            images::id(68),
            cellar::storage::CommandInputs {
                now_unix_millis: 1000,
                ..inputs
            }
        ),
        Err(ExecutionError::RetentionNotExpired)
    ));
    executor.rebuild(host.applier.current(), host.authority().await?)?;
    let pruning = executor.stage_prune(
        ClientId::new([1; 16])?,
        2,
        ProposalId::new([4; 16])?,
        images::id(68),
        cellar::storage::CommandInputs {
            now_unix_millis: 1001,
            ..inputs
        },
    )?;
    assert!(matches!(
        host.commit_private(&mut executor, pruning, 3).await?,
        AppliedExecution::Pruned { through: 2, .. }
    ));
    assert!(matches!(
        executor.stage(attempt(1, 5)?, |_| panic!("expired command executed")),
        Err(ExecutionError::Refused(CommandFailure::CommandExpired))
    ));
    let mut other = attempt(1, 5)?;
    other.request.id = CommandId::new(ClientId::new([2; 16])?, 1)?;
    assert!(matches!(
        executor.stage(other, initialize),
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert!(matches!(
        executor.stage(attempt(4, 5)?, increment),
        Err(ExecutionError::Refused(CommandFailure::SequenceGap {
            next: 3
        }))
    ));
    let third = prepared(executor.stage(attempt(3, 5)?, increment)?)?;
    host.commit_private(&mut executor, third, 4).await?;
    assert_eq!(host.counter()?, 9);
    drop(executor);
    host.close().await?;
    let mut host = Host::open(root.path(), true).await?;
    host.elect().await?;
    let mut executor = host.private(root.path(), limits).await?;
    assert!(matches!(
        executor.stage(attempt(1, 6)?, increment),
        Err(ExecutionError::Refused(CommandFailure::CommandExpired))
    ));
    assert!(matches!(
        executor.stage(attempt(3, 6)?, |_| panic!("retained command executed"))?,
        StageOutcome::Duplicate(_)
    ));
    drop(executor);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn automatic_receipt_maintenance_commits_with_command_and_survives_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    let limits = ExecutionLimits {
        max_client_identities: 1,
        max_retained_receipts: 2,
        ..Default::default()
    };
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let mut executor = host.private(root.path(), limits).await?;
    let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
    host.commit_private(&mut executor, first, 1).await?;
    let second = prepared(executor.stage(attempt(2, 2)?, increment)?)?;
    host.commit_private(&mut executor, second, 2).await?;
    let request = |sequence, tag, now| -> Result<CommandAttempt> {
        let mut request = attempt(sequence, tag)?;
        request.inputs.now_unix_millis = now;
        request.request.retry_until_ms = 3000;
        Ok(request)
    };
    let published_receipts = |host: &Host| -> Result<(i64, Vec<u8>)> {
        let query = images::query(host.applier.current().unwrap().pin())?;
        let connection = query.connection();
        Ok((
            connection.query_row("SELECT count(*) FROM __sys_raft_receipts", [], |r| r.get(0))?,
            connection.query_row("SELECT expired_through FROM __sys_raft_clients", [], |r| {
                r.get(0)
            })?,
        ))
    };
    assert!(matches!(
        executor.stage(request(3, 3, 1000)?, |_| panic!(
            "live retention admitted callback"
        )),
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert!(executor
        .stage(request(3, 3, 1001)?, |transaction| {
            increment(transaction)?;
            bail!("business callback failed after private cleanup")
        })
        .is_err());
    assert_eq!(host.counter()?, 8);
    assert_eq!(
        published_receipts(&host)?,
        (2, 0_u64.to_be_bytes().to_vec())
    );
    executor.rebuild(host.applier.current(), host.authority().await?)?;
    let unsubmitted = prepared(executor.stage(request(3, 4, 1001)?, increment)?)?;
    assert_eq!(
        published_receipts(&host)?,
        (2, 0_u64.to_be_bytes().to_vec())
    );
    drop(unsubmitted);
    executor.rebuild(host.applier.current(), host.authority().await?)?;
    assert!(matches!(
        executor.stage(attempt(1, 5)?, |_| panic!("unsubmitted cleanup escaped"))?,
        StageOutcome::Duplicate(_)
    ));
    let third = prepared(executor.stage(request(3, 6, 1001)?, increment)?)?;
    host.commit_private(&mut executor, third, 3).await?;
    assert_eq!(host.counter()?, 9);
    assert_eq!(
        published_receipts(&host)?,
        (2, 1_u64.to_be_bytes().to_vec())
    );
    drop(executor);
    host.close().await?;

    let mut host = Host::open(root.path(), true).await?;
    host.elect().await?;
    let mut executor = host.private(root.path(), limits).await?;
    assert!(matches!(
        executor.stage(attempt(1, 7)?, |_| panic!("expired sequence replayed")),
        Err(ExecutionError::Refused(CommandFailure::CommandExpired))
    ));
    let StageOutcome::Duplicate(receipt) =
        executor.stage(attempt(2, 8)?, |_| panic!("retained receipt replayed"))?
    else {
        bail!("latest pre-maintenance acknowledgement lost")
    };
    assert_eq!(receipt.receipt().result(), 8_i64.to_be_bytes());
    drop(receipt);
    let fourth = prepared(executor.stage(request(4, 9, 1002)?, increment)?)?;
    host.commit_private(&mut executor, fourth, 4).await?;
    assert_eq!(host.counter()?, 10);
    assert_eq!(
        published_receipts(&host)?,
        (2, 2_u64.to_be_bytes().to_vec())
    );
    drop(executor);
    host.close().await
}

const BEFORE_PRIVATE_COMMIT: u8 = 110;
const AFTER_PRIVATE_COMMIT: u8 = 111;
const AFTER_PRIVATE_FREEZE: u8 = 112;
const BEFORE_PRIVATE_SUBMISSION: u8 = 113;

const AFTER_PRIVATE_RAFT_COMMIT: u8 = 114;

async fn kill_private_child(root: &Path, mode: u8) -> Result<()> {
    struct Child(std::process::Child);

    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let log = std::fs::File::create(root.join("private-child.log"))?;
    let mut child = Child(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "private_crash_process",
                "--nocapture",
            ])
            .env("CELLAR_PRIVATE_CRASH_ROOT", root)
            .env("CELLAR_PRIVATE_CRASH_MODE", mode.to_string())
            .stdout(std::process::Stdio::from(log.try_clone()?))
            .stderr(std::process::Stdio::from(log))
            .spawn()?,
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !root.join("reached").exists() {
        if child.0.try_wait()?.is_some() || std::time::Instant::now() > deadline {
            bail!(
                "missed private cut {mode}: {}",
                std::fs::read_to_string(root.join("private-child.log"))?
            );
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // Observe the actual private transaction boundary independently. These
    // bytes are deliberately never offered to the application's query path.
    let private = rusqlite::Connection::open_with_flags(
        root.join("private/working.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let value: i64 = private.query_row("SELECT value FROM counter", [], |r| r.get(0))?;
    let receipts: i64 =
        private.query_row("SELECT count(*) FROM __sys_raft_receipts", [], |r| r.get(0))?;
    assert_eq!(value, if mode == BEFORE_PRIVATE_COMMIT { 7 } else { 8 });
    assert_eq!(receipts, if mode == BEFORE_PRIVATE_COMMIT { 1 } else { 2 });
    drop(private);
    child.0.kill()?;
    let _ = child.0.wait()?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_commit_not_visible() -> Result<()> {
    for mode in [
        BEFORE_PRIVATE_COMMIT,
        AFTER_PRIVATE_COMMIT,
        AFTER_PRIVATE_FREEZE,
        BEFORE_PRIVATE_SUBMISSION,
    ] {
        let root = tempfile::tempdir()?;
        let mut host = Host::open(root.path(), false).await?;
        host.elect().await?;
        let mut executor = host
            .private(root.path(), ExecutionLimits::default())
            .await?;
        let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
        host.commit_private(&mut executor, first, 1).await?;
        drop(executor);
        host.close().await?;

        kill_private_child(root.path(), mode).await?;
        let mut recovered = Host::open_process(root.path(), true, 4, None).await?;
        assert_eq!(recovered.counter()?, 7);
        recovered.elect().await?;
        let mut executor = recovered
            .private(root.path(), ExecutionLimits::default())
            .await?;
        let retry = prepared(executor.stage(attempt(2, 3)?, increment)?)?;
        recovered.commit_private(&mut executor, retry, 2).await?;
        assert_eq!(recovered.counter()?, 8);
        drop(executor);
        recovered.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "child entry point invoked and killed by the private command crash tests"]
async fn private_crash_process() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let root = std::path::PathBuf::from(std::env::var("CELLAR_PRIVATE_CRASH_ROOT")?);
    let mode: u8 = std::env::var("CELLAR_PRIVATE_CRASH_MODE")?.parse()?;
    let mut host = Host::open_process(&root, true, 3, None).await?;
    host.elect().await?;
    let trigger = Arc::new(AtomicBool::new(false));
    let clock_trigger = trigger.clone();
    let clock_root = root.clone();
    let clock = Arc::new(move || {
        if mode == AFTER_PRIVATE_COMMIT && clock_trigger.swap(false, Ordering::AcqRel) {
            pause_child(&clock_root).unwrap();
        }
        10
    });
    let mut executor = host
        .private_with_clock(&root, ExecutionLimits::default(), clock)
        .await?;
    let staged = prepared(executor.stage(attempt(2, 2)?, |transaction| {
        let result = increment(transaction)?;
        if mode == BEFORE_PRIVATE_COMMIT {
            pause_child(&root)?;
        }
        trigger.store(true, Ordering::Release);
        Ok(result)
    })?)?;
    if mode == BEFORE_PRIVATE_SUBMISSION || mode == AFTER_PRIVATE_RAFT_COMMIT {
        executor.mark_submitted(staged.proposal())?;
    }
    if mode == AFTER_PRIVATE_RAFT_COMMIT {
        host.runtime
            .propose(&scope(), staged.context(), staged.bytes().to_vec())
            .await?;
        let _ = host.next_apply().await?;
        pause_child(&root)?;
    }
    assert!(mode == AFTER_PRIVATE_FREEZE || mode == BEFORE_PRIVATE_SUBMISSION);
    pause_child(&root)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_transaction_restricts_sql_and_captures_inputs() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let first = prepared(executor.stage(attempt(1, 1)?, |transaction| {
        for sql in [
            "COMMIT",
            "ROLLBACK",
            "BEGIN",
            "SAVEPOINT escape",
            "ATTACH ':memory:' AS other",
            "CREATE TEMP TABLE escape(value)",
            "PRAGMA writable_schema=ON",
            "VACUUM",
            "DROP TABLE __sys_raft_receipts",
            "CREATE TABLE __sys_intruder(value)",
        ] {
            assert!(transaction.execute_batch(sql).is_err(), "accepted {sql}");
        }
        for sql in [
            "SELECT random()",
            "SELECT current_timestamp",
            "SELECT count(*) FROM __sys_raft_receipts",
        ] {
            assert!(
                transaction
                    .query_row(sql, [], |r| r.get::<_, rusqlite::types::Value>(0))
                    .is_err(),
                "accepted {sql}"
            );
        }
        assert_eq!(transaction.inputs().now_unix_millis, 10);
        assert_eq!(transaction.inputs().random, [17; 32]);
        initialize(transaction)
    })?)?;
    host.commit_private(&mut executor, first, 1).await?;
    assert_eq!(host.counter()?, 7);
    drop(executor);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn submitted_command_recovery() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
    host.commit_private(&mut executor, first, 1).await?;
    drop(executor);
    host.close().await?;
    kill_private_child(root.path(), AFTER_PRIVATE_RAFT_COMMIT).await?;
    let mut recovered = Host::open_process(root.path(), true, 4, None).await?;
    assert_eq!(recovered.counter()?, 7);
    let (ticket, entry) = recovered.next_apply().await?;
    let completion = recovered
        .applier
        .prepare(ticket.clone(), &entry, images::id(2))?;
    recovered.runtime.complete_apply(completion).await?;
    drop(recovered.applied(&ticket).await?);
    assert_eq!(recovered.counter()?, 8);
    recovered.elect().await?;
    let mut executor = recovered
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let retry = executor.stage(attempt(2, 3)?, |_| {
        panic!("committed command executed again")
    })?;
    let StageOutcome::Duplicate(receipt) = retry else {
        bail!("committed receipt lost during replay")
    };
    assert_eq!(receipt.receipt().result(), 8_i64.to_be_bytes());
    drop(executor);
    recovered.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_failure_and_result_limit_roll_back() -> Result<()> {
    for mode in 0..4 {
        let root = tempfile::tempdir()?;
        let mut host = Host::open(root.path(), false).await?;
        host.elect().await?;
        let mut executor = host
            .private(root.path(), ExecutionLimits::default())
            .await?;
        let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
        host.commit_private(&mut executor, first, 1).await?;
        let result = executor.stage(attempt(2, 2)?, |transaction| {
            transaction.execute("UPDATE counter SET value=999", [])?;
            match mode {
                0 => bail!("callback failed"),
                1 => panic!("callback panicked"),
                3 => {
                    transaction.execute("CREATE TABLE quota(value BLOB)", [])?;
                    // Each value stays within ordinary SQL limits. Total
                    // growth must hit SQLite's independently configured quota.
                    for _ in 0..128 {
                        transaction.execute("INSERT INTO quota VALUES(zeroblob(1048576))", [])?;
                    }
                    bail!("growth unexpectedly passed the image quota")
                }
                _ => Ok(vec![0; 64 * 1024 + 1]),
            }
        });
        if mode == 2 {
            assert!(matches!(
                result,
                Err(ExecutionError::Refused(CommandFailure::Overloaded))
            ));
        } else if mode == 3 {
            assert!(matches!(
                result,
                Err(ExecutionError::Refused(CommandFailure::StorageFull))
            ));
        } else {
            assert!(result.is_err());
        }
        assert_eq!(executor.status(), ExecutionStatus::RebuildRequired);
        assert_eq!(host.counter()?, 7);
        executor.rebuild(host.applier.current(), host.authority().await?)?;
        let retry = prepared(executor.stage(attempt(2, 3)?, increment)?)?;
        host.commit_private(&mut executor, retry, 2).await?;
        assert_eq!(host.counter()?, 8);
        drop(executor);
        host.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receipt_reserve_preserves_cleanup_retry_and_page_reuse_at_application_capacity(
) -> Result<()> {
    fn grow(transaction: &cellar::storage::CommandTransaction<'_>) -> Result<Vec<u8>> {
        transaction.execute("INSERT INTO items VALUES(zeroblob(131072))", [])?;
        increment(transaction)
    }
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let limits = ExecutionLimits {
        max_image_bytes: 4 * 1024 * 1024,
        receipt_reserve_bytes: 256 * 1024,
        ..Default::default()
    };
    let mut executor = host.private(root.path(), limits).await?;
    let initial = prepared(executor.stage(attempt(1, 1)?, |transaction| {
        initialize(transaction)?;
        transaction.execute_batch(
            "CREATE TABLE items(value BLOB);
            CREATE TABLE tiny(id INTEGER PRIMARY KEY, value TEXT);
            INSERT INTO tiny VALUES(1,'first'),(2,'second');
            CREATE TABLE dbstat(name TEXT, pgsize INTEGER, aggregate INTEGER);
            INSERT INTO dbstat VALUES('__sys_raft_receipts',0,1);",
        )?;
        assert!(transaction
            .execute_batch("DROP TABLE temp.__sys_raft_capacity")
            .is_err());
        Ok(7_i64.to_be_bytes().to_vec())
    })?)?;
    host.commit_private(&mut executor, initial, 1).await?;
    let mut expected = 7_i64;
    let mut refused = None;
    for sequence in 2..=64 {
        match executor.stage(attempt(sequence, sequence as u8)?, grow) {
            Ok(outcome) => {
                host.commit_private(&mut executor, prepared(outcome)?, sequence as u8)
                    .await?;
                expected += 1;
            }
            Err(ExecutionError::Refused(CommandFailure::StorageFull)) => {
                refused = Some(sequence);
                break;
            }
            Err(error) => bail!("unexpected quota failure: {error}"),
        }
    }
    let sequence = refused.context("growth never reached the application allowance")?;
    assert_eq!(host.counter()?, expected);
    // The application allowance is exhausted before the physical image quota:
    // receipt/control writes retain their independent, unused space.
    assert!(host.applier.current().unwrap().pin().state().length < limits.max_image_bytes);
    executor.rebuild(host.applier.current(), host.authority().await?)?;
    let duplicate = executor.stage(attempt(sequence - 1, 80)?, |_| {
        panic!("durable receipt callback repeated")
    })?;
    let StageOutcome::Duplicate(duplicate) = duplicate else {
        bail!("lost retained receipt")
    };
    assert_eq!(duplicate.receipt().result(), expected.to_be_bytes());
    let cleanup = |number, tag| -> Result<CommandAttempt> {
        let mut attempt = attempt(number, tag)?;
        attempt.request.id = CommandId::new(ClientId::new([2; 16])?, number)?;
        Ok(attempt)
    };
    let tiny = prepared(executor.stage(cleanup(1, 81)?, |transaction| {
        // Another tiny row remains on this leaf: no application page is freed.
        transaction.execute("DELETE FROM tiny WHERE id=1", [])?;
        increment(transaction)
    })?)?;
    host.commit_private(&mut executor, tiny, 81).await?;
    expected += 1;
    assert_eq!(host.counter()?, expected);
    assert!(matches!(
        executor.stage(attempt(sequence, 82)?, grow),
        Err(ExecutionError::Refused(CommandFailure::StorageFull))
    ));
    executor.rebuild(host.applier.current(), host.authority().await?)?;
    let freed = prepared(executor.stage(cleanup(2, 83)?, |transaction| {
        transaction.execute(
            "DELETE FROM items WHERE rowid=(SELECT max(rowid) FROM items)",
            [],
        )?;
        increment(transaction)
    })?)?;
    host.commit_private(&mut executor, freed, 83).await?;
    let reused = prepared(executor.stage(attempt(sequence, 84)?, grow)?)?;
    host.commit_private(&mut executor, reused, 84).await?;
    assert_eq!(host.counter()?, expected + 2);
    let query = images::query(host.applier.current().unwrap().pin())?;
    assert_eq!(
        query
            .connection()
            .query_row("SELECT count(*) FROM tiny", [], |r| r.get::<_, u64>(0))?,
        1
    );
    assert_eq!(
        query
            .connection()
            .query_row("SELECT count(*) FROM items", [], |r| r.get::<_, u64>(0))?,
        sequence - 2
    );
    drop(query);
    drop(executor);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receipt_storage_admission_refuses_without_acknowledging_private_writes() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let limits = ExecutionLimits {
        receipt_reserve_bytes: 512,
        ..Default::default()
    };
    let mut executor = host.private(root.path(), limits).await?;
    assert!(matches!(
        executor.stage(attempt(1, 1)?, initialize),
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert!(host.applier.current().is_none());
    assert_eq!(executor.status(), ExecutionStatus::RebuildRequired);
    drop(executor);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_checkpoint_capture_covers_control_writes() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
    host.commit_private(&mut executor, first, 1).await?;
    // Exceed the real capture engine's default 1000-page checkpoint threshold.
    // Its sealing/control transaction must join the same proposal's complete cut.
    let second = prepared(executor.stage(attempt(2, 2)?, |transaction| {
        transaction.execute_batch("CREATE TABLE payload(value BLOB)")?;
        for _ in 0..1400 {
            transaction.execute("INSERT INTO payload VALUES(zeroblob(4096))", [])?;
        }
        increment(transaction)
    })?)?;
    let update = CellUpdate::decode(second.bytes(), EnvelopeLimits::default())?;
    assert!(
        update.files.len() > 1,
        "fixture did not exercise checkpoint/control capture"
    );
    host.commit_private(&mut executor, second, 2).await?;
    assert_eq!(host.counter()?, 8);
    let query = images::query(host.applier.current().unwrap().pin())?;
    assert_eq!(
        query
            .connection()
            .query_row("SELECT sum(length(value)) FROM payload", [], |r| r
                .get::<_, i64>(0))?,
        1400 * 4096
    );
    drop(query);
    drop(executor);
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_recovery_discards_corrupt_working_state() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut host = Host::open(root.path(), false).await?;
    host.elect().await?;
    let mut executor = host
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let first = prepared(executor.stage(attempt(1, 1)?, initialize)?)?;
    host.commit_private(&mut executor, first, 1).await?;
    let _unsubmitted = prepared(executor.stage(attempt(2, 2)?, increment)?)?;
    drop(executor);
    host.close().await?;

    // Simulate loss or corruption of every disposable SQLite artifact. The
    // durable image and log still identify the sole admitted recovery state.
    let private = root.path().join("private");
    for suffix in ["", "-wal", "-shm", "-journal"] {
        std::fs::write(
            private.join(format!("working.sqlite{suffix}")),
            b"torn private state",
        )?;
    }
    let metadata = cellar_ltx::Db::meta_path_for_path(private.join("working.sqlite"));
    std::fs::write(metadata.join("ltx/0/incomplete.tmp"), b"torn capture")?;

    let mut recovered = Host::open(root.path(), true).await?;
    assert_eq!(recovered.counter()?, 7);
    recovered.elect().await?;
    let mut executor = recovered
        .private(root.path(), ExecutionLimits::default())
        .await?;
    let duplicate = executor.stage(attempt(1, 3)?, |_| panic!("committed callback reran"))?;
    let StageOutcome::Duplicate(receipt) = duplicate else {
        bail!("committed receipt was lost with private state")
    };
    assert_eq!(receipt.receipt().result(), 7_i64.to_be_bytes());
    let retry = prepared(executor.stage(attempt(2, 4)?, increment)?)?;
    recovered.commit_private(&mut executor, retry, 2).await?;
    assert_eq!(recovered.counter()?, 8);
    drop(executor);
    recovered.close().await
}
