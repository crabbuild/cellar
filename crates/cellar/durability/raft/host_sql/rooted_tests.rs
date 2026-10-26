// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real runtime events through the joined SQL worker, with an extent image.
//! Public node selection, remote transfer and OS-process failure are separate.
//! The test driver owns the real clock/filesystem outside the worker domain.
#![allow(clippy::disallowed_methods)]

use super::*;
use crate::application::consensus::{command_request_digest, verification::*, CommittedVersion};
use crate::asyncrt::ExecutionDomain;
use cellar_logic::consensus::*;
use cellar_ltx::{
    internal::{CacheLimits, NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem,
};
use std::{path::Path, sync::atomic::AtomicUsize};

struct Host {
    pool: Pool,
    domain: ExecutionDomain,
    token: crate::asyncrt::DomainToken,
    log: ReplicaLog,
    runtime: RaftRuntime,
    events: RuntimeEvents,
    store: NodeExtentStore,
    class: Arc<RaftClass>,
    pins: Arc<Budget>,
}

fn scope() -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:7".into(),
        group: GroupId::new([7; 16]).unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    }
}

impl Host {
    async fn open(root: &Path, class: Arc<RaftClass>, resume: bool) -> anyhow::Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let token = domain
            .run(async { crate::asyncrt::services().domain_token().unwrap() })
            .await?;
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    root.join("raft"),
                    NodeId::new([1; 16])?,
                    DiskLineage::new([101; 16])?,
                    ProcessGeneration::new([if resume { 2 } else { 1 }; 16])?,
                    if resume {
                        LogOpenMode::Resume
                    } else {
                        LogOpenMode::Create
                    },
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        if !resume {
            log.create_group(GroupBootstrap {
                scope: scope(),
                member: MemberId::new(1)?,
                birth_digest: [7; 32],
                voters: vec![MemberId::new(1)?],
                learners: vec![],
            })
            .await?;
        }
        let store = NodeExtentStore::open(
            root.join("extents"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?;
        let limits = ExecutionLimits::default();
        let pins = Budget::new(0, 1);
        let pool = Pool::start(
            token.clone(),
            1,
            4,
            2,
            Budgets {
                business: Budget::new(64 * 1024 * 1024, 4),
                archive_capture: Budget::new(64 * 1024 * 1024, 4),
                query_pins: pins.clone(),
            },
            super::super::performance::Registry::new(token.clone()),
        )?;
        let host = LtxHost::new(
            || 10,
            |_| Ok(Duration::ZERO),
            |path| async move { tokio::fs::read(path).await },
            |job| async move {
                job();
                Ok(())
            },
        )
        .with_filesystem(Arc::new(DirectFileSystem));
        let output = pool
            .call(
                0,
                scope(),
                Action::Initialize {
                    group: log.group(&scope())?,
                    class: class.clone(),
                    storage: CellStorage {
                        directory: root.join("cell"),
                        host,
                        services: ReadServices {
                            now_unix_millis: Arc::new(|| 10),
                            fill_random: Arc::new(|bytes| bytes.fill(17)),
                        },
                        images: GenerationLimits::default(),
                        execution: limits,
                        snapshot_scratch: ScratchPool::new(ScratchLimits::default())?,
                        rooted: Some(RootStorage {
                            store: store.clone(),
                            cache_directory: root.join(if resume {
                                "cache-reopened"
                            } else {
                                "cache"
                            }),
                            cache_pool: ScratchPool::new(ScratchLimits {
                                max_bytes: 4 * EXTENT_BYTES,
                                max_files: 4,
                            })?,
                            cache: CacheLimits {
                                max_cached_bytes: EXTENT_BYTES,
                                max_cached_extents: 1,
                            },
                        }),
                    },
                },
                None,
                None,
            )
            .await?;
        let Value::Initialized(view) = output.value else {
            anyhow::bail!("not initialized")
        };
        assert_eq!(view.is_some(), resume);
        if let Some(view) = view {
            assert!(matches!(
                view.image.commitment,
                crate::application::consensus::ImageCommitment::Extents(_)
            ));
        }
        assert_eq!(
            store.usage()?.workspaces,
            0,
            "initialization reserved a private writer"
        );
        let (runtime, events) = RaftRuntime::start(
            log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                election_seed: Some([11; 32]),
                ..Default::default()
            },
        )
        .await?;
        let mut host = Self {
            pool,
            domain,
            token,
            log,
            runtime,
            events,
            store,
            class,
            pins,
        };
        host.runtime.campaign(&scope()).await?;
        host.ready().await?;
        Ok(host)
    }

    async fn ready(&mut self) -> anyhow::Result<GroupStatus> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                while let Ok(event) = self.events.try_recv() {
                    if let RuntimeEventKind::Failed { error, .. } = event.kind {
                        return Err(error.into());
                    }
                }
                let status = self.runtime.status(&scope()).await?;
                if CommandAuthority::from_status(self.log.group(&scope())?, &status).is_ok() {
                    return Ok(status);
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?
    }

    async fn call(&self, action: Action) -> Result<Value, ExecutionError> {
        Ok(self.pool.call(0, scope(), action, None, None).await?.value)
    }

    async fn stage(
        &mut self,
        sequence: u64,
        tag: u8,
    ) -> anyhow::Result<(CommandIdentity, StageOutcome)> {
        let status = self.ready().await?;
        let input = 1_i64.to_be_bytes().to_vec();
        let proposal = ProposalId::new([tag; 16])?;
        let identity = CommandIdentity {
            authority: RaftAuthority {
                replica: status.replica.clone(),
                term: status.term,
                activation: ActivationGeneration::new(1),
            },
            operation: OperationId::new(u64::from(tag)),
            proposal,
        };
        let Value::Staged(outcome) = self
            .call(Action::Stage {
                identity: identity.clone(),
                status: Box::new(status),
                attempt: CommandAttempt {
                    request: CommandRequest {
                        id: CommandId::new(ClientId::new([1; 16])?, sequence)?,
                        operation: "Increment".into(),
                        contract_digest: [41; 32],
                        request_digest: command_request_digest(&input),
                        durability: Durability::Quorum,
                        retry_until_ms: 1000,
                    },
                    proposal,
                    preview: ImageId::new([tag; 16])?,
                    inputs: crate::storage::CommandInputs {
                        now_unix_millis: 10,
                        random: [17; 32],
                    },
                },
                input,
                allow_new: true,
                system: false,
            })
            .await?
        else {
            anyhow::bail!("not staged")
        };
        Ok((identity, outcome))
    }

    async fn event(&mut self) -> anyhow::Result<RuntimeEvent> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = self
                    .events
                    .recv()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("events closed"))?;
                if let RuntimeEventKind::Failed { error, .. } = &event.kind {
                    return Err(error.clone().into());
                }
                if matches!(
                    &event.kind,
                    RuntimeEventKind::Apply { .. } | RuntimeEventKind::Applied { .. }
                ) {
                    return Ok(event);
                }
            }
        })
        .await?
    }

    async fn commit(
        &mut self,
        identity: CommandIdentity,
        prepared: PreparedCommand,
    ) -> anyhow::Result<(ImageView, CommittedReceipt)> {
        self.call(Action::Submit { identity }).await?;
        self.runtime
            .propose(&scope(), prepared.context(), prepared.bytes().to_vec())
            .await?;
        let event = self.event().await?;
        let RuntimeEventKind::Apply { ticket, .. } = &event.kind else {
            anyhow::bail!("not an apply")
        };
        let ticket = ticket.clone();
        let Value::Prepared(completion) = self
            .call(Action::Prepare {
                event,
                image: ImageId::new([88; 16])?,
            })
            .await?
        else {
            anyhow::bail!("not prepared")
        };
        // The worker cannot publish just because physical preparation succeeded.
        assert!(self
            .call(Action::Publish {
                ticket: ticket.clone()
            })
            .await
            .is_err());
        self.runtime.complete_apply(completion).await?;
        let event = self.event().await?;
        let RuntimeEventKind::Applied {
            ticket: applied, ..
        } = event.kind
        else {
            anyhow::bail!("not applied")
        };
        assert_eq!(applied, ticket);
        let Value::Published {
            view,
            receipt: Some((_, receipt)),
        } = self.call(Action::Publish { ticket }).await?
        else {
            anyhow::bail!("missing publication receipt")
        };
        Ok((view, receipt))
    }

    async fn query(
        &self,
        operation: &str,
        minimum: super::super::query::Minimum,
        control: Arc<AttemptControl>,
    ) -> Result<RegisteredQueryOutput, ExecutionError> {
        let Value::Queried(output) = self
            .call(Action::Query {
                contract: self.class.contract(operation).unwrap(),
                input: vec![],
                minimum,
                control,
                source_deadline: None,
            })
            .await?
        else {
            return Err(ExecutionError::Invalid("not a query"));
        };
        Ok(output)
    }

    async fn close(mut self) -> anyhow::Result<()> {
        self.pool.join().await?;
        assert_eq!(self.store.usage()?.workspaces, 0);
        self.runtime.close().await?;
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

fn class(calls: Arc<AtomicUsize>) -> anyhow::Result<Arc<RaftClass>> {
    let class = RaftClass::new("Counter", 1, [40; 32])?
        .command("Increment", [41; 32], move |context, input| {
            calls.fetch_add(1, Ordering::SeqCst);
            let delta = i64::from_be_bytes(input.try_into()?);
            context.sql().execute_batch(
                "CREATE TABLE IF NOT EXISTS counter(value INTEGER);
                 INSERT INTO counter SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM counter)",
            )?;
            context
                .sql()
                .execute("UPDATE counter SET value=value+?1", [delta])?;
            let value: i64 = context
                .sql()
                .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Read", [42; 32], |context, _| {
            let value: i64 =
                context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Long", [43; 32], |context, _| {
            let value: i64 = context.query_row(
                "WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL
                 SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n",
                [],
                |row| row.get(0),
            )?;
            Ok(value.to_be_bytes().to_vec())
        })?;
    Ok(Arc::new(class))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extent_sql_worker_gates_publication_reconciles_receipts_and_reopens() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let class = class(calls.clone())?;
    let mut host = Host::open(directory.path(), class.clone(), false).await?;
    let (identity, outcome) = host.stage(1, 1).await?;
    let StageOutcome::Prepared(prepared) = outcome else {
        anyhow::bail!("duplicate new command")
    };
    let (view, receipt) = host.commit(identity, prepared).await?;
    let read = host
        .query(
            "Read",
            super::super::query::Minimum::ReadIndex(view.position.index),
            Arc::new(AttemptControl::new(host.token.clone())),
        )
        .await?;
    assert_eq!(read.result, 1_i64.to_be_bytes());
    assert_eq!(read.version.position, view.position);
    assert!(matches!(
        view.image.commitment,
        crate::application::consensus::ImageCommitment::Extents(_)
    ));
    let (_, duplicate) = host.stage(1, 2).await?;
    let StageOutcome::Duplicate(duplicate) = duplicate else {
        anyhow::bail!("retry reran callback")
    };
    assert_eq!(duplicate.receipt(), receipt.receipt());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // The root cannot accidentally use an old physical archive/effect contract.
    assert!(host
        .call(Action::CaptureArchive {
            limits: ArchiveLimits::default()
        })
        .await
        .is_err());
    assert_eq!(host.pins.usage(), (0, 0));
    host.close().await?;
    let mut resumed = Host::open(directory.path(), class, true).await?;
    assert_eq!(
        resumed
            .query(
                "Read",
                super::super::query::Minimum::Any,
                Arc::new(AttemptControl::new(resumed.token.clone()))
            )
            .await?
            .result,
        1_i64.to_be_bytes()
    );
    assert_eq!(
        resumed.store.usage()?.workspaces,
        0,
        "read opened a private writer"
    );
    let (_, duplicate) = resumed.stage(1, 3).await?;
    assert!(matches!(duplicate, StageOutcome::Duplicate(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let (identity, outcome) = resumed.stage(2, 4).await?;
    let StageOutcome::Prepared(prepared) = outcome else {
        anyhow::bail!("duplicate new command")
    };
    resumed.commit(identity, prepared).await?;
    assert_eq!(
        resumed
            .query(
                "Read",
                super::super::query::Minimum::Any,
                Arc::new(AttemptControl::new(resumed.token.clone()))
            )
            .await?
            .result,
        2_i64.to_be_bytes()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    resumed.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extent_sql_worker_interrupts_a_running_query_and_releases_its_pin() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut host = Host::open(
        directory.path(),
        class(Arc::new(AtomicUsize::new(0)))?,
        false,
    )
    .await?;
    let (identity, outcome) = host.stage(1, 1).await?;
    let StageOutcome::Prepared(prepared) = outcome else {
        anyhow::bail!("duplicate new command")
    };
    let (view, _) = host.commit(identity, prepared).await?;
    let control = Arc::new(AttemptControl::new(host.token.clone()));
    {
        let query = host.query("Long", super::super::query::Minimum::Any, control.clone());
        tokio::pin!(query);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                crate::asyncrt::select! {
                    result = &mut query => {
                        panic!("long query returned before cancellation: {}", result.is_ok())
                    },
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {
                        if host.pins.usage().1 == 1 { break; }
                    }
                }
            }
        })
        .await?;
        control.cancel();
        let result = tokio::time::timeout(Duration::from_secs(5), &mut query).await?;
        let Err(ExecutionError::Storage(error)) = result else {
            anyhow::bail!("running query was not interrupted");
        };
        let sqlite = error
            .downcast_ref::<rusqlite::Error>()
            .ok_or_else(|| anyhow::anyhow!("query returned a non-SQLite error: {error}"))?;
        assert_eq!(
            sqlite.sqlite_error_code(),
            Some(rusqlite::ErrorCode::OperationInterrupted)
        );
        assert_eq!(host.pins.usage(), (0, 0));
    }
    let mut future = CommittedVersion {
        scope: scope(),
        position: view.position,
    };
    future.position.index = LogIndex::new(view.position.index.get() + 1);
    assert!(matches!(
        host.query(
            "Read",
            super::super::query::Minimum::Version(future),
            Arc::new(AttemptControl::new(host.token.clone()))
        )
        .await,
        Err(ExecutionError::Refused(CommandFailure::VersionUnavailable))
    ));
    assert_eq!(
        host.query(
            "Read",
            super::super::query::Minimum::Any,
            Arc::new(AttemptControl::new(host.token.clone()))
        )
        .await?
        .result,
        1_i64.to_be_bytes()
    );
    host.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discarded_extent_commands_release_the_private_workspace_and_preserve_committed_reads(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut host = Host::open(directory.path(), class(calls.clone())?, false).await?;
    let (identity, outcome) = host.stage(1, 1).await?;
    let StageOutcome::Prepared(prepared) = outcome else {
        anyhow::bail!("duplicate new command")
    };
    host.commit(identity, prepared).await?;
    let (identity, outcome) = host.stage(2, 2).await?;
    assert!(matches!(outcome, StageOutcome::Prepared(_)));
    assert_eq!(host.store.usage()?.workspaces, 1);
    assert!(matches!(
        host.call(Action::Discard { identity }).await?,
        Value::Discarded
    ));
    assert_eq!(host.store.usage()?.workspaces, 0);
    let read = host
        .query(
            "Read",
            super::super::query::Minimum::Any,
            Arc::new(AttemptControl::new(host.token.clone())),
        )
        .await?;
    assert_eq!(read.result, 1_i64.to_be_bytes());
    let (identity, outcome) = host.stage(2, 3).await?;
    let StageOutcome::Prepared(prepared) = outcome else {
        anyhow::bail!("discard published a receipt")
    };
    let (_, receipt) = host.commit(identity, prepared).await?;
    assert_eq!(receipt.receipt().result(), 2_i64.to_be_bytes());
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    host.close().await
}
