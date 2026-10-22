//! A held physical SQL worker makes archive/command admission races repeatable.
#![allow(clippy::disallowed_methods)]
use super::{bundle_faults as faults, public_node};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, Output, RaftClass, Registry, ShutdownMode};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

struct ArchiveRelease(Arc<faults::Store>);

impl Drop for ArchiveRelease {
    fn drop(&mut self) {
        self.0.archive_hang.store(false, Ordering::Release);
        self.0.archive_resume.notify_waiters();
    }
}

fn definition(
    calls: Arc<AtomicUsize>,
    gate: Arc<public_node::Gate>,
) -> Result<public_node::Counter> {
    let class = RaftClass::new("Counter", 1, [21;32])?
        .command("Increment", [22;32], move |context,input| {
            let delta=i64::from_be_bytes(input.try_into()?);
            calls.fetch_add(1,Ordering::SeqCst);
            context.sql().execute_batch("CREATE TABLE IF NOT EXISTS counter(value INTEGER); INSERT INTO counter SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM counter)")?;
            context.sql().execute("UPDATE counter SET value=value+?1",[delta])?;
            if delta == 1000 { gate.wait()?; }
            let value:i64=context.sql().query_row("SELECT value FROM counter",[],|row|row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get",[23;32],|sql,_| {
            let value:i64=sql.query_row("SELECT value FROM counter",[],|row|row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?;
    Ok(public_node::Counter {
        schema: class.schema_digest(),
        increment: class.contract("Increment").unwrap(),
        get: class.contract("Get").unwrap(),
        application: Arc::new(
            Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
        ),
    })
}

pub async fn run(cancel: bool) -> Result<()> {
    let (gate, mut entered) = public_node::Gate::new();
    let release_callback = public_node::Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = definition(calls.clone(), gate.clone())?;
    let mut namespace = NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        definition.schema,
    );
    namespace.voters = VoterPolicy::SingleVoterDevelopment;
    let registrations = (10..12)
        .map(|id| {
            Ok(RaftCellRegistration {
                placement: RaftCellPlacement::Replica,
                namespace: namespace.clone(),
                class: "Counter".into(),
                birth: BirthProposal {
                    scope: GroupScope {
                        cluster: namespace.cluster,
                        namespace: namespace.namespace,
                        cell: format!("Counter:background-{id}"),
                        group: GroupId::new([id; 16])?,
                        incarnation: IncarnationId::new([3; 16])?,
                    },
                    voters: vec![InitialMember {
                        member: MemberId::new(1)?,
                        node: NodeId::new([1; 16])?,
                    }],
                    learners: vec![],
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let store = Arc::new(faults::Store::default());
    store.archive_hang.store(true, Ordering::Release);
    let mut release_archive = Some(ArchiveRelease(store.clone()));
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        store.clone(),
        registrations.clone(),
        false,
        RaftHostOptions {
            blocking_workers: 1,
            archive: RaftArchiveOptions {
                max_batch_groups: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        TransportOptions::default(),
    )
    .await?;
    let result = async {
        let cells = registrations
            .iter()
            .map(|r| service.cell(namespace.namespace, &r.birth.scope.cell))
            .collect::<Result<Vec<_>, _>>()?;
        for cell in &cells {
            public_node::ready(cell).await?;
        }
        cells[0]
            .command(
                definition.increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                public_node::options(1)?,
            )
            .await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.archive_attempts.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("first archive producer entered object I/O")?;
        cells[1]
            .command(
                definition.increment.clone(),
                10_i64.to_be_bytes().to_vec(),
                public_node::options(1)?,
            )
            .await?;
        let before = cells[0]
            .command(
                definition.increment.clone(),
                2_i64.to_be_bytes().to_vec(),
                public_node::options(2)?,
            )
            .await?;
        // Both new cuts are due, but the first producer still owns object I/O.
        // Occupy the sole physical SQL worker using the other cell's callback.
        let mut blocking_options = public_node::options(2)?;
        blocking_options.timeout = Duration::from_secs(10);
        let blocking = cells[1].command(
            definition.increment.clone(),
            1000_i64.to_be_bytes().to_vec(),
            blocking_options,
        );
        tokio::time::timeout(Duration::from_secs(5), entered.recv())
            .await?
            .context("blocked SQL callback")?;
        ensure!(calls.load(Ordering::SeqCst) == 4);
        drop(release_archive.take());
        // The busy cell cannot be captured. The only eligible cut belongs to
        // the target, and it queues behind the held physical SQL worker.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let resources = service.resource_usage();
                if resources.groups.is_some_and(|g| g.sql_busy == 2)
                    && resources.sql.is_some_and(|s| s.work_reservations >= 2)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("target archive capture queued behind SQL worker")?;
        let mut options = public_node::options(3)?;
        options.timeout = if cancel {
            Duration::from_millis(200)
        } else {
            Duration::from_secs(5)
        };
        let waiting = cells[0].command(
            definition.increment.clone(),
            4_i64.to_be_bytes().to_vec(),
            options.clone(),
        );
        tokio::pin!(waiting);
        let early = tokio::time::timeout(Duration::from_millis(30), waiting.as_mut()).await;
        ensure!(
            early.is_err(),
            "background SQL displaced the free command slot: {early:?}"
        );
        ensure!(
            calls.load(Ordering::SeqCst) == 4,
            "waiting command executed while physical worker held"
        );
        let overflow = cells[0]
            .command(
                definition.increment.clone(),
                8_i64.to_be_bytes().to_vec(),
                public_node::options(4)?,
            )
            .await;
        ensure!(
            matches!(overflow, Err(CommandFailure::Overloaded)),
            "second command bypassed the single pending slot: {overflow:?}"
        );
        if cancel {
            let cancelled = tokio::time::timeout(Duration::from_secs(2), waiting.as_mut()).await?;
            ensure!(
                matches!(cancelled, Err(CommandFailure::Unavailable)),
                "unsubmitted cancelled command had an unexpected result: {cancelled:?}"
            );
        }
        gate.release();
        ensure!(blocking.await?.result == 1010_i64.to_be_bytes());
        let receipt = if cancel {
            let read = cells[0]
                .query(
                    definition.get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(before.version_token),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(
                read.result == 3_i64.to_be_bytes() && calls.load(Ordering::SeqCst) == 4,
                "cancelled queued command executed after worker release"
            );
            options.timeout = Duration::from_secs(5);
            cells[0]
                .command(
                    definition.increment.clone(),
                    4_i64.to_be_bytes().to_vec(),
                    options,
                )
                .await?
        } else {
            waiting.await?
        };
        ensure!(receipt.result == 7_i64.to_be_bytes() && calls.load(Ordering::SeqCst) == 5);
        let read = cells[0]
            .query(
                definition.get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token),
                    ..Default::default()
                },
            )
            .await?;
        ensure!(read.result == 7_i64.to_be_bytes());
        Ok(())
    }
    .await;
    drop(release_callback);
    drop(release_archive);
    let closed = service.close().await;
    let stopped = node.shutdown(ShutdownMode::Preserve).await;
    result?;
    closed?;
    stopped?;
    Ok(())
}
