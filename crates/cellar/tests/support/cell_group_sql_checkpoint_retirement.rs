// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An applied RPC checkpoint retains custody without occupying unrelated SQL.

use super::super::super::fault as files;
use super::*;
use cellar::application::{
    Application, Cell, Context as CellContext, HandlerFuture, Invocation, Output, Registry, Storage,
};
use cellar::engine_api::RpcData;
use futures_util::FutureExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CheckpointCell {
    storage: Storage,
    calls: Arc<AtomicUsize>,
    resumed: Arc<AtomicUsize>,
}

impl Cell for CheckpointCell {
    fn invoke(self: Arc<Self>, _: CellContext, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            anyhow::ensure!(
                matches!(invocation, Invocation::CellRpc { ref method, .. } if method == "Run")
            );
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.storage.sql("UPDATE value SET n=n+1", &[])?;
            self.storage.sync().await?;
            self.resumed.fetch_add(1, Ordering::AcqRel);
            self.storage.sql("UPDATE value SET n=n+10", &[])?;
            Ok(Output::Rpc(RpcData::Json("11".into())))
        })
    }
}

pub(super) async fn progress(store: Arc<object_store::memory::InMemory>) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let resumed = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let continued = resumed.clone();
    let application = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
        RaftClass::new("Counter", 1, [224; 32])?.json_cell_rpc("Run", [225; 32])?,
        move |context, _| {
            Ok(Arc::new(CheckpointCell {
                storage: context.storage()?,
                calls: counted.clone(),
                resumed: continued.clone(),
            }))
        },
    )?;
    let class = application.resolve_raft_class("Counter")?;
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!(
        "retained checkpoint retirement disk: {}",
        root.path().display()
    );
    let gate = files::Gate::new(None);
    let filesystem = Arc::new(files::FaultFileSystem { gate: gate.clone() });
    let cluster = Cluster::with_store_birth(
        RaftRuntimeOptions::default(),
        root,
        filesystem.clone(),
        class.schema_digest(),
        store,
        ClusterBirthOptions {
            group: GroupId::new([3; 16])?,
            image_storage: RaftImageStorage::Extents,
            minimum: Durability::Quorum,
            log: ClusterLogLimits::default(),
        },
    )
    .await?;
    let initial = RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000).await?;
    let (mut fixture, route, source, other) =
        enroll_sql_cells_with_admission(initial, true, filesystem, true).await?;
    for node in 0..3 {
        let pool = CellGroupSqlPool::start(
            &fixture.cluster.voters[node].log,
            CellSqlLimits {
                workers: 1,
                queue_per_worker: 1,
                max_pending_cells: 2,
                group_pending_cells: 2,
                node_bytes: 1024 * 1024 * 1024,
                group_bytes: 512 * 1024 * 1024,
            },
        )?;
        apply::install_with_pool(&mut fixture, &source, node, pool).await?;
    }
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let runtime = fixture.runtimes[0].clone();
    let mut physical = None;
    let outcome = std::panic::AssertUnwindSafe(async {
        let dispatcher = CellGroupDispatcher::new(
            &fixture.cluster.domain,
            fixture.cluster.directory.clone(),
            class.clone(),
            VersionKeys::new(
                VersionSigningKey::new([226; 32], u64::MAX)?,
                vec![],
                Duration::from_secs(30),
            )?,
            CellDispatchLimits {
                max_cached_routes: 32,
                route_cache_bytes: 256 * 1024,
                max_invocations: 3,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 1024,
            },
        )?;
        let now = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 })
            .await?;
        let request = CellCommandRequest {
            id: CommandId::new(ClientId::new([227; 16])?, 1)?,
            contract: class.contract("Run").context("Run")?,
            input: b"{}".to_vec(),
            durability: Durability::Quorum,
            retry_until_ms: now + 60_000,
        };
        let limits = CellRoutingLimits {
            timeout: Duration::from_secs(15),
            max_attempts: 4,
        };
        let options = fixture.cell_options(&source).await?;
        let other_options = fixture.cell_options(&other).await?;
        let CellSqlOptions::Extents(rooted) = &options else {
            anyhow::bail!("rooted checkpoint retirement fixture");
        };
        let inventory = rooted.images.clone();
        let initial = runtime
            .reconciliation_authority(route.history, source.clone())
            .await?;
        let CellImagePin::Extents(image) = initial.image() else {
            anyhow::bail!("initial extent image");
        };
        let extent = image.manifest().extent(0).context("first extent")?;
        let selected = reads::selected(&mut fixture, 0, &other, CellReadConsistency::Stale).await?;
        let held_selected =
            reads::selected(&mut fixture, 0, &other, CellReadConsistency::Stale).await?;
        let mut invocation = dispatcher
            .command(source.clone(), request.clone(), limits, None)
            .await?;
        invocation.begin().await?;
        let mut preparing = Box::pin(invocation.prepare(&runtime, &pool, options.clone()));
        let mut held = BTreeMap::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                anyhow::ensure!(futures_util::poll!(preparing.as_mut()).is_pending());
                fixture.pump(&[route.history], &mut held).await?;
                if held.contains_key(&route.history) {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        let work = held.remove(&route.history).context("checkpoint apply")?;
        let prepared = pool
            .prepare_apply(&fixture.appliers[&(0, route.history)].1, work)
            .await?;
        let before_proof = pending::calls(
            &fixture.cluster.voters[0].log.performance_snapshot(),
            "cell_checkpoint_proof",
        )?;
        let before_handoff = pending::calls(
            &fixture.cluster.voters[0].log.performance_snapshot(),
            "cell_checkpoint_handoff",
        )?;
        // Reference only the original command, before either unrelated read
        // adds its independently admitted bytes. Query completion may shrink
        // its own reservation while the command must retain its whole charge.
        let admission = pool.usage()?;
        anyhow::ensure!(admission.active_cells == 1 && admission.retained_bytes > 0);
        let (entered, waiting) = tokio::sync::oneshot::channel();
        let (release_sql, wait_sql) = std::sync::mpsc::sync_channel::<()>(0);
        let mut holding =
            Box::pin(
                pool.query(other_options.clone(), held_selected, move |db, _| {
                    let _ = entered.send(());
                    let _ = wait_sql.recv();
                    let n: i64 = db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                    Ok(n.to_le_bytes().to_vec())
                }),
            );
        anyhow::ensure!(futures_util::poll!(holding.as_mut()).is_pending());
        tokio::time::timeout(Duration::from_secs(5), waiting).await??;
        prepared.complete(&runtime).await?;
        // The proof owner finishes while SQL is deliberately held. This cut
        // cannot confuse a proof/apply wait with preview retirement.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if pending::calls(
                    &fixture.cluster.voters[0].log.performance_snapshot(),
                    "cell_checkpoint_proof",
                )? > before_proof
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        let checkpoint = runtime
            .reconciliation_authority(route.history, source.clone())
            .await?;
        anyhow::ensure!(
            checkpoint.cell().version.sequence.get() == 2 && resumed.load(Ordering::Acquire) == 0
        );
        let charge = inventory.usage()?;
        let admission_with_query = pool.usage()?;
        anyhow::ensure!(admission_with_query.retained_bytes > admission.retained_bytes);
        let release = gate.arm(files::HOLD_EXTENT_READ);
        let checking = inventory.clone();
        physical = Some(
            fixture
                .cluster
                .domain
                .blocking(move || checking.verify_extent(extent))?,
        );
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        anyhow::ensure!(inventory.usage()?.busy);
        drop(release_sql);
        let held_output = holding.await?;
        anyhow::ensure!(held_output.result() == 0_i64.to_le_bytes());
        drop(held_output);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if pending::calls(
                    &fixture.cluster.voters[0].log.performance_snapshot(),
                    "cell_checkpoint_handoff",
                )? > before_handoff
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        let mut query = Box::pin(pool.query(other_options, selected, |db, _| {
            let n: i64 = db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
            Ok(n.to_le_bytes().to_vec())
        }));
        let early = tokio::time::timeout(Duration::from_secs(1), &mut query).await;
        let progressed = early.is_ok();
        let held_admission = pool.usage()?;
        let held_charge = inventory.usage()?;
        let continuation_withheld = resumed.load(Ordering::Acquire) == 0
            && futures_util::poll!(preparing.as_mut()).is_pending();
        eprintln!(
            "CHECKPOINT_RETIREMENT_ADMISSION {}",
            serde_json::json!({
                "commandBytesBeforeQueries": admission.retained_bytes,
                "bytesWithHoldingQuery": admission_with_query.retained_bytes,
                "bytesWhileRetirementBlocked": held_admission.retained_bytes,
                "commandCellsBeforeQueries": admission.active_cells,
                "commandCellsWhileBlocked": held_admission.active_cells,
                "continuationWithheld": continuation_withheld,
                "unrelatedSqlProgress": progressed,
            })
        );
        drop(release);
        physical
            .take()
            .context("inventory verification owner")?
            .await??;
        let output = match early {
            Ok(result) => result,
            Err(_) => query.await,
        }?;
        anyhow::ensure!(output.result() == 0_i64.to_le_bytes());
        drop(output);
        let staged = loop {
            if let std::task::Poll::Ready(result) = futures_util::poll!(preparing.as_mut()) {
                break result?;
            }
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        };
        drop(preparing);
        anyhow::ensure!(matches!(staged, CellCommandPreparation::Prepared));
        let accepted = command_dispatch::accepted(&mut invocation).await?;
        fixture.drive(Some(accepted.position.index)).await?;
        let output = command_dispatch::committed(&mut invocation).await?;
        let receipt = output.local_receipt().context("final RPC receipt")?;
        anyhow::ensure!(receipt.receipt.result() == b"11" && receipt.version.sequence.get() == 3);
        for replica in &fixture.runtimes {
            let current = replica
                .reconciliation_authority(route.history, source.clone())
                .await?;
            anyhow::ensure!(
                current.cell().version == receipt.version
                    && current.cell().witness == receipt.witness
            );
        }
        drop(output);
        drop(invocation);
        let mut replay = dispatcher.command(source, request, limits, None).await?;
        replay.begin().await?;
        let CellCommandPreparation::Committed(output) =
            replay.prepare(&runtime, &pool, options).await?
        else {
            anyhow::bail!("RPC replay entered callback");
        };
        anyhow::ensure!(
            output
                .local_receipt()
                .context("replayed receipt")?
                .receipt
                .result()
                == b"11"
        );
        drop(output);
        drop(replay);
        anyhow::ensure!(calls.load(Ordering::Acquire) == 1 && resumed.load(Ordering::Acquire) == 1);
        anyhow::ensure!(pool.usage()?.active_cells == 0 && dispatcher.usage() == (0, 0));
        anyhow::ensure!(
            held_admission.active_cells == 1
                && held_admission.retained_bytes >= admission.retained_bytes,
            "checkpoint retirement released original admission"
        );
        anyhow::ensure!(
            held_charge.workspaces == charge.workspaces
                && held_charge.workspace_bytes == charge.workspace_bytes,
            "checkpoint retirement refunded the live workspace"
        );
        anyhow::ensure!(
            continuation_withheld,
            "RPC continued before checkpoint retirement joined"
        );
        Ok::<_, anyhow::Error>(progressed)
    })
    .catch_unwind()
    .await;
    let joined = match physical.take() {
        Some(task) => task
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result.map_err(Into::into)),
        None => Ok(()),
    };
    drop(pool);
    drop(runtime);
    let closed = fixture.close().await;
    eprintln!(
        "CHECKPOINT_RETIREMENT_CLEANUP {{\"returnedOk\":{}}}",
        closed.is_ok()
    );
    let progressed = match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    joined?;
    closed?;
    eprintln!("CHECKPOINT_RETIREMENT {{\"unrelatedSqlProgress\":{progressed}}}");
    anyhow::ensure!(progressed, "checkpoint retirement blocked unrelated SQL");
    Ok(())
}
