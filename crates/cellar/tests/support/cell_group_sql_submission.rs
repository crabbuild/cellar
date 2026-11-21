// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use futures_util::{future::BoxFuture, FutureExt};
use raft::Storage;
use std::sync::atomic::{AtomicUsize, Ordering};

// These rejection phases deliberately have one ordinary request slot. Applied
// publication does not join peer frames accepted by admit_step in that pump.
// Join those frame owners within the original apply-through deadline before
// the next independent phase; every original overload/seal assertion remains.
async fn apply_through(fixture: &mut RuntimeCluster, index: LogIndex) -> Result<()> {
    let group = fixture.cluster.birth.route().history;
    let mut observed = false;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            if fixture.applied[&group].iter().all(|applied| *applied >= index) {
                let ordinary: Vec<_> = fixture
                    .runtimes
                    .iter()
                    .map(|runtime| runtime.usage().ordinary)
                    .collect();
                if !observed {
                    eprintln!(
                        "SUBMISSION_FRAME_DRAIN {}",
                        serde_json::json!({
                            "ordinaryItems": ordinary.iter().map(|usage| usage.items).collect::<Vec<_>>(),
                            "pendingFrames": fixture.network.len(),
                        })
                    );
                    observed = true;
                }
                if fixture.network.is_empty()
                    && ordinary.iter().all(|usage| usage.items == 0 && usage.bytes == 0)
                {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
}

/// Exercise queued prepared producers, not the legacy byte-only proposal API.
/// The held first sync makes the following queue prefix deterministic.
pub(super) async fn queued(rooted: bool, read_between: bool, max_events: usize) -> Result<()> {
    let mut fixture = RuntimeCluster::with_limits(max_events, 256, 1_000_000).await?;
    fixture.cluster._root.disable_cleanup(true);
    eprintln!(
        "retained queued submission disk: {}",
        fixture.cluster._root.path().display()
    );
    let mut held_sync = None;
    let checked = std::panic::AssertUnwindSafe(async {
        if rooted {
            for (n, voter) in fixture.cluster.voters.iter_mut().enumerate() {
                voter.extents = Some(NodeExtentStore::open(
                    fixture.cluster._root.path().join(format!("submission-{n}")),
                    Arc::new(DirectFileSystem),
                    Default::default(),
                )?);
            }
        }
        let route = fixture.cluster.birth.route().clone();
        fixture.runtimes[0].campaign(route.history).await?;
        fixture.drive(None).await?;
        let mut cells = vec![
            history(&route, "queued-trigger")?,
            history(&route, "queued-a")?,
            history(&route, "queued-b")?,
        ];
        if max_events == 4 {
            cells.push(history(&route, "queued-c")?);
        }
        let count = cells.len();
        // The same existing SQL producer envelope in rejections() permits all
        // original producers. No capacity or deadline changes in the fix.
        let limits = CellSqlLimits {
            workers: 2,
            queue_per_worker: 4,
            max_pending_cells: 4,
            group_pending_cells: 4,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        };
        for node in 0..3 {
            let config = match fixture.cell_options_at(&cells[0], node).await? {
                CellSqlOptions::Materialized(o) => CellGroupApplyImages::Materialized {
                    images: o.images,
                    services: o.query_services,
                },
                CellSqlOptions::Extents(o) => CellGroupApplyImages::Extents {
                    images: o.images,
                    services: o.query_services,
                    limits: Default::default(),
                    cache_directory: o.cache_directory,
                    cache_pool: o.cache_pool,
                    cache: o.cache,
                },
            };
            let voter = &mut fixture.cluster.voters[node];
            let pool = CellGroupSqlPool::start(&voter.log, limits)?;
            let applier = CellGroupApplier::new(
                voter.log.cell_group(&route.history)?,
                voter.pages.take().context("page owner")?,
                config,
                ManifestLimits::default(),
                GroupEntryLimits::default(),
                100,
            )?;
            fixture.appliers.insert((node, route.history), (pool, applier));
        }
        let pool = fixture.appliers[&(0, route.history)].0.clone();
        let callbacks = Arc::new(AtomicUsize::new(0));
        let mut prepared = Vec::new();
        for (n, cell) in cells.iter().enumerate() {
            let authority = fixture.runtimes[0]
                .enrollment_authority(route.history, cell.clone())
                .await?;
            let count = callbacks.clone();
            let tag = 191 + u8::try_from(n)?;
            let CellSqlOutcome::Prepared(command) = pool
                .initialize(
                    fixture.cell_options(cell).await?,
                    authority,
                    CellEnrollmentAttempt {
                        creation: ProposalId::new([tag; 16])?,
                        preview: ImageId::new([tag; 16])?,
                        inputs: cellar::storage::CommandInputs {
                            now_unix_millis: 10,
                            random: [17; 32],
                        },
                    },
                    move |tx| {
                        count.fetch_add(1, Ordering::SeqCst);
                        tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES (0);")?;
                        Ok(())
                    },
                )
                .await?
            else {
                anyhow::bail!("new queued enrollment")
            };
            pool.mark_submitted(cell.clone(), command.proposal()).await?;
            prepared.push(command);
        }
        let log = &fixture.cluster.voters[0].log;
        let group = log.cell_group(&route.history)?;
        let before = log.performance_snapshot();
        let gate = fixture.cluster.voters[0].gate.clone();
        held_sync = Some(gate.arm(fault::HOLD_SYNC));
        let first = accepted_sql(&fixture.runtimes[0], &prepared[0]).await?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        let runtime = &fixture.runtimes[0];
        let mut requests: Vec<BoxFuture<'_, Result<Option<CellGroupProposal>>>> = Vec::new();
        requests.push(async { accepted_sql(runtime, &prepared[1]).await.map(Some) }.boxed());
        if read_between {
            requests.push(
                async {
                    runtime.read_index(route.history, vec![77]).await?;
                    Ok(None)
                }
                .boxed(),
            );
        }
        for command in &prepared[2..] {
            requests.push(async move { accepted_sql(runtime, command).await.map(Some) }.boxed());
        }
        let mut queued = Box::pin(futures_util::future::join_all(requests));
        assert!(tokio::time::timeout(Duration::from_millis(30), queued.as_mut()).await.is_err());
        assert_eq!(runtime.usage().ordinary.items, count - 1 + usize::from(read_between));
        assert_eq!(group.group_manifest()?.context("selected group")?.root().cells(), 0);
        assert_eq!(pool.usage()?.active_cells, count);
        assert!(prepared.iter().all(|command| command.rejection().unwrap().is_none()));
        drop(held_sync.take());
        let mut accepted = vec![first];
        let responses = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                cellar::asyncrt::select! {
                    responses = queued.as_mut() => break Ok::<_, anyhow::Error>(responses),
                    event = fixture.events[0].recv() => {
                        let event = event.context("leader event stream")?;
                        let CellGroupEffect::Message(message) = event.effect()
                            .map_err(|error| anyhow::anyhow!(error.to_string()))? else {
                            anyhow::bail!("unexpected event before peer delivery")
                        };
                        // Release finite output admission without delivering any
                        // frame or losing it. The normal pump forwards it later.
                        fixture.network.push_back((route.history, message.clone()));
                    }
                }
            }
        }).await??;
        drop(queued);
        for response in responses {
            if let Some(proposal) = response? {
                accepted.push(proposal);
            }
        }
        for pair in accepted.windows(2) {
            assert_eq!(pair[0].position.index.get() + 1, pair[1].position.index.get());
        }
        let last = accepted.last().context("last admission")?.position.index;
        // Do not deliver any peer messages yet: only the admitted entries
        // can contribute to this persistence delta, with no apply/commit noise.
        let after = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let sample = log.performance_snapshot();
                let op = sample.operations.iter().find(|op| op.name == "engine_persist").unwrap();
                if group.last_index()? == last.get() && op.started == op.succeeded {
                    break Ok::<_, anyhow::Error>(sample);
                }
                tokio::task::yield_now().await;
            }
        }).await??;
        assert!(before.available && after.available && !before.saturated && !after.saturated);
        assert_eq!(before.source, after.source);
        let entries = |sample: &RaftPerformanceSnapshot| sample.counters.iter()
            .find(|counter| counter.name == "engine_persist_entries").unwrap().value;
        let batches = |sample: &RaftPerformanceSnapshot| sample.operations.iter()
            .find(|op| op.name == "engine_persist").unwrap().succeeded;
        let entry_delta = entries(&after) - entries(&before);
        let batch_delta = batches(&after) - batches(&before);
        assert_eq!(entry_delta, count as u64);
        fixture.drive(Some(last)).await?;
        for ((cell, command), proposal) in cells.iter().zip(prepared).zip(&accepted) {
            let record = super::enrollment::resolved(&fixture, &pool, 0, cell, command.proposal()).await?;
            assert_eq!(record.record().creation(), command.proposal());
            assert_eq!(record.record().cell(), cell);
            assert_eq!(record.durability(), Durability::Quorum);
            drop(record);
            let replay = accepted_sql(&fixture.runtimes[0], &command).await?;
            assert_eq!(replay.position, proposal.position);
            assert_eq!(replay.digest, proposal.digest);
            assert!(command.rejection()?.is_none());
            let creation = command.proposal();
            drop(command);
            for node in 0..3 {
                let state = fixture.runtimes[node].enrollment_state(route.history, cell.clone()).await?;
                assert_eq!(state.record().context("replicated creation")?.creation(), creation);
                let authority = super::reads::selected(&mut fixture, node, cell, CellReadConsistency::Stale).await?;
                let output = fixture.appliers[&(node, route.history)].0.query(
                    fixture.cell_options_at(cell, node).await?, authority, |db, version| {
                        assert_eq!(version.sequence.get(), 1);
                        let value: i64 = db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                        assert_eq!(value, 0);
                        Ok(value.to_le_bytes().to_vec())
                    }).await?;
                drop(output);
            }
        }
        assert_eq!(callbacks.load(Ordering::SeqCst), count);
        assert_eq!(pool.usage()?.active_cells, 0);
        assert_eq!(pool.usage()?.retained_operations, 0);
        assert_eq!(pool.usage()?.retained_bytes, 0);
        eprintln!("queued submission observations: rooted={rooted} read_between={read_between} entries={entry_delta} batches={batch_delta} callbacks={count} replicated_cells={count}");
        Ok::<_, anyhow::Error>(batch_delta)
    }).catch_unwind().await;
    drop(held_sync.take());
    let closed = fixture.close().await;
    let batches = match checked {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    closed?;
    // All correctness checks and joined shutdown precede the performance oracle.
    assert_eq!(
        batches,
        if read_between || max_events == 4 {
            3
        } else {
            2
        },
        "adjacent prepared submissions must share Ready persistence without crossing a queued read"
    );
    Ok(())
}

async fn initial(
    fixture: &RuntimeCluster,
    pool: &CellGroupSqlPool,
    cell: &CellHistory,
    creation: u8,
    preview: u8,
) -> Result<CellSqlPrepared> {
    let authority = fixture.runtimes[0]
        .enrollment_authority(fixture.cluster.birth.route().history, cell.clone())
        .await?;
    let CellSqlOutcome::Prepared(prepared) = pool
        .initialize(
            fixture.cell_options(cell).await?,
            authority,
            CellEnrollmentAttempt {
                creation: ProposalId::new([creation; 16])?,
                preview: ImageId::new([preview; 16])?,
                inputs: cellar::storage::CommandInputs {
                    now_unix_millis: 10,
                    random: [17; 32],
                },
            },
            |tx| {
                tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES (0);")?;
                Ok(())
            },
        )
        .await?
    else {
        anyhow::bail!("new enrollment")
    };
    pool.mark_submitted(cell.clone(), prepared.proposal())
        .await?;
    Ok(prepared)
}

async fn mutation(
    fixture: &RuntimeCluster,
    pool: &CellGroupSqlPool,
    cell: &CellHistory,
    proposal: u8,
    preview: u8,
) -> Result<CellSqlPrepared> {
    let authority = fixture.runtimes[0]
        .execution_authority(fixture.cluster.birth.route().history, cell.clone())
        .await?;
    let mut attempt = cell_attempt(proposal)?;
    attempt.preview = ImageId::new([preview; 16])?;
    let CellSqlOutcome::Prepared(prepared) = pool
        .stage(
            fixture.cell_options(cell).await?,
            authority,
            attempt,
            true,
            |tx| {
                let value: i64 = tx.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                assert_eq!(value, 0, "rejected private effects must not be reused");
                tx.execute("UPDATE value SET n=n+1", [])?;
                Ok(b"committed".to_vec())
            },
        )
        .await?
    else {
        anyhow::bail!("new mutation")
    };
    pool.mark_submitted(cell.clone(), prepared.proposal())
        .await?;
    Ok(prepared)
}

pub(super) async fn rejections(rooted: bool) -> Result<()> {
    let mut fixture = RuntimeCluster::with_limits(256, 1, 2)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    if rooted {
        for (n, voter) in fixture.cluster.voters.iter_mut().enumerate() {
            voter.extents = Some(NodeExtentStore::open(
                fixture.cluster._root.path().join(format!("submission-{n}")),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?);
        }
    }
    let route = fixture.cluster.birth.route().clone();
    fixture.runtimes[0]
        .campaign(route.history)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    fixture.drive(None).await.with_context(|| {
        format!(
            "submission rejection at line {} (extents={rooted})",
            line!()
        )
    })?;
    let limits = CellSqlLimits {
        workers: 2,
        queue_per_worker: 4,
        max_pending_cells: 4,
        group_pending_cells: 4,
        node_bytes: 512 * 1024 * 1024,
        group_bytes: 512 * 1024 * 1024,
    };
    let pool = CellGroupSqlPool::start(&fixture.cluster.voters[0].log, limits)?;
    let a = history(&route, "submission-a")?;
    let b = history(&route, "submission-b")?;
    let c = history(&route, "submission-c")?;
    let pa = initial(&fixture, &pool, &a, 181, 181)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    let pb = initial(&fixture, &pool, &b, 182, 182)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;

    // Drop the acceptance response, then hold real engine sync so a queued
    // ReadIndex occupies the only ordinary request slot. Queue overload must
    // not produce a false owner rejection for B.
    let gate = fixture.cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    drop(pa.submit(&fixture.runtimes[0]).await.with_context(|| {
        format!(
            "submission rejection at line {} (extents={rooted})",
            line!()
        )
    })?);
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    let mut waiting = Box::pin(fixture.runtimes[0].read_index(route.history, vec![77]));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), waiting.as_mut())
            .await
            .is_err()
    );
    assert!(matches!(
        pb.submit(&fixture.runtimes[0]).await,
        Err(RuntimeError::Overloaded)
    ));
    assert!(pb.rejection()?.is_none());
    assert!(pa.rejection()?.is_none());
    assert_eq!(pool.usage()?.active_cells, 2);
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), waiting)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    let accepted_a = accepted_sql(&fixture.runtimes[0], &pa)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert!(pa.rejection()?.is_none());
    let state = fixture.runtimes[0]
        .enrollment_state(route.history, b.clone())
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert!(matches!(
        pool.resolve_enrollment(pb.proposal(), state).await,
        Err(ExecutionError::Uncertain)
    ));

    // The actual owner now refuses B against A's retained acceptance slot.
    // Discard the response and recover its irreversible seal from the producer.
    drop(pb.submit(&fixture.runtimes[0]).await.with_context(|| {
        format!(
            "submission rejection at line {} (extents={rooted})",
            line!()
        )
    })?);
    let proof_b = pb.rejection()?.context("sealed B")?;
    assert!(pool.reject(a.clone(), proof_b.clone()).await.is_err());
    assert_eq!(pool.usage()?.active_cells, 2);
    let foreign = CellGroupSqlPool::start(&fixture.cluster.voters[1].log, limits)?;
    assert!(foreign.reject(b.clone(), proof_b.clone()).await.is_err());
    foreign.close().await.with_context(|| {
        format!(
            "submission rejection at line {} (extents={rooted})",
            line!()
        )
    })?;
    assert!(matches!(
        pb.submit(&fixture.runtimes[1]).await,
        Err(RuntimeError::Invalid(_))
    ));
    pool.reject(b.clone(), proof_b.clone())
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert_eq!(pool.usage()?.active_cells, 1);
    assert!(pool.usage()?.retained_bytes > 0);
    assert!(matches!(
        fixture.runtimes[0]
            .propose_at_term(route.history, pb.term(), pb.bytes().to_vec())
            .await,
        Err(RuntimeError::Invalid(_))
    ));
    apply_through(&mut fixture, accepted_a.position.index)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    drop(
        super::enrollment::resolved(&fixture, &pool, 0, &a, pa.proposal())
            .await
            .with_context(|| {
                format!(
                    "submission rejection at line {} (extents={rooted})",
                    line!()
                )
            })?,
    );

    // Retrying after capacity returns must still reject every old clone. A fresh
    // producer of the same creation identity gets a distinct submission seal.
    let fresh_b = initial(&fixture, &pool, &b, 182, 183)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert_eq!(pb.proposal(), fresh_b.proposal());
    assert!(pool.reject(b.clone(), proof_b.clone()).await.is_err());
    assert_eq!(pool.usage()?.active_cells, 1);
    assert!(matches!(
        pb.submit(&fixture.runtimes[0])
            .await
            .with_context(|| format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            ))?,
        CellSqlSubmission::Rejected(_)
    ));
    let accepted_b = accepted_sql(&fixture.runtimes[0], &fresh_b)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert_eq!(
        accepted_b.position.index.get(),
        accepted_a.position.index.get() + 1
    );
    apply_through(&mut fixture, accepted_b.position.index)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    drop(
        super::enrollment::resolved(&fixture, &pool, 0, &b, fresh_b.proposal())
            .await
            .with_context(|| {
                format!(
                    "submission rejection at line {} (extents={rooted})",
                    line!()
                )
            })?,
    );
    let pc = initial(&fixture, &pool, &c, 184, 184)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    drop(pc.submit(&fixture.runtimes[0]).await.with_context(|| {
        format!(
            "submission rejection at line {} (extents={rooted})",
            line!()
        )
    })?);
    pool.reject(c.clone(), pc.rejection()?.context("full group rejection")?)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert_eq!(pool.usage()?.active_cells, 0);
    assert!(fixture.runtimes[0]
        .enrollment_state(route.history, c)
        .await
        .with_context(|| format!(
            "submission rejection at line {} (extents={rooted})",
            line!()
        ))?
        .record()
        .is_none());

    // Completed/rejected producers release cell names, but their retained
    // outputs still consume all four operation reservations. Release this
    // phase's outputs before preparing the independent mutation phase.
    assert_eq!(pool.usage()?.retained_operations, 4);
    drop((pa, pb, fresh_b, pc, proof_b));
    assert_eq!(pool.usage()?.retained_operations, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);

    let ma = mutation(&fixture, &pool, &a, 185, 185)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    let mb = mutation(&fixture, &pool, &b, 186, 186)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    drop(ma.submit(&fixture.runtimes[0]).await.with_context(|| {
        format!(
            "submission rejection at line {} (extents={rooted})",
            line!()
        )
    })?);
    let accepted_ma = accepted_sql(&fixture.runtimes[0], &ma)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    drop(mb.submit(&fixture.runtimes[0]).await.with_context(|| {
        format!(
            "submission rejection at line {} (extents={rooted})",
            line!()
        )
    })?);
    let proof_mb = mb.rejection()?.context("mutation seal")?;
    assert!(pool.reject(a.clone(), proof_mb.clone()).await.is_err());
    pool.reject(b.clone(), proof_mb.clone())
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    apply_through(&mut fixture, accepted_ma.position.index)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert_eq!(
        pool.finish(authority)
            .await
            .with_context(|| format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            ))?
            .receipt()
            .receipt
            .result(),
        b"committed"
    );
    let cached = accepted_sql(&fixture.runtimes[0], &ma)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert_eq!(cached.position, accepted_ma.position);
    assert_eq!(cached.digest, accepted_ma.digest);
    assert!(ma.rejection()?.is_none());
    let fresh_mb = mutation(&fixture, &pool, &b, 186, 187)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert_eq!(mb.proposal(), fresh_mb.proposal());
    assert!(pool.reject(b.clone(), proof_mb.clone()).await.is_err());
    assert!(matches!(
        mb.submit(&fixture.runtimes[0])
            .await
            .with_context(|| format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            ))?,
        CellSqlSubmission::Rejected(_)
    ));
    let accepted_mb = accepted_sql(&fixture.runtimes[0], &fresh_mb)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert_eq!(
        accepted_mb.position.index.get(),
        accepted_ma.position.index.get() + 1
    );
    apply_through(&mut fixture, accepted_mb.position.index)
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await
        .with_context(|| {
            format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            )
        })?;
    assert_eq!(
        pool.finish(authority)
            .await
            .with_context(|| format!(
                "submission rejection at line {} (extents={rooted})",
                line!()
            ))?
            .receipt()
            .receipt
            .result(),
        b"committed"
    );
    assert_eq!(pool.usage()?.active_cells, 0);
    for node in 0..3 {
        for cell in [&a, &b] {
            let proof = fixture.runtimes[node]
                .reconciliation_authority(route.history, cell.clone())
                .await
                .with_context(|| {
                    format!(
                        "submission rejection at line {} (extents={rooted})",
                        line!()
                    )
                })?;
            assert_eq!(proof.cell().version.sequence.get(), 2);
        }
    }
    drop((ma, mb, fresh_mb, proof_mb));
    pool.close().await.with_context(|| {
        format!(
            "submission rejection at line {} (extents={rooted})",
            line!()
        )
    })?;
    assert_eq!(pool.usage()?.retained_bytes, 0);
    assert_eq!(pool.usage()?.running_workers, 0);
    if rooted {
        assert_eq!(
            fixture.cluster.voters[0]
                .extents
                .as_ref()
                .context("extents")?
                .usage()?
                .workspaces,
            0
        );
    }
    fixture.close().await
}
