// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A completed immutable read joins the catalog confirmation it raced with.
use super::remote_queries::{dispatcher, keys, wait_for};
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

pub(crate) async fn automatic_queries_finish_catalog_refresh(rooted: bool) -> Result<()> {
    for mode in 0..3 {
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let physical = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let count = calls.clone();
        let entry = entered.clone();
        let release = physical.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.query(
            "Get",
            [82; 32],
            move |ctx, _| {
                anyhow::ensure!(
                    count.fetch_add(1, Ordering::SeqCst) == 0,
                    "another SQL callback"
                );
                entry.notify_one();
                let (lock, wake) = release.as_ref();
                let released = wake
                    .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(15), |done| {
                        !*done
                    })
                    .unwrap();
                anyhow::ensure!(*released.0, "physical query was not released");
                Ok(ctx
                    .query_row("SELECT n FROM value", [], |r| r.get::<_, i64>(0))?
                    .to_be_bytes()
                    .to_vec())
            },
        )?);
        let store = Arc::new(catalog_fault::Store::default());
        let (mut fixture, route, cell, _) =
            Box::pin(registered::setup_store(rooted, &class, store.clone())).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        // Peers have actual TLS destinations but no query gateway. Their typed
        // refusals force the fallback into rooted discovery without another SQL.
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let query = dispatch.query_routed(
            cell.clone(),
            CellQueryRequest {
                invocation: ProposalId::new([217; 16])?,
                contract: class.contract("Get").context("Get")?,
                input: vec![],
                consistency: CellQueryConsistency::Linearizable,
            },
            CellRoutingLimits {
                timeout: Duration::from_secs(2),
                max_attempts: 4,
            },
            None,
            cellar::application::consensus::verification::cell_groups::CellQueryRouting::Automatic,
        )?;
        let call = controllers[1].query(query, fixture.cell_options_at(&cell, 1).await?)?;
        let result: Result<()> = async {
            tokio::time::timeout(Duration::from_secs(3), entered.notified()).await?;
            store.group_head_read.armed.store(true, Ordering::Release);
            tokio::time::timeout(
                Duration::from_secs(3),
                store.group_head_read.entered.notified(),
            )
            .await?;
            if mode == 1 {
                let directory = fixture.cluster.directory.clone();
                let next = directory.prepare_group_birth(CellGroupBirthSpec {
                    history: GroupHistory {
                        group: GroupId::new([218; 16])?,
                        ..route.history
                    },
                    range: route.range.clone(),
                    epoch: RouteEpoch::new(2)?,
                    creation: ProposalId::new([219; 16])?,
                    voters: fixture.cluster.birth.voters().to_vec(),
                    learners: vec![],
                })?;
                fixture
                    .cluster
                    .domain
                    .run(directory.reserve_group_birth(&next))
                    .await??;
                let view = fixture.cluster.domain.run(directory.observe()).await??;
                let publish = directory.prepare_replacement(
                    &view,
                    ProposalId::new([220; 16])?,
                    std::slice::from_ref(&route),
                    &[next.route().clone()],
                )?;
                fixture
                    .cluster
                    .domain
                    .run(directory.publish(&publish))
                    .await??;
            } else if mode == 2 {
                store
                    .deny_routing_metadata_reads
                    .store(true, Ordering::Release);
            }
            *physical.0.lock().unwrap() = true;
            physical.1.notify_all();
            let mut response = Box::pin(call.response());
            // This observation interval remains inside the original two-second
            // invocation. The held real catalog read has not confirmed a route.
            let response = match tokio::time::timeout(Duration::from_millis(200), response.as_mut())
                .await
            {
                Ok(result) if mode != 0 => result,
                Ok(Err(error)) => {
                    anyhow::bail!("completed query refused before catalog confirmation: {error}")
                }
                Ok(Ok(_)) => anyhow::bail!("completed query escaped before catalog confirmation"),
                Err(_) => {
                    store.group_head_read.resume.notify_one();
                    response.await
                }
            };
            if mode == 0 {
                let response =
                    response.context("completed query must join same-route confirmation")?;
                assert_eq!(response.result(), 0i64.to_be_bytes());
                assert_eq!(response.observation().version.history, cell);
                assert_eq!(response.replica().member, MemberId::new(2)?);
            } else {
                let error = response.err().context("catalog refusal")?;
                anyhow::ensure!(
                    matches!(
                        (mode, error.as_ref()),
                        (
                            1,
                            CellDispatchError::Routing(CellRoutingError {
                                reason: CellRoutingFailure::StaleAttempt,
                                ..
                            })
                        ) | (
                            2,
                            CellDispatchError::Routing(CellRoutingError {
                                reason: CellRoutingFailure::Catalog(_),
                                ..
                            })
                        )
                    ),
                    "mode {mode}: expected catalog refusal, observed {error}"
                );
            }
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "catalog confirmation replayed SQL"
            );
            Ok(())
        }
        .await;
        *physical.0.lock().unwrap() = true;
        physical.1.notify_all();
        store.group_head_read.resume.notify_one();
        store
            .deny_routing_metadata_reads
            .store(false, Ordering::Release);
        wait_for(async || {
            Ok(controllers
                .iter()
                .all(|owner| owner.usage().retained_commands == 0)
                && dispatch.usage() == (0, 0))
        })
        .await?;
        if result.is_err() {
            let root = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
            eprintln!(
                "retained failed query refresh disk: {}",
                root.keep().display()
            );
        }
        for controller in controllers {
            controller.close().await?;
        }
        fixture.close().await?;
        result?;
        eprintln!("QUERY_REFRESH_JOIN extents={rooted} mode={mode}: PASS");
    }
    Ok(())
}
