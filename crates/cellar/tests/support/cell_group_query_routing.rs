// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Controller-owned attempts keep one invocation while expired physical SQL joins.
use super::remote_queries::{dispatcher, gateways, keys, wait_for};
use super::*;
use cellar::application::consensus::verification::cell_groups::CellQueryRouting;
use cellar::application::consensus::VersionError;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex,
};

pub(crate) async fn controller_routed_queries(rooted: bool) -> Result<()> {
    for mode in 0..4 {
        let (entered, started) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered));
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let held = Mutex::new(held);
        let blocked = AtomicBool::new(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.query(
            "Get",
            [82; 32],
            move |ctx, input| {
                count.fetch_add(1, Ordering::SeqCst);
                assert!(input == b"warm" || input == b"captured-input");
                if input == b"captured-input" && !blocked.swap(true, Ordering::SeqCst) {
                    entered
                        .lock()
                        .unwrap()
                        .take()
                        .context("first entry")?
                        .send(())
                        .map_err(|_| anyhow::anyhow!("entry observer"))?;
                    held.lock().unwrap().recv_timeout(Duration::from_secs(15))?;
                }
                Ok(ctx
                    .query_row("SELECT n FROM value", [], |r| r.get::<_, i64>(0))?
                    .to_be_bytes()
                    .to_vec())
            },
        )?);
        let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let owners = gateways(&fixture, &controllers, class.clone()).await?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let options = fixture.cell_options_at(&a, 1).await?;
        let request = |tag, input: &[u8], consistency| -> Result<CellQueryRequest> {
            Ok(CellQueryRequest {
                invocation: ProposalId::new([tag; 16])?,
                contract: class.contract("Get").context("Get")?,
                input: input.to_vec(),
                consistency,
            })
        };
        let bounds = CellRoutingLimits {
            timeout: Duration::from_secs(2),
            max_attempts: if mode == 2 { 1 } else { 4 },
        };
        for policy in [
            CellQueryRouting::Replicas {
                members: vec![],
                attempt_timeout: Duration::from_millis(180),
                leader_fallback: false,
            },
            CellQueryRouting::Replicas {
                members: vec![MemberId::new(2)?, MemberId::new(2)?],
                attempt_timeout: Duration::from_millis(180),
                leader_fallback: false,
            },
            CellQueryRouting::Replicas {
                members: vec![MemberId::new(2)?],
                attempt_timeout: Duration::ZERO,
                leader_fallback: false,
            },
            CellQueryRouting::Replicas {
                members: vec![MemberId::new(2)?],
                attempt_timeout: Duration::from_secs(3),
                leader_fallback: false,
            },
        ] {
            assert!(matches!(
                dispatch.query_routed(
                    a.clone(),
                    request(211, b"warm", CellQueryConsistency::Stale)?,
                    bounds,
                    None,
                    policy
                ),
                Err(CellDispatchError::Invalid("query replica routing bounds"))
            ));
        }
        assert_eq!(dispatch.usage(), (0, 0));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let warm = dispatch.query_routed(
            a.clone(),
            request(212, b"warm", CellQueryConsistency::Linearizable)?,
            bounds,
            None,
            CellQueryRouting::Replicas {
                members: vec![MemberId::new(3)?],
                attempt_timeout: Duration::from_millis(180),
                leader_fallback: false,
            },
        )?;
        let warm = controllers[1]
            .query(warm, options.clone())?
            .response()
            .await?;
        let token = warm.token().clone();
        let observed = warm.observation().clone();
        assert_eq!(warm.replica().member, MemberId::new(3)?);
        assert_eq!(
            warm.replica().node_identity(),
            fixture.cluster.voters[2]
                .log
                .cell_group(&route.history)?
                .identity()
                .node_identity()
        );
        assert_eq!(fixture.transports[1].usage().query_items, 1);
        drop(warm);
        wait_for(async || {
            Ok(fixture.transports[1].usage().query_items == 0
                && dispatch.usage() == (0, 0)
                && controllers[1].usage().retained_commands == 0)
        })
        .await?;

        let now = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms().max(0) as u64 })
            .await?;
        let wrong_schema = keys(190)?.issue_cell(&observed, [99; 32], now)?;
        let invalid = dispatch.query_routed(
            a.clone(),
            request(215, b"warm", CellQueryConsistency::AtLeast(wrong_schema))?,
            bounds,
            None,
            CellQueryRouting::Replicas {
                members: vec![MemberId::new(3)?, MemberId::new(1)?],
                attempt_timeout: Duration::from_millis(180),
                leader_fallback: true,
            },
        )?;
        assert!(matches!(
            controllers[1]
                .query(invalid, options.clone())?
                .response()
                .await
                .err()
                .context("wrong schema")?
                .as_ref(),
            CellDispatchError::Version(VersionError::WrongSchema)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Delivery may wake this task before the sending producer has returned.
        // Observe its joined cleanup; the reply itself is not release authority.
        wait_for(async || {
            Ok(dispatch.usage() == (0, 0) && controllers[1].usage().retained_commands == 0)
        })
        .await?;
        assert_eq!(dispatch.usage(), (0, 0));
        assert_eq!(fixture.transports[1].usage().query_items, 0);

        let (members, fallback, held_node) = match mode {
            0 | 2 => (vec![MemberId::new(2)?, MemberId::new(3)?], false, 1),
            1 => (vec![MemberId::new(3)?], true, 2),
            _ => (vec![MemberId::new(1)?], true, 0),
        };
        let routed = dispatch.query_routed(
            a.clone(),
            request(
                213,
                b"captured-input",
                CellQueryConsistency::AtLeast(token.clone()),
            )?,
            bounds,
            None,
            CellQueryRouting::Replicas {
                members,
                attempt_timeout: Duration::from_millis(180),
                leader_fallback: fallback,
            },
        )?;
        let response = controllers[1].query(routed, options.clone())?;
        tokio::time::timeout(Duration::from_secs(5), started).await??;
        let response = response.response().await;
        if mode < 2 {
            let response = response?;
            assert_eq!(response.result(), 0i64.to_be_bytes());
            assert_eq!(response.observation(), &observed);
            let member = if mode == 0 { 3 } else { 1 };
            assert_eq!(response.replica().member, MemberId::new(member)?);
            assert_eq!(
                response.replica().node_identity(),
                fixture.cluster.voters[member as usize - 1]
                    .log
                    .cell_group(&route.history)?
                    .identity()
                    .node_identity()
            );
            assert_eq!(calls.load(Ordering::SeqCst), 3);
            drop(response);
        } else {
            let error = response.err().context("bounded routing must refuse")?;
            if mode == 2 {
                assert!(
                    matches!(
                        error.as_ref(),
                        CellDispatchError::Routing(CellRoutingError {
                            reason: CellRoutingFailure::AttemptsExhausted,
                            ..
                        })
                    ),
                    "mode={mode} error={error:?}"
                );
            } else {
                assert!(matches!(
                    error.as_ref(),
                    CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
                        | CellDispatchError::Transport(
                            TransportError::Timeout
                                | TransportError::Query(CommandFailure::VersionUnavailable)
                        )
                ));
            }
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        }
        let held_pool = fixture.appliers[&(held_node, route.history)].0.clone();
        assert_eq!(held_pool.usage()?.retained_operations, 1);
        assert_eq!(controllers[held_node].usage().retained_commands, 1);
        if held_node == 1 {
            assert_eq!(
                dispatch.usage().1,
                1,
                "expired local callback retains invocation admission"
            );
            assert!(dispatch.usage().0 >= 8192 + 2 * b"captured-input".len());
        }
        release.send(())?;
        wait_for(async || {
            Ok(held_pool.usage()?.retained_operations == 0
                && controllers[held_node].usage().retained_commands == 0
                && dispatch.usage() == (0, 0)
                && fixture.transports[1].usage().query_items == 0)
        })
        .await?;
        // A correctly signed token for the other cell cannot be retried into A.
        let before = calls.load(Ordering::SeqCst);
        let wrong = dispatch.query_routed(
            b.clone(),
            request(214, b"warm", CellQueryConsistency::AtLeast(token))?,
            bounds,
            None,
            CellQueryRouting::Replicas {
                members: vec![MemberId::new(2)?, MemberId::new(3)?],
                attempt_timeout: Duration::from_millis(180),
                leader_fallback: true,
            },
        )?;
        assert!(matches!(
            controllers[1]
                .query(wrong, fixture.cell_options_at(&b, 1).await?)?
                .response()
                .await
                .err()
                .context("foreign token")?
                .as_ref(),
            CellDispatchError::Version(VersionError::WrongHistory)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), before);
        drop(owners);
        for controller in controllers {
            controller.close().await?;
        }
        fixture.close().await?;
    }
    Ok(())
}
