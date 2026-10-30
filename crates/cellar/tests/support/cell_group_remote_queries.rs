// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared catalog dispatch over the production query TLS lane and SQL owners.
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

fn dispatcher(
    fixture: &RuntimeCluster,
    class: Arc<RaftClass>,
    keys: VersionKeys,
) -> Result<CellGroupDispatcher> {
    Ok(CellGroupDispatcher::new(
        &fixture.cluster.domain,
        fixture.cluster.directory.clone(),
        class,
        keys,
        CellDispatchLimits {
            max_invocations: 8,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
    )?)
}

fn keys(tag: u8) -> Result<VersionKeys> {
    Ok(VersionKeys::new(
        VersionSigningKey::new([tag; 32], u64::MAX)?,
        vec![],
        Duration::from_secs(30),
    )?)
}

async fn invocation(
    dispatcher: &CellGroupDispatcher,
    class: &RaftClass,
    cell: &CellHistory,
    consistency: CellQueryConsistency,
    timeout: Duration,
    input: &[u8],
) -> Result<CellQueryInvocation> {
    let mut query = dispatcher.query(
        cell.clone(),
        CellQueryRequest {
            invocation: ProposalId::new([197; 16])?,
            contract: class.contract("Get").context("Get")?,
            input: input.to_vec(),
            consistency,
        },
        CellRoutingLimits {
            timeout,
            max_attempts: 2,
        },
        None,
    )?;
    query.begin().await?;
    Ok(query)
}

async fn gateways(
    fixture: &RuntimeCluster,
    class: Arc<RaftClass>,
) -> Result<Vec<CellQueryGateway>> {
    let mut gateways = Vec::new();
    let group = fixture.cluster.birth.route().history;
    let cell = history(fixture.cluster.birth.route(), "query-options")?;
    for n in 0..3 {
        gateways.push(fixture.transports[n].install_cell_query_gateway(
            dispatcher(fixture, class.clone(), keys(190)?)?,
            fixture.appliers[&(n, group)].0.clone(),
            fixture.cell_options_at(&cell, n).await?,
        )?);
    }
    Ok(gateways)
}

async fn wait_for(mut predicate: impl AsyncFnMut() -> Result<bool>) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !predicate().await? {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

pub(crate) async fn remote_queries() -> Result<()> {
    for rooted in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.query(
            "Get",
            [82; 32],
            move |context, _| {
                count.fetch_add(1, Ordering::SeqCst);
                let QueryVersion::Cell(observation) = context.version() else {
                    anyhow::bail!("physical group cut");
                };
                assert_eq!(context.identity(), &observation.version.history);
                assert_eq!(
                    context.query_row("SELECT n FROM value", [], |r| r.get::<_, i64>(0))?,
                    0
                );
                Ok(context.identity().cell.as_str().as_bytes().to_vec())
            },
        )?);
        let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let mut gateways = gateways(&fixture, class.clone()).await?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        assert_eq!(gateways[0].namespace(), (a.cluster, a.namespace));
        let options = fixture.cell_options(&a).await?;
        assert!(matches!(
            fixture.transports[0].install_cell_query_gateway(
                dispatch.clone(),
                fixture.appliers[&(0, route.history)].0.clone(),
                options.clone()
            ),
            Err(TransportError::Refused)
        ));
        assert!(matches!(
            fixture.transports[0].install_cell_query_gateway(
                dispatch.clone(),
                fixture.appliers[&(1, route.history)].0.clone(),
                options.clone()
            ),
            Err(TransportError::Identity)
        ));
        let source = &fixture.transports[1];
        let leader = MemberId::new(1)?;
        let mut first = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Linearizable,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        let attempt = first.attempt().context("attempt")?.clone();
        let response = first.execute_remote(source, leader).await?;
        assert_eq!(response.result(), a.cell.as_str().as_bytes());
        assert_eq!(response.observation().version.history, a);
        assert_eq!(response.observation().version.sequence.get(), 1);
        assert_eq!(response.served_by(), leader);
        let token = response.token().clone();
        let expected = response.observation().clone();
        assert_eq!(source.usage().query_items, 1);
        assert!(source.usage().query_bytes > response.result().len());
        assert!(matches!(
            first.execute_remote(source, leader).await,
            Err(CellDispatchError::Invalid("consumed route attempt"))
        ));
        drop(response);
        assert_eq!(source.usage().query_items, 0);
        first.retry().await?;
        assert_eq!(
            first.attempt().context("retry")?.deadline_ms(),
            attempt.deadline_ms()
        );
        assert_eq!(first.attempt().context("retry")?.ordinal(), 2);
        assert_eq!(
            *first.execute_remote(source, leader).await?.observation(),
            expected
        );
        assert!(matches!(
            first.retry().await,
            Err(CellDispatchError::Routing(CellRoutingError {
                reason: CellRoutingFailure::AttemptsExhausted,
                ..
            }))
        ));
        drop(first);
        for (cell, member, consistency) in [
            (&a, leader, CellQueryConsistency::AtLeast(token.clone())),
            (&b, MemberId::new(3)?, CellQueryConsistency::Stale),
        ] {
            let mut query = invocation(
                &dispatch,
                &class,
                cell,
                consistency,
                Duration::from_secs(5),
                b"",
            )
            .await?;
            let response = query.execute_remote(source, member).await?;
            assert_eq!(response.result(), cell.cell.as_str().as_bytes());
            assert_eq!(response.observation().version.history, *cell);
            assert_eq!(response.observation().version.sequence.get(), 1);
        }
        let before = calls.load(Ordering::SeqCst);
        let mut wrong = invocation(
            &dispatch,
            &class,
            &b,
            CellQueryConsistency::AtLeast(token),
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert!(matches!(
            wrong.execute_remote(source, leader).await,
            Err(CellDispatchError::Transport(TransportError::Query(
                CommandFailure::WrongIncarnation
            )))
        ));
        drop(wrong);
        let mut wrong_cell = a.clone();
        wrong_cell.incarnation = IncarnationId::new([199; 16])?;
        let mut wrong = invocation(
            &dispatch,
            &class,
            &wrong_cell,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert!(wrong.execute_remote(source, leader).await.is_err());
        drop(wrong);
        assert_eq!(calls.load(Ordering::SeqCst), before);
        // A signature under a different namespace key cannot become a read result.
        let other_keys = dispatcher(&fixture, class.clone(), keys(191)?)?;
        let mut wrong = invocation(
            &other_keys,
            &class,
            &a,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert!(matches!(
            wrong.execute_remote(source, leader).await,
            Err(CellDispatchError::Transport(TransportError::Authentication))
        ));
        drop(wrong);
        // Dropping the host registration refuses new reads; transport does not
        // secretly retain a public handle or keep an abandoned gateway alive.
        drop(gateways.remove(0));
        let before = calls.load(Ordering::SeqCst);
        let mut absent = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert!(absent.execute_remote(source, leader).await.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), before);
        let replacement = fixture.transports[0].install_cell_query_gateway(
            dispatch.clone(),
            fixture.appliers[&(0, route.history)].0.clone(),
            options,
        )?;
        absent.retry().await?;
        assert_eq!(
            absent.execute_remote(source, leader).await?.result(),
            a.cell.as_str().as_bytes()
        );
        drop(absent);
        assert_eq!(dispatch.usage(), (0, 0));
        // Change only the catalog, not physical state: the old destination must
        // refuse the captured route. This is not a live-movement qualification.
        let mut stale = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        let original_deadline = stale.attempt().context("original")?.deadline_ms();
        let directory = fixture.cluster.directory.clone();
        let next = directory.prepare_group_birth(CellGroupBirthSpec {
            history: GroupHistory {
                group: GroupId::new([203; 16])?,
                ..route.history
            },
            range: route.range.clone(),
            epoch: RouteEpoch::new(2)?,
            creation: ProposalId::new([204; 16])?,
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
            ProposalId::new([205; 16])?,
            std::slice::from_ref(&route),
            &[next.route().clone()],
        )?;
        fixture
            .cluster
            .domain
            .run(directory.publish(&publish))
            .await??;
        let before = calls.load(Ordering::SeqCst);
        assert!(matches!(
            stale.execute_remote(source, leader).await,
            Err(CellDispatchError::Transport(TransportError::Query(
                CommandFailure::Unavailable
            )))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), before);
        stale.redirect(next.route()).await?;
        assert_eq!(
            stale.attempt().context("redirect")?.deadline_ms(),
            original_deadline
        );
        assert_eq!(stale.attempt().context("redirect")?.ordinal(), 2);
        assert!(matches!(
            stale.execute_remote(source, leader).await,
            Err(CellDispatchError::Transport(TransportError::UnknownGroup))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), before);
        drop(stale);
        assert_eq!(dispatch.usage(), (0, 0));
        for transport in &fixture.transports {
            assert_eq!(transport.usage().groups, 1);
            assert_eq!(transport.usage().peers, 2);
        }
        for controller in controllers {
            controller.close().await?;
        }
        drop(replacement);
        drop(gateways);
        fixture.close().await?;
    }
    Ok(())
}

pub(crate) async fn remote_query_lifecycle(expire: bool) -> Result<()> {
    for rooted in [false, true] {
        let (entered, started) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered));
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let held = Mutex::new(held);
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.query(
            "Get",
            [82; 32],
            move |context, _| {
                entered
                    .lock()
                    .unwrap()
                    .take()
                    .context("once")?
                    .send(())
                    .map_err(|_| anyhow::anyhow!("entered"))?;
                held.lock().unwrap().recv_timeout(Duration::from_secs(15))?;
                Ok(context
                    .query_row("SELECT n FROM value", [], |r| r.get::<_, i64>(0))?
                    .to_be_bytes()
                    .to_vec())
            },
        )?);
        let (mut fixture, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let gateways = gateways(&fixture, class.clone()).await?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let timeout = if expire {
            Duration::from_millis(600)
        } else {
            Duration::from_secs(10)
        };
        let mut query = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Linearizable,
            timeout,
            b"",
        )
        .await?;
        let source = fixture.transports[1].clone();
        let waiter = fixture.cluster.domain.spawn(async move {
            query
                .execute_remote(&source, MemberId::new(1).unwrap())
                .await
        })?;
        tokio::time::timeout(Duration::from_secs(5), started).await??;
        let pool = fixture.appliers[&(0, route.history)].0.clone();
        assert_eq!(pool.usage()?.retained_operations, 1);
        // The blocked query does not occupy control connections or Raft work.
        let deadline = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::mono_ms() + 1000 })
            .await?;
        let authority = fixture.runtimes[0]
            .read_authority(
                route.history,
                a.clone(),
                CellReadConsistency::Linearizable,
                deadline,
            )
            .await?;
        drop(authority);
        if expire {
            assert!(waiter.await?.is_err());
        } else {
            waiter.abort();
            assert!(waiter.await.is_err());
        }
        wait_for(async || {
            Ok(fixture.transports[1].usage().query_items == 0
                && fixture.transports[0].usage().query_items == 0)
        })
        .await?;
        assert_eq!(pool.usage()?.retained_operations, 1);
        assert_eq!(dispatch.usage(), (0, 0));
        // Cancelling the close waiter cannot release the entered SQL worker.
        assert!(
            tokio::time::timeout(Duration::from_millis(30), controllers[0].close())
                .await
                .is_err()
        );
        assert_eq!(pool.usage()?.running_workers, 1);
        release.send(())?;
        tokio::time::timeout(Duration::from_secs(5), controllers[0].close()).await??;
        assert_eq!(pool.usage()?.retained_operations, 0);
        assert_eq!(pool.usage()?.running_workers, 0);
        for controller in &controllers[1..] {
            controller.close().await?;
        }
        drop(gateways);
        fixture.close().await?;
    }
    Ok(())
}
