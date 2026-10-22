//! Read consistency and authenticated history observations at public boundaries.
#![allow(clippy::disallowed_methods)]
#[allow(dead_code)]
#[path = "support/raft_cluster.rs"]
mod cluster;
#[path = "support/raft_read_context.rs"]
mod ordinary_context;
#[allow(dead_code)]
#[path = "support/raft_node.rs"]
mod public_node;
#[path = "support/raft_readiness_native.rs"]
mod readiness;
#[path = "support/raft_catalog_fault.rs"]
mod store;

use anyhow::{Context as _, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use cellar::sdk::consensus::{verification::*, *};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

type Query = dyn for<'a> Fn(&QueryContext<'a>, &[u8]) -> Result<Vec<u8>> + Send + Sync;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_readiness_reports_voters_learners_and_empty_image() -> Result<()> {
    readiness::voter_learner_and_empty().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_readiness_distinguishes_historical_and_recent_source() -> Result<()> {
    readiness::historical_without_recent_source().await
}

fn counter(query: Arc<Query>) -> Result<public_node::Counter> {
    counter_query(Some(query))
}

fn counter_query(query: Option<Arc<Query>>) -> Result<public_node::Counter> {
    use cellar::sdk::{Output, Registry};
    let class = RaftClass::new("Counter", 1, [21; 32])?
        .command("Increment", [22; 32], |context, input| {
            let delta = i64::from_be_bytes(input.try_into()?);
            context.sql().execute_batch("CREATE TABLE IF NOT EXISTS counter(value INTEGER); INSERT INTO counter SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM counter)")?;
            context.sql().execute("UPDATE counter SET value=value+?1", [delta])?;
            let value: i64 = context.sql().query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?;
    let class = if let Some(query) = query {
        class.query("Get", [23; 32], move |context, input| query(context, input))?
    } else {
        class
    };
    Ok(public_node::Counter {
        schema: class.schema_digest(),
        increment: class.contract("Increment").unwrap(),
        get: OperationContract {
            operation: "Get".into(),
            kind: OperationKind::Query,
            digest: [23; 32],
        },
        application: Arc::new(
            Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
        ),
    })
}

fn get(context: &QueryContext<'_>, _: &[u8]) -> Result<Vec<u8>> {
    let value: i64 = context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
    Ok(value.to_be_bytes().to_vec())
}

fn at_least(receipt: &CommandReceipt) -> QueryOptions {
    QueryOptions {
        consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
        ..Default::default()
    }
}

struct Fixture {
    cluster: cluster::Cluster,
    handles: Vec<RaftCellHandle>,
    definition: public_node::Counter,
    initial: CommandReceipt,
}

impl Fixture {
    async fn start(query: Arc<Query>) -> Result<Self> {
        Self::start_with_host(
            query,
            RaftHostOptions {
                stale_source_timeout_ms: 200,
                ..Default::default()
            },
        )
        .await
    }

    async fn start_with_host(query: Arc<Query>, host: RaftHostOptions) -> Result<Self> {
        let definitions = (0..3)
            .map(|_| counter(query.clone()))
            .collect::<Result<Vec<_>>>()?;
        Self::start_definitions(definitions, host).await
    }

    async fn start_definitions(
        definitions: Vec<public_node::Counter>,
        host: RaftHostOptions,
    ) -> Result<Self> {
        let namespace = NamespaceConfig::new(
            scope().cluster,
            scope().namespace,
            NamespaceMode::Raft,
            definitions[0].schema,
        );
        let cluster = cluster::Cluster::start_with_host(
            &definitions,
            vec![RaftCellRegistration {
                placement: cellar::sdk::consensus::RaftCellPlacement::Replica,
                namespace,
                class: "Counter".into(),
                birth: BirthProposal {
                    scope: scope(),
                    voters: (1..=3)
                        .map(|i| {
                            Ok(InitialMember {
                                member: MemberId::new(i)?,
                                node: NodeId::new([i as u8; 16])?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    learners: vec![],
                },
            }],
            20,
            host,
        )
        .await?;
        let handles = cluster
            .services
            .iter()
            .map(|service| service.cell(scope().namespace, &scope().cell))
            .collect::<Result<Vec<_>, _>>()?;
        let leader = leader(&handles, None).await?;
        let initial = handles[leader]
            .command(
                definitions[0].increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                public_node::options(1)?,
            )
            .await?;
        for (index, handle) in handles.iter().enumerate() {
            if definitions[index]
                .application
                .resolve_raft_class("Counter")?
                .validate_operation(&definitions[0].get)
                .is_err()
            {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while !handle
                        .status()
                        .await
                        .is_ok_and(|status| status.applied.index >= initial.version.position.index)
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await?;
                continue;
            }
            let result = handle
                .query(definitions[0].get.clone(), vec![], at_least(&initial))
                .await?;
            assert_eq!(result.result, 1_i64.to_be_bytes());
        }
        let definition = definitions.into_iter().next().unwrap();
        Ok(Self {
            cluster,
            handles,
            definition,
            initial,
        })
    }

    async fn write(&self, leader: usize, sequence: u64, delta: i64) -> Result<CommandReceipt> {
        Ok(self.handles[leader]
            .command(
                self.definition.increment.clone(),
                delta.to_be_bytes().to_vec(),
                public_node::options(sequence)?,
            )
            .await?)
    }

    async fn close(self) -> Result<()> {
        self.cluster.close().await
    }
}

async fn leader(handles: &[RaftCellHandle], excluded: Option<usize>) -> Result<usize> {
    Ok(tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            for (i, handle) in handles.iter().enumerate() {
                if Some(i) != excluded
                    && handle
                        .status()
                        .await
                        .is_ok_and(|status| status.command_ready)
                {
                    return i;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn read_your_writes_follower() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let primary = leader(&fixture.handles, None).await?;
    let follower = (primary + 1) % 3;
    fixture.cluster.isolate(follower, false).await?;
    let written = fixture.write(primary, 2, 4).await?;
    assert_eq!(written.result, 5_i64.to_be_bytes());
    let pending =
        fixture.handles[follower].query(fixture.definition.get.clone(), vec![], at_least(&written));
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    let historical = fixture.handles[follower]
        .query(
            fixture.definition.get.clone(),
            vec![],
            QueryOptions {
                stale_policy: StaleReadPolicy::DisconnectedHistorical,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(historical.result, 1_i64.to_be_bytes());
    fixture.cluster.heal();
    let read = pending.await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert!(read.version.covers(&written.version)?);
    fixture.close().await
}

fn routed(receipt: &CommandReceipt, member: usize, fallback: bool) -> QueryOptions {
    QueryOptions {
        routing: QueryRouting::Replicas {
            members: vec![MemberId::new(member as u64 + 1).unwrap()],
            attempt_timeout: Duration::from_millis(180),
            leader_fallback: fallback,
        },
        timeout: Duration::from_secs(2),
        ..at_least(receipt)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn fallback_preserves_contract() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get))
        .await
        .context("fallback fixture startup")?;
    let primary = leader(&fixture.handles, None)
        .await
        .context("fallback initial leader")?;
    let source = (primary + 1) % 3;
    let target = (primary + 2) % 3;
    fixture.cluster.isolate_consensus(target).await?;
    let newer = fixture
        .write(primary, 2, 4)
        .await
        .context("fallback committed command")?;
    assert_eq!(newer.result, 5_i64.to_be_bytes());
    // A real RPC can reach the isolated replica's older image. It must wait
    // for the exact token rather than answer its stale value of one.
    assert!(matches!(
        fixture.handles[source]
            .query(
                fixture.definition.get.clone(),
                vec![],
                routed(&newer, target, false)
            )
            .await,
        Err(CommandFailure::VersionUnavailable)
    ));
    let read = fixture.handles[source]
        .query(
            fixture.definition.get.clone(),
            vec![],
            routed(&newer, target, true),
        )
        .await
        .context("minimum-version leader fallback")?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert_eq!(read.served_by, MemberId::new(primary as u64 + 1)?);
    assert!(read.version.covers(&newer.version)?);
    let mut strong = routed(&newer, target, true);
    strong.consistency = QueryConsistency::Linearizable;
    let read = fixture.handles[source]
        .query(fixture.definition.get.clone(), vec![], strong)
        .await
        .context("linearizable leader fallback")?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert_eq!(read.served_by, MemberId::new(primary as u64 + 1)?);
    // The candidate's longer attempt budget and fallback cannot restart the
    // original invocation deadline. Both nodes must release cancelled work.
    let mut short = routed(&newer, target, true);
    short.timeout = Duration::from_millis(100);
    short.routing = QueryRouting::Replicas {
        members: vec![MemberId::new(target as u64 + 1)?],
        attempt_timeout: Duration::from_secs(1),
        leader_fallback: true,
    };
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_millis(500),
            fixture.handles[source].query(fixture.definition.get.clone(), vec![], short)
        )
        .await?,
        Err(CommandFailure::VersionUnavailable)
    ));
    tokio::time::timeout(Duration::from_millis(500), async {
        while [source, target].iter().any(|i| {
            fixture.cluster.nodes[*i]
                .services()
                .application_work()
                .pending_events
                != 0
        }) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    fixture.cluster.heal();
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn remote_query_wait_cannot_block_its_ltx_catchup() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let primary = leader(&fixture.handles, None).await?;
    let source = (primary + 1) % 3;
    let target = (primary + 2) % 3;
    fixture.cluster.isolate_consensus(target).await?;
    let newer = fixture.write(primary, 2, 4).await?;
    let mut options = routed(&newer, target, false);
    options.timeout = Duration::from_secs(5);
    options.routing = QueryRouting::Replicas {
        members: vec![MemberId::new(target as u64 + 1)?],
        attempt_timeout: Duration::from_secs(5),
        leader_fallback: false,
    };
    let query = fixture.handles[source].query(fixture.definition.get.clone(), vec![], options);
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture.cluster.nodes[target]
            .services()
            .application_work()
            .pending_events
            == 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    fixture.cluster.heal();
    let read = query.await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert_eq!(read.served_by, MemberId::new(target as u64 + 1)?);
    assert!(read.version.covers(&newer.version)?);
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn saturated_query_peer_refuses_and_expires_queued_work() -> Result<()> {
    use std::sync::atomic::AtomicUsize;
    let (gate, mut entered) = public_node::Gate::new();
    let release = public_node::Release(gate.clone());
    let unexpected = Arc::new(AtomicUsize::new(0));
    let executed = unexpected.clone();
    let fixture = Fixture::start(Arc::new(move |context, input| {
        if input == b"queued" {
            executed.fetch_add(1, Ordering::SeqCst);
        }
        if input == b"hold" {
            gate.wait()?;
        }
        get(context, input)
    }))
    .await?;
    let primary = leader(&fixture.handles, None).await?;
    let source = (primary + 1) % 3;
    let target = (primary + 2) % 3;
    let options = |timeout| QueryOptions {
        timeout,
        routing: QueryRouting::Replicas {
            members: vec![MemberId::new(target as u64 + 1).unwrap()],
            attempt_timeout: timeout,
            leader_fallback: false,
        },
        ..at_least(&fixture.initial)
    };
    // Hold the real peer connection in its destination callback. Later calls
    // must consume finite peer bytes while queued behind this invocation.
    let first = fixture.handles[source].query(
        fixture.definition.get.clone(),
        b"hold".to_vec(),
        options(Duration::from_secs(5)),
    );
    tokio::time::timeout(Duration::from_secs(2), entered.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("remote callback did not enter"))?;
    let source_usage = fixture.cluster.services[source]
        .resource_usage()
        .transport
        .context("source transport observer")?;
    let target_usage = fixture.cluster.services[target]
        .resource_usage()
        .transport
        .context("target transport observer")?;
    assert!(source_usage.outgoing.query.items > 0 && source_usage.outgoing.query.bytes > 0);
    assert!(target_usage.incoming.query.items > 0 && target_usage.incoming.query.bytes > 0);
    assert!(source_usage.outgoing_connections.query > 0);
    assert!(target_usage.incoming_connections.query > 0);
    assert!(
        target_usage.incoming_connections.query <= target_usage.incoming_connection_limits.query
    );
    assert!(source_usage.outgoing.query.bytes <= source_usage.outgoing.query.max_bytes);
    assert!(target_usage.incoming.query.bytes <= target_usage.incoming.query.max_bytes);
    let queued: Vec<_> = (0..64)
        .map(|_| {
            fixture.handles[source].query(
                fixture.definition.get.clone(),
                b"queued".to_vec(),
                options(Duration::from_secs(1)),
            )
        })
        .collect();
    let mut overloaded = 0;
    let mut expired = 0;
    tokio::time::timeout(Duration::from_secs(2), async {
        for response in queued {
            match response.await {
                Err(CommandFailure::Overloaded) => overloaded += 1,
                Err(CommandFailure::VersionUnavailable) => expired += 1,
                result => anyhow::bail!("unexpected queued response: {result:?}"),
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert!(overloaded > 0, "peer saturation was not observed");
    assert!(
        expired > 0,
        "no admitted request expired behind the held RPC"
    );
    assert_eq!(unexpected.load(Ordering::SeqCst), 0);
    // Consensus and LTX use separate connections/capacity. They must publish a
    // new command while the first query and expired peer queue are still held.
    let newer = fixture.write(primary, 2, 4).await?;
    assert_eq!(newer.result, 5_i64.to_be_bytes());
    release.0.release();
    let first = first.await?;
    assert_eq!(first.result, 1_i64.to_be_bytes());
    assert_eq!(first.served_by, MemberId::new(target as u64 + 1)?);
    assert!(first.version.position.index < newer.version.position.index);
    let mut fresh = options(Duration::from_secs(2));
    fresh.consistency = QueryConsistency::AtLeast(newer.version_token.clone());
    let read = fixture.handles[source]
        .query(fixture.definition.get.clone(), vec![], fresh)
        .await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert!(read.version.covers(&newer.version)?);
    assert_eq!(
        unexpected.load(Ordering::SeqCst),
        0,
        "expired queued request executed after the peer resumed"
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture
            .cluster
            .nodes
            .iter()
            .any(|node| node.services().application_work().pending_events != 0)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn delayed_tcp_query_reply_cannot_settle_a_later_read() -> Result<()> {
    let (gate, mut entered) = public_node::Gate::new();
    let release = public_node::Release(gate.clone());
    let fixture = Fixture::start(Arc::new(move |context, input| {
        if input == b"hold" {
            gate.wait()?;
        }
        get(context, input)
    }))
    .await?;
    let primary = leader(&fixture.handles, None).await?;
    let source = (primary + 1) % 3;
    let target = (primary + 2) % 3;
    let options = QueryOptions {
        timeout: Duration::from_secs(1),
        routing: QueryRouting::Replicas {
            members: vec![MemberId::new(target as u64 + 1)?],
            attempt_timeout: Duration::from_secs(1),
            leader_fallback: false,
        },
        ..at_least(&fixture.initial)
    };
    // Warm the pooled TLS connection before holding server-to-client bytes,
    // so this history delays a query response rather than its handshake.
    let warm = fixture.handles[source]
        .query(fixture.definition.get.clone(), vec![], options.clone())
        .await?;
    assert_eq!(warm.served_by, MemberId::new(target as u64 + 1)?);
    let delayed = fixture.handles[source].query(
        fixture.definition.get.clone(),
        b"hold".to_vec(),
        options.clone(),
    );
    tokio::time::timeout(Duration::from_secs(2), entered.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("remote query missing"))?;
    let held = fixture.cluster.hold_query_responses(source, target)?;
    release.0.release();
    tokio::time::timeout(Duration::from_millis(750), async {
        while fixture.cluster.held_query_response_bytes(source, target) == 0
            || fixture.cluster.nodes[target]
                .services()
                .application_work()
                .pending_events
                != 0
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let newer = fixture.write(primary, 2, 4).await?;
    assert_eq!(newer.result, 5_i64.to_be_bytes());
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), delayed).await?,
        Err(CommandFailure::VersionUnavailable)
    ));
    // Dropping the timed-out connection discards its delayed ciphertext. A
    // later attempt must perform its own exchange and cover the newer token.
    drop(held);
    let read = fixture.handles[source]
        .query(
            fixture.definition.get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(newer.version_token.clone()),
                ..options
            },
        )
        .await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert_eq!(read.served_by, MemberId::new(target as u64 + 1)?);
    assert!(read.version.covers(&newer.version)?);
    tokio::time::timeout(Duration::from_secs(2), async {
        while fixture
            .cluster
            .nodes
            .iter()
            .any(|node| node.services().application_work().pending_events != 0)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn voter_without_query_code_can_route_to_a_compatible_replica() -> Result<()> {
    let definitions = vec![
        counter_query(None)?,
        counter(Arc::new(get))?,
        counter(Arc::new(get))?,
    ];
    assert_eq!(definitions[0].schema, definitions[1].schema);
    let fixture = Fixture::start_definitions(definitions, RaftHostOptions::default()).await?;
    readiness::without_query_code(&fixture.handles[0]).await?;
    assert!(matches!(
        fixture.handles[0]
            .query(
                fixture.definition.get.clone(),
                vec![],
                at_least(&fixture.initial)
            )
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    let mut options = at_least(&fixture.initial);
    options.routing = QueryRouting::Replicas {
        members: vec![MemberId::new(1)?, MemberId::new(2)?],
        attempt_timeout: Duration::from_millis(500),
        leader_fallback: false,
    };
    let read = fixture.handles[0]
        .query(fixture.definition.get.clone(), vec![], options)
        .await?;
    assert_eq!(read.result, 1_i64.to_be_bytes());
    assert_eq!(read.served_by, MemberId::new(2)?);
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn nonmember_router_reads_only_granted_namespaces_without_local_state() -> Result<()> {
    let mut definitions = (0..3)
        .map(|_| counter(Arc::new(get)))
        .collect::<Result<Vec<_>>>()?;
    definitions.push(counter_query(None)?);
    let first = scope();
    let mut second = first.clone();
    second.namespace = NamespaceId::new([8; 16])?;
    second.group = GroupId::new([9; 16])?;
    let registrations = [&first, &second]
        .into_iter()
        .map(|scope| {
            Ok(RaftCellRegistration {
                placement: RaftCellPlacement::Replica,
                namespace: NamespaceConfig::new(
                    scope.cluster,
                    scope.namespace,
                    NamespaceMode::Raft,
                    definitions[0].schema,
                ),
                class: "Counter".into(),
                birth: BirthProposal {
                    scope: scope.clone(),
                    voters: (1..=3)
                        .map(|member| {
                            Ok(InitialMember {
                                node: NodeId::new([member as u8; 16])?,
                                member: MemberId::new(member)?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    learners: vec![],
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let cluster = cluster::Cluster::start_with_router(
        &definitions,
        registrations,
        20,
        RaftHostOptions::default(),
        &[(first.cluster, first.namespace)],
    )
    .await?;
    let members = cluster
        .services
        .iter()
        .take(3)
        .map(|service| service.cell(first.namespace, &first.cell))
        .collect::<Result<Vec<_>, _>>()?;
    let primary = leader(&members, None).await?;
    let initial = members[primary]
        .command(
            definitions[0].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(1)?,
        )
        .await?;
    let other_members = cluster
        .services
        .iter()
        .take(3)
        .map(|service| service.cell(second.namespace, &second.cell))
        .collect::<Result<Vec<_>, _>>()?;
    let other_primary = leader(&other_members, None).await?;
    other_members[other_primary]
        .command(
            definitions[0].increment.clone(),
            37_i64.to_be_bytes().to_vec(),
            public_node::options(1)?,
        )
        .await?;
    let router = cluster.services[3].cell(first.namespace, &first.cell)?;
    let other = cluster.services[3].cell(second.namespace, &second.cell)?;
    let options = |consistency| QueryOptions {
        consistency,
        routing: QueryRouting::Replicas {
            members: vec![MemberId::new(2).unwrap()],
            attempt_timeout: Duration::from_millis(180),
            leader_fallback: true,
        },
        timeout: Duration::from_secs(3),
        ..Default::default()
    };
    for consistency in [
        QueryConsistency::Stale,
        QueryConsistency::AtLeast(initial.version_token.clone()),
        QueryConsistency::Linearizable,
    ] {
        let read = router
            .query(definitions[0].get.clone(), vec![], options(consistency))
            .await?;
        assert_eq!(read.result, 1_i64.to_be_bytes());
        assert!(read.version.covers(&initial.version)?);
        assert!(read.served_by == MemberId::new(2)? || read.served_by == MemberId::new(1)?);
    }
    assert!(matches!(
        router
            .command(
                definitions[0].increment.clone(),
                99_i64.to_be_bytes().to_vec(),
                public_node::options(2)?
            )
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    assert!(matches!(
        router.status().await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    assert!(matches!(
        router
            .query(definitions[0].get.clone(), vec![], QueryOptions::default())
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    // The other history is real and queryable, but this physical origin has
    // no permission there. Neither local execution nor fallback can grant it.
    assert!(matches!(
        other
            .query(
                definitions[0].get.clone(),
                vec![],
                options(QueryConsistency::Stale)
            )
            .await,
        Err(CommandFailure::VersionUnavailable | CommandFailure::Unavailable)
    ));
    assert!(matches!(
        other
            .query(
                definitions[0].get.clone(),
                vec![],
                options(QueryConsistency::AtLeast(initial.version_token.clone()))
            )
            .await,
        Err(CommandFailure::WrongIncarnation)
    ));
    // Member 2 remains reachable for queries but loses consensus catch-up.
    // The router has no local status/log; its hint may name any live replica,
    // which still must independently obtain the requested read proof.
    cluster.isolate_consensus(1).await?;
    let primary = leader(&members, Some(1)).await?;
    let newer = members[primary]
        .command(
            definitions[0].increment.clone(),
            4_i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )
        .await?;
    let read = router
        .query(
            definitions[0].get.clone(),
            vec![],
            options(QueryConsistency::AtLeast(newer.version_token.clone())),
        )
        .await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert_eq!(read.served_by, MemberId::new(1)?);
    assert!(read.version.covers(&newer.version)?);
    let read = router
        .query(
            definitions[0].get.clone(),
            vec![],
            options(QueryConsistency::Linearizable),
        )
        .await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert_eq!(read.served_by, MemberId::new(1)?);
    cluster.heal();
    assert!(!cluster.nodes[3].info().data_dir.join("raft-cells").exists());
    assert!(cluster.services[3].close().await?.unfinished.is_empty());
    // Inspect the actual durable engine after join: querying must not have
    // fabricated a voter, learner or group, including for the denied namespace.
    let domain = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                cluster.nodes[3].info().data_dir.join("raft"),
                NodeId::new([4; 16])?,
                DiskLineage::new([13; 16])?,
                ProcessGeneration::new([99; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    assert!(log.groups()?.is_empty());
    log.close().await?;
    domain.shutdown().await;
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn lost_read_index_response_retries_without_leader_change() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let primary = leader(&fixture.handles, None).await?;
    let follower = (primary + 1) % 3;
    fixture.handles[follower]
        .query(
            fixture.definition.get.clone(),
            vec![],
            at_least(&fixture.initial),
        )
        .await?;
    let before = fixture.handles[follower].status().await?;
    fixture
        .cluster
        .block_control(primary, follower, true)
        .await?;
    let pending = fixture.handles[follower].query(
        fixture.definition.get.clone(),
        vec![],
        QueryOptions {
            consistency: QueryConsistency::Linearizable,
            timeout: Duration::from_secs(2),
            ..Default::default()
        },
    );
    // The request can reach the leader, and the third voter supplies its safe
    // ReadIndex majority. Only the reply direction is unavailable. Heal before
    // the follower's minimum election timeout, keeping the term/leader fixed.
    tokio::time::sleep(Duration::from_millis(200)).await;
    fixture
        .cluster
        .block_control(primary, follower, false)
        .await?;
    let result = pending.await;
    let after = fixture.handles[follower].status().await?;
    assert_eq!(after.term, before.term);
    assert_eq!(after.leader, before.leader);
    fixture.close().await?;
    assert_eq!(
        result
            .context("a lost ReadIndex response must be retried under the original invocation")?
            .result,
        1i64.to_be_bytes()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn linearizable_read_barrier() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let old = leader(&fixture.handles, None).await?;
    let written = fixture.write(old, 2, 4).await?;
    let options = QueryOptions {
        consistency: QueryConsistency::Linearizable,
        ..Default::default()
    };
    for (i, handle) in fixture.handles.iter().enumerate() {
        let read = handle
            .query(fixture.definition.get.clone(), vec![], options.clone())
            .await
            .with_context(|| format!("initial linearizable read on replica {i}"))?;
        assert_eq!(read.result, 5_i64.to_be_bytes());
        assert!(read.version.covers(&written.version)?);
    }
    fixture.cluster.isolate(old, false).await?;
    let next = leader(&fixture.handles, Some(old)).await?;
    // No data command follows the election. Its protocol-only barrier must be
    // covered by the same pinned image and returned as the served position.
    let read = fixture.handles[next]
        .query(fixture.definition.get.clone(), vec![], options.clone())
        .await
        .context("new leader protocol-only cut")?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    assert!(read.version.position.index > written.version.position.index);
    assert!(read.version.position.term > written.version.position.term);
    let newer = fixture.write(next, 3, 7).await?;
    assert_eq!(newer.result, 12_i64.to_be_bytes());
    for (i, handle) in fixture
        .handles
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != old)
    {
        let read = handle
            .query(fixture.definition.get.clone(), vec![], options.clone())
            .await
            .with_context(|| format!("post-election linearizable read on replica {i}"))?;
        assert_eq!(read.result, 12_i64.to_be_bytes(), "replica {i}");
        assert!(read.version.covers(&newer.version)?);
    }
    fixture.cluster.heal();
    let read = fixture.handles[old]
        .query(fixture.definition.get.clone(), vec![], options)
        .await
        .context("healed former leader linearizable read")?;
    assert_eq!(read.result, 12_i64.to_be_bytes());
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn minority_strong_read_refused() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let primary = leader(&fixture.handles, None).await?;
    let follower = (primary + 1) % 3;
    fixture.cluster.isolate(follower, false).await?;
    let newer = fixture.write(primary, 2, 4).await?;
    let strong = QueryOptions {
        consistency: QueryConsistency::Linearizable,
        timeout: Duration::from_millis(250),
        ..Default::default()
    };
    assert!(matches!(
        fixture.handles[follower]
            .query(fixture.definition.get.clone(), vec![], strong)
            .await,
        Err(CommandFailure::VersionUnavailable)
    ));
    let mut minimum = at_least(&newer);
    minimum.timeout = Duration::from_millis(250);
    assert!(matches!(
        fixture.handles[follower]
            .query(fixture.definition.get.clone(), vec![], minimum)
            .await,
        Err(CommandFailure::VersionUnavailable)
    ));
    // A minimum-version read can legitimately use an already-covered prefix
    // without quorum; it must not be confused with a fresh linearizable read.
    assert_eq!(
        fixture.handles[follower]
            .query(
                fixture.definition.get.clone(),
                vec![],
                at_least(&fixture.initial)
            )
            .await?
            .result,
        1_i64.to_be_bytes()
    );
    assert!(matches!(
        fixture.handles[follower]
            .query(
                fixture.definition.get.clone(),
                vec![],
                QueryOptions::default()
            )
            .await,
        Err(CommandFailure::VersionUnavailable)
    ));
    assert_eq!(
        fixture.handles[follower]
            .query(
                fixture.definition.get.clone(),
                vec![],
                QueryOptions {
                    stale_policy: StaleReadPolicy::DisconnectedHistorical,
                    ..Default::default()
                }
            )
            .await?
            .result,
        1_i64.to_be_bytes()
    );
    fixture.cluster.heal();
    assert_eq!(
        fixture.handles[follower]
            .query(fixture.definition.get.clone(), vec![], at_least(&newer))
            .await?
            .result,
        5_i64.to_be_bytes()
    );
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn stale_is_committed_prefix() -> Result<()> {
    let (gate, mut entered) = public_node::Gate::new();
    let _release = public_node::Release(gate.clone());
    let query = Arc::new(move |context: &QueryContext<'_>, input: &[u8]| {
        let mut value = get(context, input)?;
        if input == b"hold" {
            let index = context.version().position.index.get();
            gate.wait()?;
            assert_eq!(get(context, input)?, value);
            value.extend_from_slice(&index.to_be_bytes());
        }
        Ok(value)
    });
    let fixture = Fixture::start(query).await?;
    let primary = leader(&fixture.handles, None).await?;
    let follower = (primary + 1) % 3;
    let pending = fixture.handles[follower].query(
        fixture.definition.get.clone(),
        b"hold".to_vec(),
        QueryOptions {
            stale_policy: StaleReadPolicy::DisconnectedHistorical,
            ..Default::default()
        },
    );
    tokio::time::timeout(Duration::from_secs(5), entered.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("query did not pin"))?;
    let newer = fixture.write(primary, 2, 4).await?;
    assert_eq!(newer.result, 5_i64.to_be_bytes());
    _release.0.release();
    let read = pending.await?;
    assert_eq!(&read.result[..8], &1_i64.to_be_bytes());
    assert_eq!(
        u64::from_be_bytes(read.result[8..].try_into()?),
        read.version.position.index.get()
    );
    assert!(read.version.position.index < newer.version.position.index);
    let latest = fixture.handles[follower]
        .query(fixture.definition.get.clone(), vec![], at_least(&newer))
        .await?;
    assert_eq!(latest.result, 5_i64.to_be_bytes());
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn query_write_capability_denied() -> Result<()> {
    let query = Arc::new(|context: &QueryContext<'_>, input: &[u8]| {
        if input == b"attack" {
            for sql in [
                "UPDATE counter SET value=999 RETURNING value",
                "CREATE TABLE injected(value INTEGER)",
                "PRAGMA journal_mode=WAL",
                "ATTACH ':memory:' AS injected",
                "DELETE FROM counter RETURNING value",
            ] {
                let error = context
                    .query_row(sql, [], |row| row.get::<_, i64>(0))
                    .unwrap_err();
                let sqlite = error
                    .downcast_ref::<rusqlite::Error>()
                    .ok_or_else(|| anyhow::anyhow!("missing SQLite refusal: {error}"))?;
                assert!(
                    matches!(sqlite, rusqlite::Error::SqliteFailure(error, _) if error.code == rusqlite::ErrorCode::AuthorizationForStatementDenied)
                );
            }
        }
        get(context, input)
    });
    let fixture = Fixture::start(query).await?;
    for handle in &fixture.handles {
        let result = handle
            .query(
                fixture.definition.get.clone(),
                b"attack".to_vec(),
                at_least(&fixture.initial),
            )
            .await?;
        assert_eq!(result.result, 1_i64.to_be_bytes());
    }
    let primary = leader(&fixture.handles, None).await?;
    assert_eq!(
        fixture.write(primary, 2, 4).await?.result,
        5_i64.to_be_bytes()
    );
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn unpolled_query_deadline_interrupts_sql_and_joins_work() -> Result<()> {
    let (entered, mut receive) = tokio::sync::mpsc::unbounded_channel();
    let query = Arc::new(move |context: &QueryContext<'_>, input: &[u8]| {
        if input == b"slow" {
            entered.send(())?;
            let _: i64 = context.query_row("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n", [], |row| row.get(0))?;
        }
        get(context, input)
    });
    let fixture = Fixture::start(query).await?;
    let primary = leader(&fixture.handles, None).await?;
    let pending = fixture.handles[primary].query(
        fixture.definition.get.clone(),
        b"slow".to_vec(),
        QueryOptions {
            timeout: Duration::from_millis(150),
            ..at_least(&fixture.initial)
        },
    );
    tokio::time::timeout(Duration::from_secs(5), receive.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("query missing"))?;
    tokio::time::timeout(Duration::from_secs(3), async {
        while fixture.cluster.nodes[primary]
            .services()
            .application_work()
            .pending_events
            != 0
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert!(matches!(
        pending.await,
        Err(CommandFailure::VersionUnavailable)
    ));
    assert_eq!(
        fixture.handles[primary]
            .query(
                fixture.definition.get.clone(),
                vec![],
                at_least(&fixture.initial)
            )
            .await?
            .result,
        1_i64.to_be_bytes()
    );
    fixture.close().await
}

fn scope() -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:one".into(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    }
}

fn version() -> CommittedVersion {
    CommittedVersion {
        scope: scope(),
        position: LogPosition {
            index: LogIndex::new(7),
            term: Term::new(2),
        },
    }
}

fn keys(secret: u8, until: u64, retained: Vec<VersionVerificationKey>) -> Result<VersionKeys> {
    Ok(VersionKeys::new(
        VersionSigningKey::new([secret; 32], until)?,
        retained,
        Duration::from_millis(1000),
    )?)
}

#[test]
fn query_token_auth_scope() -> Result<()> {
    let ring = keys(11, 10000, vec![])?;
    let expected = version();
    let token = ring.issue(&expected, [7; 32], 1000)?;
    assert_eq!(
        ring.verify(&token, &expected.scope, [7; 32], 1000)?,
        expected
    );
    assert_eq!(VersionToken::from_encoded(token.as_str())?, token);
    for mismatch in 0..5 {
        let mut other = expected.scope.clone();
        match mismatch {
            0 => other.cluster = ClusterId::new([99; 16])?,
            1 => other.namespace = NamespaceId::new([99; 16])?,
            2 => other.cell = "Counter:other".into(),
            3 => other.group = GroupId::new([99; 16])?,
            _ => other.incarnation = IncarnationId::new([99; 16])?,
        }
        assert_eq!(
            ring.verify(&token, &other, [7; 32], 1000),
            Err(VersionError::WrongHistory)
        );
    }
    assert_eq!(
        ring.verify(&token, &scope(), [8; 32], 1000),
        Err(VersionError::WrongSchema)
    );
    assert_eq!(
        ring.verify(&token, &scope(), [7; 32], 999),
        Err(VersionError::Expired)
    );
    assert_eq!(
        ring.verify(&token, &scope(), [7; 32], 2000),
        Err(VersionError::Expired)
    );
    assert!(keys(12, 10000, vec![])?
        .verify(&token, &scope(), [7; 32], 1000)
        .is_err());
    let raw = URL_SAFE_NO_PAD.decode(token.as_str())?;
    // Alter every byte of an otherwise valid observation: framing, identity,
    // position, timestamp, key ID and signature must all be protected.
    for offset in 0..raw.len() {
        let mut forged = raw.clone();
        forged[offset] ^= 1;
        if let Ok(forged) = VersionToken::from_encoded(&URL_SAFE_NO_PAD.encode(forged)) {
            assert!(
                ring.verify(&forged, &scope(), [7; 32], 1000).is_err(),
                "accepted modified byte {offset}"
            );
        }
    }
    assert!(VersionToken::from_encoded(&"a".repeat(100000)).is_err());
    assert!(VersionToken::from_encoded(&format!("{}=", token.as_str())).is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn completed_unpolled_responses_keep_their_byte_budget() -> Result<()> {
    let fixture = Fixture::start_with_host(
        Arc::new(|context, input| {
            if input == b"large" {
                Ok(vec![17; 60000])
            } else {
                get(context, input)
            }
        }),
        RaftHostOptions {
            max_pending_bytes: 1024 * 1024,
            max_input_bytes: 4096,
            ..Default::default()
        },
    )
    .await?;
    let primary = leader(&fixture.handles, None).await?;
    let mut responses = Vec::new();
    // These requests complete on the node, but their futures remain unpolled.
    // Sixteen retained payloads fit; admitting a seventeenth payload must not
    // reuse byte capacity released merely by producer completion.
    for _ in 0..16 {
        responses.push(fixture.handles[primary].query(
            fixture.definition.get.clone(),
            b"large".to_vec(),
            at_least(&fixture.initial),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while fixture.cluster.nodes[primary]
                .services()
                .application_work()
                .pending_events
                != 0
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
    }
    assert!(matches!(
        fixture.handles[primary]
            .query(
                fixture.definition.get.clone(),
                b"large".to_vec(),
                at_least(&fixture.initial)
            )
            .await,
        Err(CommandFailure::Overloaded)
    ));
    // Consume one charged result and free enough space for exactly one more.
    let read = responses.remove(0).await?;
    assert_eq!(read.result, vec![17; 60000]);
    assert_eq!(
        fixture.handles[primary]
            .query(
                fixture.definition.get.clone(),
                b"large".to_vec(),
                at_least(&fixture.initial)
            )
            .await?
            .result,
        vec![17; 60000]
    );
    drop(responses);
    fixture.close().await
}

#[tokio::test]
async fn version_key_rotation_survives_restart_and_ambiguous_catalog_write() -> Result<()> {
    let store = Arc::new(store::Store::default());
    let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
    let namespace = catalog
        .open_namespace(NamespaceConfig::new(
            scope().cluster,
            scope().namespace,
            NamespaceMode::Raft,
            [7; 32],
        ))
        .await?;
    let old = keys(11, 10000, vec![])?;
    store.fail_after.store(true, Ordering::Release);
    namespace.require_version_keys(&old, 1000).await?;
    let token = old.issue(&version(), [7; 32], 1000)?;
    let next_key = VersionSigningKey::new([12; 32], 20000)?;
    // Verifier-first rolling deployment: advertise both keys while the old
    // signer is active, then switch to the new signer with identical trust.
    let distributed = keys(11, 10000, vec![next_key.verification_key()])?;
    store.fail_confirmation.store(true, Ordering::Release);
    assert!(namespace
        .require_version_keys(&distributed, 1100)
        .await
        .is_err());
    namespace.require_version_keys(&distributed, 1100).await?;
    let next = VersionKeys::new(
        next_key,
        old.verification_keys(),
        Duration::from_millis(1000),
    )?;
    namespace.require_version_keys(&next, 1100).await?;
    assert_eq!(next.verify(&token, &scope(), [7; 32], 1100)?, version());
    assert!(namespace.require_version_keys(&old, 1100).await.is_err());
    let dropped = keys(12, 20000, vec![])?;
    assert!(namespace
        .require_version_keys(&dropped, 1100)
        .await
        .is_err());
    // Reopening the catalog must preserve the same refusal; it cannot rely on
    // an in-memory history of the previous configuration.
    let reopened = Catalog::new(store, CatalogOptions::default())?
        .open_namespace(namespace.config().clone())
        .await?;
    assert!(reopened.require_version_keys(&dropped, 1100).await.is_err());
    reopened.require_version_keys(&dropped, 10000).await?;
    assert!(next.verify(&token, &scope(), [7; 32], 10000).is_err());
    Ok(())
}

#[test]
fn version_key_deadline_and_payload_limits() -> Result<()> {
    let ring = keys(11, 2000, vec![])?;
    assert!(ring.issue(&version(), [7; 32], 1001).is_err());
    assert!(ring.issue(&version(), [7; 32], u64::MAX).is_err());
    let mut invalid = version();
    invalid.position.index = LogIndex::new(0);
    assert!(ring.issue(&invalid, [7; 32], 1000).is_err());
    let mut longest = version();
    longest.scope.cell = "x".repeat(1024);
    let token = ring.issue(&longest, [7; 32], 1000)?;
    assert_eq!(ring.verify(&token, &longest.scope, [7; 32], 1000)?, longest);
    longest.scope.cell.push('x');
    assert!(ring.issue(&longest, [7; 32], 1000).is_err());
    let old = VersionSigningKey::new([11; 32], 10000)?.verification_key();
    assert!(keys(11, 20000, vec![old]).is_err());
    assert!(VersionSigningKey::new([0; 32], 20000).is_err());
    Ok(())
}
