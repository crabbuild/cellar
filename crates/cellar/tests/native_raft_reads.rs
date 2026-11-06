// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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

use anyhow::{Context as _, Result};
use cellar::sdk::consensus::{verification::*, *};
use std::{sync::Arc, time::Duration};

type Query = dyn for<'a> Fn(&QueryContext<'a>, &[u8]) -> Result<Vec<u8>> + Send + Sync;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_readiness_reports_voters_learners_and_empty_image() -> Result<()> {
    readiness::voter_learner_and_empty().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_readiness_distinguishes_historical_and_recent_source() -> Result<()> {
    readiness::historical_without_recent_source().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_readiness_survives_protocol_only_leadership_transfer() -> Result<()> {
    readiness::after_protocol_only_leadership_transfer().await
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

fn scope() -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:one".into(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    }
}
