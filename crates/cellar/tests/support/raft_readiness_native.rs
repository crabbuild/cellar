//! Actual voter/learner observations and disconnected query policies.
use super::{counter, get, leader, scope, Fixture};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::consensus::*;
use std::{sync::Arc, time::Duration};

async fn observe(
    handle: &RaftCellHandle,
    ready: impl Fn(&CellStatus) -> bool,
) -> Result<CellStatus> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match handle.status().await {
                Ok(status) if ready(&status) => return Ok(status),
                Ok(_) | Err(CommandFailure::Overloaded) => (),
                Err(error) => return Err(error.into()),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

pub async fn voter_learner_and_empty() -> Result<()> {
    let definitions = (0..4)
        .map(|_| counter(Arc::new(get)))
        .collect::<Result<Vec<_>>>()?;
    let scope = scope();
    let namespace = NamespaceConfig::new(
        scope.cluster,
        scope.namespace,
        NamespaceMode::Raft,
        definitions[0].schema,
    );
    let cluster = super::cluster::Cluster::start_with_initial_learner(
        &definitions,
        vec![RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace,
            class: "Counter".into(),
            birth: BirthProposal {
                scope: scope.clone(),
                voters: (1..=3)
                    .map(|id| {
                        Ok(InitialMember {
                            member: MemberId::new(id)?,
                            node: NodeId::new([id as u8; 16])?,
                        })
                    })
                    .collect::<Result<_>>()?,
                learners: vec![InitialMember {
                    member: MemberId::new(4)?,
                    node: NodeId::new([4; 16])?,
                }],
            },
        }],
        20,
        RaftHostOptions {
            stale_source_timeout_ms: 2000,
            ..Default::default()
        },
    )
    .await?;
    let result = async {
        let handles = cluster
            .services
            .iter()
            .map(|service| service.cell(scope.namespace, &scope.cell))
            .collect::<Result<Vec<_>, _>>()?;
        let primary = leader(&handles, None).await?;
        for (index, handle) in handles.iter().enumerate() {
            let expected = if index == 3 {
                RaftReplicaRole::Learner
            } else if index == primary {
                RaftReplicaRole::Leader
            } else {
                RaftReplicaRole::Voter
            };
            let status = observe(handle, |s| {
                s.role == expected && s.command_ready == (index == primary)
            })
            .await?;
            ensure!(
                status.version.is_none()
                    && !status.queries.coherent_image
                    && !status.queries.historical
                    && !status.queries.recent_source
            );
            ensure!(status.queries.registered_queries);
            ensure!(
                status.command_ready == (index == primary),
                "empty replica {index}, initial leader {primary}: {status:?}"
            );
        }
        let written = handles[primary]
            .command(
                definitions[0].increment.clone(),
                7i64.to_be_bytes().to_vec(),
                super::public_node::options(1)?,
            )
            .await?;
        for (index, handle) in handles.iter().enumerate() {
            let value = handle
                .query(
                    definitions[index].get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(written.version_token.clone()),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(value.result == 7i64.to_be_bytes());
            let expected = if index == 3 {
                RaftReplicaRole::Learner
            } else if index == primary {
                RaftReplicaRole::Leader
            } else {
                RaftReplicaRole::Voter
            };
            let status = observe(handle, |s| {
                // Even an applied leader briefly closes command readiness
                // while a protocol Ready turn (such as a heartbeat) is owned.
                s.role == expected
                    && s.command_ready == (index == primary)
                    && s.queries.historical
                    && s.queries.recent_source
            })
            .await?;
            ensure!(
                status.queries.coherent_image
                    && status
                        .queries
                        .source_age_ms
                        .is_some_and(|age| age < status.queries.source_timeout_ms)
            );
            ensure!(status
                .version
                .as_ref()
                .context("published readiness version")?
                .covers(&written.version)?);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(sample) = cluster.services[index].resource_usage().groups {
                        let role_matches = match expected {
                            RaftReplicaRole::Leader => sample.leaders == 1,
                            RaftReplicaRole::Voter => sample.voters == 1,
                            RaftReplicaRole::Learner => sample.learners == 1,
                            _ => false,
                        };
                        if role_matches
                            && sample.coherent_images == 1
                            && sample.historical_queries == 1
                        {
                            ensure!(sample.attached_groups == 1 && sample.parked_groups == 0);
                            ensure!(
                                sample.leaders
                                    + sample.voters
                                    + sample.learners
                                    + sample.removed
                                    + sample.unknown
                                    == 1
                            );
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            ensure!(
                status.command_ready == (index == primary),
                "published replica {index}, initial leader {primary}: {status:?}"
            );
        }
        // A learner still obtains the same fresh quorum barrier as any other
        // linearizable query; its local image/readiness is not that proof.
        let read = handles[3]
            .query(
                definitions[3].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(read.result == 7i64.to_be_bytes() && read.version.covers(&written.version)?);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined = cluster.close().await;
    result?;
    joined
}

pub async fn historical_without_recent_source() -> Result<()> {
    let fixture = Fixture::start(Arc::new(get)).await?;
    let result = async {
        let primary = leader(&fixture.handles, None).await?;
        let follower = (primary + 1) % 3;
        fixture.cluster.isolate_consensus(follower).await?;
        let status = observe(&fixture.handles[follower], |s| {
            s.queries.historical && !s.queries.recent_source
        })
        .await?;
        ensure!(
            status.role == RaftReplicaRole::Voter
                && !status.command_ready
                && status.queries.coherent_image
        );
        ensure!(status
            .queries
            .source_age_ms
            .is_none_or(|age| age >= status.queries.source_timeout_ms));
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
        ensure!(historical.result == 1i64.to_be_bytes());
        for consistency in [QueryConsistency::Stale, QueryConsistency::Linearizable] {
            let refused = fixture.handles[follower]
                .query(
                    fixture.definition.get.clone(),
                    vec![],
                    QueryOptions {
                        consistency,
                        timeout: Duration::from_millis(150),
                        ..Default::default()
                    },
                )
                .await;
            ensure!(
                matches!(refused, Err(CommandFailure::VersionUnavailable)),
                "isolated readiness bypassed read policy: {refused:?}"
            );
        }
        fixture.cluster.heal();
        observe(&fixture.handles[follower], |s| s.queries.recent_source).await?;
        let strong = fixture.handles[follower]
            .query(
                fixture.definition.get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(strong.result == 1i64.to_be_bytes());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined = fixture.close().await;
    result?;
    joined
}

pub(super) async fn without_query_code(handle: &RaftCellHandle) -> Result<()> {
    let status = observe(handle, |s| s.queries.coherent_image).await?;
    ensure!(
        !status.queries.registered_queries
            && !status.queries.historical
            && !status.queries.recent_source
    );
    Ok(())
}
