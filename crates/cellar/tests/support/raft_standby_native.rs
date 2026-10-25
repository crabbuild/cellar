// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real already-running standby hosts; no additional node-wide runtime is
//! constructed by activation, and recovery does not invoke business callbacks.
use super::*;
use futures_util::TryStreamExt;

async fn write_with_admission_retry(
    cell: &RaftCellHandle,
    definition: &public_node::Counter,
    value: i64,
    options: CommandOptions,
) -> Result<CommandReceipt> {
    let deadline = tokio::time::Instant::now() + options.timeout;
    tokio::time::timeout_at(deadline, async {
        loop {
            let mut attempt = options.clone();
            attempt.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            match cell
                .command(
                    definition.increment.clone(),
                    value.to_be_bytes().to_vec(),
                    attempt,
                )
                .await
            {
                Err(CommandFailure::Overloaded) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                result => return result,
            }
        }
    })
    .await
    .context("standby command admission deadline elapsed")?
    .map_err(Into::into)
}

pub(in super::super) async fn registered_standby_refuses_quorumless_s3_activation() -> Result<()> {
    let mut f = Fixture::new(true).await?;
    let cell = registered(&mut f).await?;
    f.fail_obsolete().await?;
    f.begin().await?;
    for (index, service) in f.cluster.services[..3].iter().enumerate() {
        if index != f.primary {
            ensure!(service.close().await?.unfinished.is_empty());
        }
    }
    let mut options = activation(&f, true)?;
    options.timeout = Duration::from_millis(500);
    ensure!(cell.activate_learner(options).await.is_err());
    ensure!(!claimed(&f).await?);
    ensure!(matches!(
        cell.status().await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    assert_eq!(f.calls[3].load(Ordering::SeqCst), 0);
    f.cluster.close().await
}

async fn registered(f: &mut Fixture) -> Result<RaftCellHandle> {
    let service = f
        .cluster
        .join_standby(
            f.definitions[3].application.clone(),
            RaftCellPlacement::Standby,
        )
        .await?;
    let cell = service.cell(
        f.registration.namespace.namespace,
        &f.registration.birth.scope.cell,
    )?;
    f.handles.push(cell.clone());
    assert_eq!(f.cluster.services.len(), 4);
    ensure!(matches!(
        cell.status().await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    ensure!(matches!(
        cell.command(
            f.definitions[3].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?
        )
        .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    assert_eq!(f.calls[3].load(Ordering::SeqCst), 0);
    Ok(cell)
}

fn activation(f: &Fixture, from_s3: bool) -> Result<LearnerActivation> {
    Ok(LearnerActivation {
        member: MemberId::new(4)?,
        source: MemberId::new(f.primary as u64 + 1)?,
        source_node: NodeId::new([f.primary as u8 + 1; 16])?,
        from_s3,
        timeout: Duration::from_secs(10),
    })
}

async fn claimed(f: &Fixture) -> Result<bool> {
    let objects = f.cluster.store.list(None).try_collect::<Vec<_>>().await?;
    Ok(objects
        .iter()
        .any(|o| o.location.as_ref().ends_with("/claims/0000000000000004")))
}

async fn caught_up(
    f: &Fixture,
    cell: &RaftCellHandle,
    token: VersionToken,
    value: i64,
) -> Result<()> {
    let read = cell
        .query(
            f.definitions[3].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(token),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(read.served_by, MemberId::new(4)?);
    assert_eq!(read.result, value.to_be_bytes());
    assert_eq!(f.calls[3].load(Ordering::SeqCst), 0);
    Ok(())
}

pub(in super::super) async fn registered_standby_peer_activation_retains_handles() -> Result<()> {
    let mut f = Fixture::new(false).await?;
    let cell = registered(&mut f).await?;
    let retained = cell.clone();
    // A registered class/endpoint or catalog birth cannot authorize a member.
    ensure!(cell.activate_learner(activation(&f, false)?).await.is_err());
    ensure!(!claimed(&f).await?);
    ensure!(matches!(
        retained.status().await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    f.fail_obsolete().await?;
    f.begin().await?;
    let hold = f.cluster.hold_data_responses_after(3, f.primary, 4096)?;
    let lost = cell.activate_learner(activation(&f, false)?);
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.cluster.held_data_response_bytes(3, f.primary) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    ensure!(!claimed(&f).await?);
    ensure!(matches!(
        retained.status().await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    ensure!(matches!(
        retained.activate_learner(activation(&f, false)?).await,
        Err(CommandFailure::Overloaded)
    ));
    drop(lost);
    drop(hold);
    tokio::time::timeout(Duration::from_secs(10), async {
        while retained.status().await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let receipt = retained.activate_learner(activation(&f, false)?).await?;
    assert_eq!(receipt.member, f.plan.replacement);
    ensure!(claimed(&f).await?);
    caught_up(&f, &retained, f.retained.clone(), 5).await?;
    let mut rebound = activation(&f, false)?;
    rebound.member = MemberId::new(5)?;
    ensure!(matches!(
        retained.activate_learner(rebound).await,
        Err(CommandFailure::CommandConflict)
    ));
    f.step(RepairStep::PromoteAndRemove).await?;
    f.step(RepairStep::FinishJoint).await?;
    let write = write_with_admission_retry(
        &f.handles[f.primary],
        &f.definitions[f.primary],
        1,
        public_node::options(2)?,
    )
    .await?;
    assert_eq!(write.result, 6_i64.to_be_bytes());
    caught_up(&f, &retained, write.version_token, 6).await?;
    assert_eq!(f.cluster.services.len(), 4);
    f.cluster.close().await
}

pub(in super::super) async fn registered_standby_s3_activation_and_restart() -> Result<()> {
    let mut f = Fixture::new(true)
        .await
        .context("initialize archived repair fixture")?;
    let cell = registered(&mut f).await?;
    f.fail_obsolete().await?;
    f.begin().await?;
    let receipt = cell
        .activate_learner(activation(&f, true)?)
        .await
        .context("activate S3 learner")?;
    assert_eq!(receipt.member, f.plan.replacement);
    caught_up(&f, &cell, f.retained.clone(), 5).await?;
    f.step(RepairStep::PromoteAndRemove).await?;
    f.step(RepairStep::FinishJoint).await?;
    let mut options = public_node::options(2)?;
    options.durability = Durability::S3;
    let receipt =
        write_with_admission_retry(&f.handles[f.primary], &f.definitions[f.primary], 1, options)
            .await
            .context("write after joint repair")?;
    caught_up(&f, &cell, receipt.version_token.clone(), 6).await?;
    let namespace = f.registration.namespace.namespace;
    let name = f.registration.birth.scope.cell.clone();
    let get = f.definitions[3].get.clone();
    let token = receipt.version_token;
    let check = Box::new(
        move |service: RaftCells| -> futures_util::future::BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                let cell = service.cell(namespace, &name)?;
                let read = cell
                    .query(
                        get.clone(),
                        vec![],
                        QueryOptions {
                            consistency: QueryConsistency::AtLeast(token),
                            ..Default::default()
                        },
                    )
                    .await?;
                assert_eq!(read.result, 6_i64.to_be_bytes());
                ensure!(!cell.status().await?.command_ready);
                ensure!(matches!(
                    cell.query(
                        get,
                        vec![],
                        QueryOptions {
                            consistency: QueryConsistency::Linearizable,
                            timeout: Duration::from_millis(200),
                            ..Default::default()
                        }
                    )
                    .await,
                    Err(CommandFailure::VersionUnavailable)
                ));
                Ok(())
            })
        },
    );
    let cluster = f
        .cluster
        .restart(
            &f.definitions,
            vec![f.registration.clone()],
            vec![
                RaftCellPlacement::Replica,
                RaftCellPlacement::Replica,
                RaftCellPlacement::Replica,
                RaftCellPlacement::Standby,
            ],
            check,
        )
        .await?;
    assert_eq!(f.calls[3].load(Ordering::SeqCst), 0);
    let cells = cluster
        .services
        .iter()
        .map(|service| service.cell(namespace, &f.registration.birth.scope.cell))
        .collect::<Result<Vec<_>, _>>()?;
    let primary = leader(&cells).await?;
    let receipt = write_with_admission_retry(
        &cells[primary],
        &f.definitions[primary],
        2,
        public_node::options(3)?,
    )
    .await
    .context("write after standby restart")?;
    assert_eq!(receipt.result, 8_i64.to_be_bytes());
    let reader = if primary == 3 {
        (0..3).find(|i| *i != f.failed).unwrap()
    } else {
        3
    };
    let read = cells[reader]
        .query(
            f.definitions[reader].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(receipt.version_token),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(read.result, 8_i64.to_be_bytes());
    cluster.close().await
}

pub(in super::super) async fn registered_standby_activation_shutdown_joins() -> Result<()> {
    let mut f = Fixture::new(false).await?;
    let cell = registered(&mut f).await?;
    f.fail_obsolete().await?;
    f.begin().await?;
    let hold = f.cluster.hold_data_responses_after(3, f.primary, 4096)?;
    let pending = cell.activate_learner(activation(&f, false)?);
    tokio::time::timeout(Duration::from_secs(5), async {
        while f.cluster.held_data_response_bytes(3, f.primary) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let closed =
        tokio::time::timeout(Duration::from_secs(5), f.cluster.services[3].close()).await??;
    ensure!(closed.unfinished.is_empty());
    ensure!(matches!(pending.await, Err(CommandFailure::Unavailable)));
    ensure!(!claimed(&f).await?);
    assert_eq!(f.calls[3].load(Ordering::SeqCst), 0);
    drop(hold);
    f.cluster.close().await
}
