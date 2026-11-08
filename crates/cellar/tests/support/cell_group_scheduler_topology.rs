// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real shared histories through the rooted topology owner and movement worker.
use super::*;
use cellar::application::consensus::verification::{
    cell_groups::RangeDirectory, FleetChange, FleetMutation, FleetTable, RuntimeError,
};
use cellar::sdk::{NodeOutcome, ShutdownMode};
use std::{future::Future, pin::Pin, sync::Arc};

struct Evidence {
    hosts: Vec<RaftCells>,
    directory: RangeDirectory,
    history: GroupHistory,
    pause_joint: std::sync::atomic::AtomicBool,
    joint_ready: tokio::sync::Notify,
}

impl Evidence {
    async fn committed_joint(
        &self,
        timeout: Duration,
    ) -> std::result::Result<bool, FleetCatalogError> {
        let proof = self.hosts[2]
            .group(self.history)
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .quorum_membership(timeout)
            .await
            .map_err(|_| FleetCatalogError::Unconfirmed)?;
        Ok(proof
            .membership()
            .fleet_status()
            .is_some_and(|status| status.phase == Some(FleetPermitPhase::Joint)))
    }

    async fn attestation(
        &self,
        node: NodeId,
        timeout: Duration,
    ) -> std::result::Result<FleetResourceAttestation, FleetCatalogError> {
        let host = self
            .hosts
            .iter()
            .find(|h| h.identity().node == node)
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        let view = host
            .observe_node(timeout)
            .await
            .map_err(|_| FleetCatalogError::Unconfirmed)?;
        Ok(FleetResourceAttestation {
            boot: view.fleet_boot.ok_or(FleetCatalogError::Unconfirmed)?,
            profile_revision: 1,
            used: FleetResourceBounds {
                engine_slots: view.node.groups,
                durable_bytes: view.node.reserved_bytes.max(1024 * 1024),
                memory_bytes: 64 * 1024 * 1024,
            },
            evidence_digest: [71; 32],
            evidence_reference: "fixture-only/topology-attestation".into(),
        })
    }
}

impl FleetTopologyEvidence for Evidence {
    fn refresh<'a>(
        &'a self,
        operation: &'a FleetTopologyOperation,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<Output = std::result::Result<FleetTopologyServiceInputs, FleetCatalogError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut nodes = Vec::new();
            for host in &self.hosts {
                nodes.push(FleetTopologyNodeResources {
                    attestation: self.attestation(host.identity().node, timeout).await?,
                    reserve: FleetResourceBounds {
                        engine_slots: 1,
                        durable_bytes: 32 * 1024 * 1024,
                        memory_bytes: 8 * 1024 * 1024,
                    },
                });
            }
            let view = self.hosts[0]
                .observe_node(timeout)
                .await
                .map_err(|_| FleetCatalogError::Unconfirmed)?;
            let resident = FleetResourceBounds {
                engine_slots: 1,
                durable_bytes: view.node.profile.bytes_per_group,
                memory_bytes: 8 * 1024 * 1024,
            };
            Ok(FleetTopologyServiceInputs {
                request: FleetTopologyRequest {
                    keep: operation.keep.clone(),
                    policy: operation.policy.clone(),
                    nodes,
                    histories: vec![FleetTopologyHistoryResources {
                        group: self.history,
                        resident,
                        peak: resident,
                    }],
                },
                namespaces: vec![self.directory.clone()],
            })
        })
    }
}

impl FleetMovementEvidence for Evidence {
    fn refresh<'a>(
        &'a self,
        movement: &'a FleetPendingMovement,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = std::result::Result<FleetMovementExecutionRequest, FleetCatalogError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            if self.pause_joint.load(Ordering::Acquire) && self.committed_joint(timeout).await? {
                self.joint_ready.notify_one();
                // The owned worker must cancel and join this read-only callback
                // on stop. No configuration action has started in this turn.
                std::future::pending::<()>().await;
            }
            Ok(FleetMovementExecutionRequest {
                reservation: movement.reservation,
                attestation: self
                    .attestation(movement.plan.replacement.node, timeout)
                    .await?,
                from_s3: true,
            })
        })
    }
}

async fn start(cluster: &ManagedCluster, evidence: Arc<Evidence>) -> Result<FleetMovementService> {
    Ok(cluster.fixtures[0]
        .catalog
        .start_topology_service(
            cluster.hosts[2].clone(),
            evidence.clone(),
            evidence,
            FleetMovementServiceOptions {
                interval: Duration::from_millis(50),
                retry_max_interval: Duration::from_millis(200),
                turn_timeout: TIMEOUT,
            },
        )
        .await?)
}

pub async fn exercise(cluster: &mut ManagedCluster) -> Result<()> {
    exercise_inner(cluster, false).await
}

pub async fn exercise_member_return(cluster: &mut ManagedCluster) -> Result<()> {
    exercise_inner(cluster, true).await
}

async fn exercise_inner(cluster: &mut ManagedCluster, collect: bool) -> Result<()> {
    let f = &cluster.fixtures[0];
    let evidence = Arc::new(Evidence {
        hosts: cluster.hosts.clone(),
        directory: cluster.directory.clone(),
        history: cluster.history,
        pause_joint: std::sync::atomic::AtomicBool::new(true),
        joint_ready: tokio::sync::Notify::new(),
    });
    let control = RaftFleetControl::new(
        &f.domain,
        f.catalog.clone(),
        vec![cluster.directory.clone()],
    )?
    .with_node_observer(cluster.hosts[2].clone());
    // Group membership does not grant application ingress. Authorize the
    // stable origin's exact verified process on every possible destination.
    let cut = f.catalog.read(TIMEOUT).await?;
    let origin = cluster.peers[2].clone();
    for (index, host) in cluster.hosts.iter().enumerate() {
        if index == 2 {
            continue;
        }
        managed_fixture::inspect_current(host, origin.node, cut.revision()).await?;
        host.grant_command_origin(NamespaceCommandOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: cut.revision(),
            peer: origin.clone(),
        })?;
        host.grant_query_origin(NamespaceQueryOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: cut.revision(),
            peer: origin.clone(),
        })?;
    }
    let mut service = Some(start(cluster, evidence.clone()).await?);
    let result = run(cluster, &control, &evidence, &mut service, collect).await;
    if let Some(service) = service.take() {
        service.shutdown().await;
    }
    let seed = result?;
    if collect {
        return movement::verify_cells(&cluster.hosts[2], &seed, 5).await;
    }
    // Placement completion is not a deletion certificate. The obsolete copies
    // stay on disk, while closing both former voters must leave a serving RF3.
    for index in [1, 0] {
        let node = cluster.nodes.remove(index);
        let host = cluster.hosts.remove(index);
        ensure!(matches!(
            node.shutdown(ShutdownMode::Preserve).await?,
            NodeOutcome::Stopped(_)
        ));
        let closure = host.close().await?;
        ensure!(
            closure.groups_total == 1
                && closure.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
        );
    }
    movement::verify_cells(&cluster.hosts[0], &seed, 4).await
}

async fn run(
    cluster: &ManagedCluster,
    control: &RaftFleetControl,
    evidence: &Arc<Evidence>,
    service: &mut Option<FleetMovementService>,
    collect: bool,
) -> Result<SeededCells> {
    // Admit the larger fleet before changing the keep set. A single RF3 group
    // needs no balancing move merely because three empty nodes are available.
    let mut seed = None;
    let mut outgoing = None;
    let keeps: Vec<Vec<usize>> = if collect {
        vec![(0..6).collect(), (1..6).collect(), (0..3).collect(), vec![]]
    } else {
        vec![(0..6).collect(), (1..6).collect(), (2..6).collect()]
    };
    for (leg, keep) in keeps.into_iter().enumerate() {
        let keep = if collect && leg == 3 {
            let target = outgoing.as_ref().context("original joined target")?;
            let index = cluster
                .fixtures
                .iter()
                .position(|f| hex::encode(f.spec.node.bytes()) == *target)
                .context("original target index")?;
            vec![1, 2, index]
        } else {
            keep
        };
        let status = control.status(TIMEOUT).await?;
        let request = RaftFleetBegin {
            catalog_id: status.catalog_id,
            operation_id: hex::encode([220 + leg as u8; 16]),
            expected_revision: status.observed_revision,
            keep: keep
                .iter()
                .map(|index| hex::encode(cluster.fixtures[*index].spec.node.bytes()))
                .collect(),
            policy: FleetTopologyPolicy {
                revision: leg as u64 + 1,
                distinct_zones: false,
                distinct_regions: false,
                max_work: 1_000_000,
            },
        };
        let begun = control.begin("fixture", request.clone(), TIMEOUT).await?;
        ensure!(!begun.terminal && begun.phase == FleetTopologyPhase::Running);
        // A new public control value must recover the original rooted request,
        // including its original revision, rather than inventing a new intent.
        let replacement = RaftFleetControl::new(
            &cluster.fixtures[0].domain,
            cluster.fixtures[0].catalog.clone(),
            vec![cluster.directory.clone()],
        )?;
        let replay = replacement
            .begin("fixture", request.clone(), TIMEOUT)
            .await?;
        ensure!(
            replay.operation_id == begun.operation_id
                && replay.request_id == begun.request_id
                && replay.expected_revision == request.expected_revision
                && replay.policy == request.policy
                && replay.keep == begun.keep
                && replay.applied_revision >= begun.applied_revision
        );
        let mut restarted = false;
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let current = cellar::asyncrt::select_biased! {
                    "observe committed Joint before polling topology completion";
                    _ = evidence.joint_ready.notified(), if leg == 1 && !restarted => {
                        ensure!(evidence.committed_joint(TIMEOUT).await?, "restart must occur in committed Joint");
                        if collect {
                            let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
                            let pending = cluster.fixtures[0].catalog.pending_movements(&cut, TIMEOUT).await?;
                            ensure!(pending.len() == 1);
                            ensure!(cluster.hosts[0].reclaim_removed_member(pending[0].reservation, TIMEOUT).await.is_err(), "Joint consensus cannot authorize old-member GC");
                            ensure!(cluster.hosts[0].observe_node(TIMEOUT).await?.node.groups == 1);
                        }

                        let refused = start(cluster, evidence.clone()).await;
                        ensure!(matches!(refused, Err(ref error) if matches!(error.downcast_ref::<FleetCatalogError>(), Some(FleetCatalogError::Overloaded))), "second worker must not bypass node ownership");
                        evidence.pause_joint.store(false, Ordering::Release);
                        service.take().context("old topology worker")?.shutdown().await;
                        *service = Some(start(cluster, evidence.clone()).await?);
                        restarted = true;
                        continue;
                    }
                    current = replacement.operation(&request.operation_id, TIMEOUT) => current,
                };
                let current = match current {
                    Ok(current) => current,
                    Err(error) if error.code == RaftFleetFailureCode::Busy => {
                        // Only the read-only status sample is retried. The
                        // enclosing operation deadline continues to run.
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    Err(error) => return Err(anyhow::Error::new(error)
                        .context(format!("topology leg {leg} status"))),
                };
                if current.terminal {
                    ensure!(current.converged && current.phase == FleetTopologyPhase::Complete);
                    ensure!(current.admitted_movements == u64::from(leg != 0));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        }).await.with_context(|| format!("topology leg {leg}, status={:?}", service.as_ref().map(FleetMovementService::status)))??;
        if leg == 1 {
            ensure!(restarted, "movement worker was not replaced during Joint");
        }
        let page = control
            .movements(&request.operation_id, None, 128, TIMEOUT)
            .await?;
        ensure!(page.entries.len() == usize::from(leg != 0));
        for entry in &page.entries {
            ensure!(
                entry.group == hex::encode(cluster.history.group.bytes())
                    && entry.incarnation == hex::encode(cluster.history.incarnation.bytes())
                    && entry.source
                        == if collect && leg == 2 {
                            outgoing.clone().context("return source")?
                        } else if collect && leg == 3 {
                            hex::encode(cluster.fixtures[0].spec.node.bytes())
                        } else {
                            hex::encode(cluster.fixtures[leg - 1].spec.node.bytes())
                        }
                    && request.keep.contains(&entry.target)
            );
        }
        let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
        ensure!(cluster.fixtures[0]
            .catalog
            .pending_reservations(&cut, TIMEOUT)
            .await?
            .is_empty());
        if leg == 0 {
            seed = Some(seed_cells(&cluster.hosts, cluster.history).await?);
        } else {
            movement::verify_cells(
                &cluster.hosts[2],
                seed.as_ref().context("seed")?,
                leg as u64 + 1,
            )
            .await
            .with_context(|| format!("topology leg {leg} cells"))?;
        }
        let membership = cluster.hosts[2]
            .group(cluster.history)?
            .quorum_membership(TIMEOUT)
            .await?;
        ensure!(membership.root().route().history == cluster.history);
        if collect && leg != 0 {
            let entry = page.entries.first().context("completed movement")?;
            let reservation: [u8; 16] = hex::decode(&entry.reservation)?
                .try_into()
                .map_err(|_| anyhow::anyhow!("reservation length"))?;
            let source = cluster
                .fixtures
                .iter()
                .position(|f| hex::encode(f.spec.node.bytes()) == entry.source)
                .context("obsolete source")?;
            ensure!(
                cluster.hosts[2]
                    .reclaim_removed_member(reservation, TIMEOUT)
                    .await
                    .is_err(),
                "surviving voter cannot collect itself from another member's completion"
            );
            let before = cluster.hosts[source].observe_node(TIMEOUT).await?;
            ensure!(before.node.groups == 1);
            let before_root = cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision();
            if leg == 1 {
                cluster.fixtures[0]
                    .store
                    .fail_fleet_confirmation
                    .store(true, Ordering::Release);
            }
            let mut lost_settlement = false;
            let deadline = tokio::time::Instant::now() + TIMEOUT;
            tokio::time::timeout_at(deadline, async {
                loop {
                    match cluster.hosts[source]
                        .reclaim_removed_member(
                            reservation,
                            deadline.saturating_duration_since(tokio::time::Instant::now()),
                        )
                        .await
                    {
                        Ok(true) => return Ok::<_, anyhow::Error>(()),
                        Ok(false)
                        | Err(CellGroupControllerError::Runtime(
                            RuntimeError::NotReady | RuntimeError::Overloaded,
                        )) => tokio::time::sleep(Duration::from_millis(10)).await,
                        Err(CellGroupControllerError::Catalog(error)) if leg == 1 && !cluster.fixtures[0].store.fail_fleet_confirmation.load(Ordering::Acquire)
                            && matches!(error.as_ref(), cellar::application::consensus::verification::CatalogError::FleetAuthority(error) if matches!(error.as_ref(), FleetCatalogError::Unconfirmed | FleetCatalogError::Storage(_))) => {
                            ensure!(!lost_settlement, "only the injected settlement confirmation may be lost");
                            ensure!(cluster.hosts[source].observe_node(TIMEOUT).await?.node.groups == 0);
                            ensure!(cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision() == before_root + 1, "fault must follow the successful slot-settlement CAS");
                            lost_settlement = true;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            })
            .await
            .context("joined member reclamation budget")??;
            ensure!(
                leg != 1 || lost_settlement,
                "actual root CAS confirmation loss was not observed"
            );
            let settled = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
            let forged = FleetMutation {
                id: [245 + leg as u8; 16],
                operation: [245 + leg as u8; 16],
                principal: "fixture".into(),
                expected_revision: settled.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Operations,
                    key: format!("member-cleanup/{}", entry.reservation),
                    value: None,
                }],
            };
            ensure!(
                matches!(
                    cluster.fixtures[0]
                        .catalog
                        .commit(&settled, &forged, TIMEOUT)
                        .await,
                    Err(FleetCatalogError::Invalid(_))
                ),
                "generic mutation changed cleanup authority"
            );
            ensure!(
                cluster.hosts[source]
                    .reclaim_removed_member(reservation, TIMEOUT)
                    .await?,
                "exact physical reclamation replay"
            );
            ensure!(
                cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision() == settled.revision(),
                "settlement replay refunded twice"
            );
            let after = cluster.hosts[source].observe_node(TIMEOUT).await?;
            ensure!(
                after.node.groups == 0,
                "obsolete engine mapping survived GC"
            );
            ensure!(
                cluster.hosts[source]
                    .start_managed_first_group(cluster.history, TIMEOUT)
                    .await
                    .is_err(),
                "member fence allowed old genesis resurrection"
            );
            ensure!(
                cluster.hosts[source]
                    .observe_node(TIMEOUT)
                    .await?
                    .node
                    .groups
                    == 0
            );
            if leg == 1 {
                outgoing = Some(entry.target.clone());
            }
            if leg == 2 || leg == 3 {
                let returned_index = if leg == 2 {
                    0
                } else {
                    cluster
                        .fixtures
                        .iter()
                        .position(|f| hex::encode(f.spec.node.bytes()) == entry.target)
                        .context("returned joined target")?
                };
                let physical = cluster.hosts[2]
                    .inspect_peer_physical_inventory(
                        cluster.fixtures[returned_index].spec.node,
                        cut.revision(),
                        TIMEOUT,
                    )
                    .await?;
                let inventory = physical.inventory.context("returned member inventory")?;
                let returned = inventory
                    .group_history(&cluster.history)
                    .context("returned shared history")?;
                ensure!(
                    returned.in_configuration() && returned.member().get() > leg as u64 + 2,
                    "return reused an obsolete member ID"
                );
            }
        }

        ensure!(
            membership.configuration().voters.len() == 3
                && membership.configuration().voters_outgoing.is_empty()
        );
    }
    if collect {
        return seed.context("seeded logical cells");
    }
    let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    for index in [0, 1] {
        let physical = cluster.hosts[2]
            .inspect_peer_physical_inventory(
                cluster.fixtures[index].spec.node,
                cut.revision(),
                TIMEOUT,
            )
            .await?;
        let inventory = physical.inventory.context("obsolete member inventory")?;
        let retained = inventory
            .group_history(&cluster.history)
            .context("retained obsolete copy")?;
        ensure!(
            physical.view.node.groups == 1
                && !retained.in_configuration()
                && retained.member().get() == index as u64 + 1
        );
    }
    seed.context("seeded logical cells")
}
