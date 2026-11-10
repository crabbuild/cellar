// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Quarantined obsolete voters can be replaced without a live inventory probe.
//! The fixture joins the complete Node before attesting isolation; it does not
//! qualify a deployment's permanent process/launch fencing mechanism.
use super::*;
use cellar::application::consensus::verification::FleetTable;

fn intent(id: u8, revision: u64) -> FleetRequestIdentity {
    let mut identity = request(id, revision);
    identity.operation = [231; 16];
    identity
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_isolated_voter_s3_replacement_retains_disk_and_fence() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(isolated(rooted, Arc::new(fault::Store::default()))).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_isolated_voter_s3_replacement_retains_disk_and_fence() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-isolated-voter",
            rooted,
        )?));
        Box::pin(isolated(rooted, store)).await?;
    }
    Ok(())
}

#[path = "raft_retained_disk.rs"]
mod retained_disk;
pub(super) use retained_disk::disk_digest;

async fn isolated(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let mut stopped_index = None;
    let work = async {
        let seed = seed_cells(&cluster.hosts, cluster.history).await?;
        discover(&cluster).await?;
        let index = leader(&cluster.hosts[..3], cluster.history).await?;
        let obsolete = (index + 1) % 3;
        let failed = &cluster.fixtures[obsolete];
        let host = &cluster.hosts[obsolete];
        let boot = host
            .observe_node(TIMEOUT)
            .await?
            .fleet_boot
            .context("obsolete boot")?;
        ensure!(host.resource_usage().retained_manifest_cells == Some(2));
        // Retain an authorization and listeners minted before quarantine. The
        // startup path must recheck them, rather than treating them as a lease.
        let (mut stale, services) = failed
            .config(&cluster.nodes[obsolete], LogOpenMode::Resume, 90)
            .await?;
        archives::configure(&mut stale);
        stale.groups.push(RaftGroupAssignment {
            history: cluster.history,
            placement: RaftGroupPlacement::Replica,
        });
        let catalog = &cluster.fixtures[0].catalog;
        let cut = catalog.read(TIMEOUT).await?;
        let mut isolation = FleetNodeIsolationRequest {
            identity: intent(221, cut.revision()),
            attestation: FleetIsolationAttestation {
                boot: boot.clone(),
                evidence_digest: [92; 32],
                evidence_reference: "fixture/joined-node-and-restart-disabled".into(),
            },
        };
        ensure!(catalog
            .attest_node_isolation(&cut, &isolation, TIMEOUT)
            .await
            .is_err());
        ensure!(catalog.read(TIMEOUT).await?.revision() == cut.revision());
        let node = cluster.nodes.remove(obsolete);
        stopped_index = Some(obsolete);
        ensure!(matches!(
            node.shutdown(ShutdownMode::Preserve).await?,
            NodeOutcome::Stopped(_)
        ));
        let closure = host.close().await?;
        ensure!(
            host.is_closed()
                && closure.groups_total == 1
                && closure.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
        );
        let disk = cluster.roots[obsolete].path().join("raft");
        let retained = disk_digest(&disk)?;
        let cut = catalog.read(TIMEOUT).await?;
        catalog
            .begin_evacuation(
                &cut,
                &intent(220, cut.revision()),
                failed.spec.node,
                std::slice::from_ref(&cluster.directory),
                TIMEOUT,
            )
            .await?;
        let cut = catalog.read(TIMEOUT).await?;
        isolation.identity.expected_revision = cut.revision();
        let mut wrong = isolation.clone();
        wrong.attestation.boot.process = [93; 16];
        ensure!(catalog
            .attest_node_isolation(&cut, &wrong, TIMEOUT)
            .await
            .is_err());
        ensure!(catalog.read(TIMEOUT).await?.revision() == cut.revision());
        let accepted = catalog
            .attest_node_isolation(&cut, &isolation, TIMEOUT)
            .await?;
        ensure!(accepted.boot() == &boot);
        let cut = catalog.read(TIMEOUT).await?;
        ensure!(catalog
            .authorize_host(&cut, failed.spec.node, TIMEOUT)
            .await
            .is_err());
        let retry = start_node(
            cluster.roots[obsolete].path(),
            failed.application.clone(),
            &failed.spec.label,
        )
        .await?;
        ensure!(
            disk_digest(&disk)? == retained,
            "quarantine changed a joined engine before startup"
        );
        let attempt = retry.start_raft_cells(stale, services).await;
        retry.shutdown(ShutdownMode::Preserve).await?;
        let refusal = match attempt {
            Ok(host) => {
                host.close().await?;
                anyhow::bail!("saved pre-isolation startup authority reopened the voter");
            }
            Err(error) => error,
        };
        ensure!(
            refusal.chain().any(|error| matches!(
                error.downcast_ref::<FleetCatalogError>(),
                Some(FleetCatalogError::Conflict)
            )),
            "unexpected startup refusal: {refusal:#}"
        );
        ensure!(
            catalog.read(TIMEOUT).await?.revision() == cut.revision(),
            "refused startup mutated the fleet root"
        );
        let after = disk_digest(&disk)?;
        let changed = retained
            .keys()
            .chain(after.keys())
            .filter(|path| retained.get(*path) != after.get(*path))
            .collect::<std::collections::BTreeSet<_>>();
        ensure!(
            after == retained,
            "refused startup mutated the quarantined disk: {changed:?}"
        );
        discover_survivors(&cluster, Some(obsolete)).await?;
        let observer = &cluster.hosts[index];
        let membership = observer
            .group(cluster.history)?
            .quorum_membership(TIMEOUT)
            .await?;
        let target = cluster.hosts[3].observe_node(TIMEOUT).await?;
        let cut = catalog.read(TIMEOUT).await?;
        let movement = FleetMovementRequest {
            resources: FleetResourceRequest {
                identity: intent(210, cut.revision()),
                group: cluster.history,
                attestation: FleetResourceAttestation {
                    boot: target.fleet_boot.context("spare boot")?,
                    profile_revision: 1,
                    used: FleetResourceBounds {
                        engine_slots: 0,
                        durable_bytes: 1024 * 1024,
                        memory_bytes: 64 * 1024 * 1024,
                    },
                    evidence_digest: [71; 32],
                    evidence_reference: "fixture-only/occupancy-attestation".into(),
                },
                peak: FleetResourceBounds {
                    engine_slots: 1,
                    durable_bytes: target.node.profile.bytes_per_group,
                    memory_bytes: 8 * 1024 * 1024,
                },
            },
            source: cluster.fixtures[index].spec.node,
            plan: RepairPlan {
                id: ProposalId::new([211; 16])?,
                policy_revision: 1,
                expected_revision: membership.membership().revision(),
                obsolete: MemberId::new(obsolete as u64 + 1)?,
                replacement: JoiningMember {
                    member: MemberId::new(4)?,
                    node: cluster.fixtures[3].spec.node,
                    disk: cluster.fixtures[3].spec.disk,
                },
            },
        };
        drop(membership);
        catalog
            .reserve_movement(
                &cut,
                &movement,
                observer,
                std::slice::from_ref(&cluster.directory),
                TIMEOUT,
            )
            .await
            .context("reserve isolated shared-group replacement")?;
        let execution = FleetMovementExecutionRequest {
            reservation: movement.resources.identity.id,
            attestation: movement.resources.attestation.clone(),
            from_s3: true,
        };
        let expected = (1..=4)
            .filter(|id| *id != obsolete as u64 + 1)
            .collect::<Vec<_>>();
        let mut completion = None;
        let mut receipt = None;
        let mut lost_reply = false;
        for turn in 0..8 {
            discover_survivors(&cluster, Some(obsolete)).await?;
            let cut = catalog.read(TIMEOUT).await?;
            let membership = observer
                .group(cluster.history)?
                .quorum_membership(TIMEOUT)
                .await?;
            let configuration = membership.configuration();
            let mut voters = configuration.voters.clone();
            voters.sort_unstable();
            let complete = voters == expected
                && configuration.voters_outgoing.is_empty()
                && configuration.learners.is_empty();
            drop(membership);
            if complete {
                if completion.is_none() {
                    completion = Some(FleetMovementCompletionRequest {
                        identity: intent(212, cut.revision()),
                        reservation: execution.reservation,
                        attestation: execution.attestation.clone(),
                    });
                    cluster.fixtures[0]
                        .store
                        .fail_fleet_confirmation
                        .store(true, Ordering::Release);
                }
                match catalog
                    .complete_movement(&cut, completion.as_ref().unwrap(), observer, TIMEOUT)
                    .await
                {
                    Ok(done) => {
                        receipt = Some(done);
                        break;
                    }
                    Err(error) => {
                        let root = catalog.read(TIMEOUT).await?;
                        if root.revision() == cut.revision() + 1
                            && catalog
                                .pending_reservations(&root, TIMEOUT)
                                .await?
                                .is_empty()
                        {
                            lost_reply = true;
                        } else if matches!(error, FleetCatalogError::Unconfirmed)
                            && root.revision() == cut.revision()
                            && catalog.pending_reservations(&root, TIMEOUT).await?.len() == 1
                        {
                            eprintln!(
                                "isolated settlement turn {turn}: Unconfirmed; debit retained"
                            );
                        } else {
                            return Err(error)
                                .with_context(|| format!("isolated settlement turn {turn}"));
                        }
                    }
                }
            } else {
                match catalog
                    .reconcile_movement(&cut, &execution, observer, TIMEOUT)
                    .await
                {
                    Ok(progress) => ensure!(!progress.settled),
                    Err(FleetCatalogError::Unconfirmed) => {
                        eprintln!("isolated replacement turn {turn}: Unconfirmed; debit retained");
                    }
                    Err(error) => {
                        return Err(error)
                            .with_context(|| format!("isolated replacement turn {turn}"))
                    }
                }
                ensure!(
                    catalog
                        .pending_reservations(&catalog.read(TIMEOUT).await?, TIMEOUT)
                        .await?
                        .len()
                        == 1
                );
            }
        }
        let receipt = receipt.context("isolated replacement did not settle within eight turns")?;
        ensure!(lost_reply && receipt.isolated_sources() == [failed.spec.node.bytes()]);
        let cut = catalog.read(TIMEOUT).await?;
        let replay = catalog
            .complete_movement(&cut, completion.as_ref().unwrap(), observer, TIMEOUT)
            .await?;
        ensure!(
            replay.applied_revision() == receipt.applied_revision()
                && replay.isolated_sources() == receipt.isolated_sources()
        );
        ensure!(catalog.pending_movements(&cut, TIMEOUT).await?.is_empty());
        ensure!(catalog
            .pending_reservations(&cut, TIMEOUT)
            .await?
            .is_empty());
        ensure!(catalog
            .authorize_host(&cut, failed.spec.node, TIMEOUT)
            .await
            .is_err());
        // Settling a move grants no old-volume cleanup or terminal node removal.
        ensure!(
            disk_digest(&disk)? == retained,
            "movement settlement reclaimed an isolated voter disk"
        );
        ensure!(
            catalog
                .get(
                    &cut,
                    FleetTable::Operations,
                    &hex::encode(isolation.identity.operation),
                    TIMEOUT
                )
                .await?
                .is_some(),
            "isolation settlement dropped the evacuation operation"
        );
        let floor = catalog
            .get(
                &cut,
                FleetTable::Operations,
                &format!(
                    "resource-floor/{}",
                    hex::encode(cluster.fixtures[3].spec.node.bytes())
                ),
                TIMEOUT,
            )
            .await?
            .context("replacement physical floor")?;
        let floor: serde_json::Value = serde_json::from_slice(&floor[13..floor.len() - 32])?;
        ensure!(floor["physical"]["engine_slots"] == 1);
        ensure!(cluster.hosts[3].resource_usage().retained_manifest_cells == Some(2));
        ensure!(
            cluster.fixtures[3]
                .store
                .group_payload_reads
                .load(Ordering::Acquire)
                > 0,
            "replacement did not independently fetch S3 payloads"
        );
        for (i, (cell, original)) in seed.cells.iter().zip(&seed.receipts).enumerate() {
            let handle = cluster.hosts[3].cell(cell.clone())?;
            let restored = handle
                .query(
                    ProposalId::new([222 + i as u8; 16])?,
                    &handle.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(original.version_token.clone()),
                        routing: QueryRouting::Local,
                        timeout: TIMEOUT,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(
                restored.result == [7i64, 11][i].to_be_bytes()
                    && restored.version == original.version
            );
            ensure!(
                restored.served_by == MemberId::new(4)?
                    && restored.served_by_node == cluster.hosts[3].identity()
            );
        }
        verify_cells(observer, &seed, 2).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Some(index) = stopped_index {
        cluster.hosts.remove(index);
    }
    let joined = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!(
                "retained failed managed isolation disk: {}",
                root.keep().display()
            );
        }
    }
    joined?;
    work
}
