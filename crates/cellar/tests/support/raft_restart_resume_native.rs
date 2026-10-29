// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual stop/return/resume, with capacity held until a fresh terminal proof.
#[path = "raft_restart_stop_refresh_native.rs"]
mod stop_refresh;
use super::*;
use cellar::application::consensus::verification::{
    FleetRestartResumeAdmission, FleetRestartResumeCompleteRequest, FleetRestartResumePhase,
    FleetRestartResumeProgress, FleetRestartResumeRequest, FleetRestartResumeVerification,
};
use cellar::application::consensus::InspectionError;

async fn setup(primary_target: bool) -> Result<(Setup, usize, FleetRestartResumeRequest)> {
    let mut s = Box::pin(Setup::new()).await?;
    let leader = primary(&s.f, &s.cells).await?;
    let p = if primary_target {
        leader
    } else {
        (leader + 1) % 3
    };
    let (mut target, returning) = offline(&mut s, p).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .admit_restart_return(&root, &returning, TIMEOUT)
        .await?;
    let node = public_node::node_named(
        target.root.path(),
        s.f.definition.application.clone(),
        &target.spec.label,
    )
    .await?;
    let (config, services) = configuration(&s, &target, &node, &returning).await?;
    let host = node.start_raft_cells(config, services).await?;
    s.cells[p] = host.cell(s.proposal.scope.namespace, &s.proposal.scope.cell)?;
    target.node = node;
    target.host = host;
    s.nodes.insert(p, target);
    trust(&s).await?;
    // This is a planned open profile in a trusted fixture attestation. The
    // actual physical observation remains paused and is checked independently.
    let mut attestation =
        crate::hosts::capacity_tests::attestation(&s.f, &s.spare, &s.nodes[p].host).await?;
    let mut profile = s.nodes[p].host.observe_node(TIMEOUT).await?.node.profile;
    ensure!(profile.draining);
    profile.draining = false;
    attestation.profile_digest = fleet_capacity_profile_digest(&profile)?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let request = FleetRestartResumeRequest {
        identity: request_identity(190, root.revision(), returning.identity.operation),
        returning: returning.identity,
        attestation,
        resume: ProposalId::new([192; 16])?,
    };
    Ok((s, p, request))
}

async fn prepare_resume(
    s: &Setup,
    request: &FleetRestartResumeRequest,
) -> Result<FleetRestartResumeVerification> {
    trust(s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    Ok(s.f
        .catalog
        .prepare_restart_resume(
            &root,
            request,
            std::slice::from_ref(&s.namespace),
            &s.spare,
            TIMEOUT,
        )
        .await?)
}

async fn consume_resume(s: &Setup, walk: &mut FleetRestartResumeVerification) -> Result<usize> {
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            let mut handoffs = 0;
            loop {
                match walk.reconcile_next(TIMEOUT).await {
                    Ok(Some(FleetRestartResumeProgress::HandoffRequested { group, from, to })) => {
                        ensure!(group == s.proposal.scope && from != to);
                        ensure!(walk.next_history(TIMEOUT).await?.is_some());
                        handoffs += 1;
                    }
                    Ok(Some(FleetRestartResumeProgress::Verified {
                        group,
                        current_member,
                        retained_member,
                        replaced,
                    })) => {
                        ensure!(
                            group == s.proposal.scope
                                && current_member.is_some()
                                && retained_member.is_none()
                                && !replaced
                        );
                    }
                    Ok(None) => return Ok::<_, anyhow::Error>(handoffs),
                    Err(FleetCatalogError::Unconfirmed | FleetCatalogError::Overloaded)
                    | Err(FleetCatalogError::Observation(
                        InspectionError::Unavailable | InspectionError::Overloaded,
                    )) => {
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                    other => anyhow::bail!("resume verification: {other:?}"),
                }
            }
        }))
        .await??
}

async fn consent_resume(
    s: &Setup,
    request: &FleetRestartResumeRequest,
) -> Result<FleetRestartResumeAdmission> {
    let mut walk = prepare_resume(s, request).await?;
    ensure!(walk.phase() == FleetRestartResumePhase::Paused);
    consume_resume(s, &mut walk).await?;
    ensure!(walk.verified_histories() == 1);
    Ok(walk.admit_resume(request, TIMEOUT).await?)
}

async fn pending(s: &Setup, p: usize, paused: bool) -> Result<()> {
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .authorize_host(&root, s.nodes[p].spec.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::RestartPending
    );
    ensure!(s
        .f
        .catalog
        .authorize_capacity(&root, s.nodes[p].spec.node, TIMEOUT)
        .await
        .is_err());
    ensure!(
        s.nodes[p]
            .host
            .observe_node(TIMEOUT)
            .await?
            .node
            .profile
            .draining
            == paused
    );
    let attempt = request_identity(198, root.revision(), [199; 16]);
    ensure!(matches!(
        s.f.catalog
            .begin_evacuation(
                &root,
                &attempt,
                NodeId::new([7; 16])?,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::DisruptionInProgress { .. })
    ));
    Ok(())
}

async fn resumed(primary_target: bool, lost: bool) -> Result<()> {
    let (s, p, request) = Box::pin(setup(primary_target)).await?;
    pending(&s, p, true).await?;
    let mut walk = prepare_resume(&s, &request).await?;
    consume_resume(&s, &mut walk).await?;
    if lost {
        s.f.store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
    }
    let admitted = walk.admit_resume(&request, TIMEOUT).await;
    if lost {
        ensure!(admitted.is_err());
        ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    } else {
        admitted?;
    }
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = reopened.read(TIMEOUT).await?;
    let admission = reopened
        .restart_resume_admission(&root, &request, TIMEOUT)
        .await?;
    ensure!(admission.applied_revision() == request.identity.expected_revision + 1);
    pending(&s, p, true).await?;
    trust(&s).await?;
    // Advance real SQL after consent; the receiving owner must renew proof.
    let survivors = s
        .cells
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != p)
        .map(|(_, c)| c.clone())
        .collect::<Vec<_>>();
    write_read(&s.f, &survivors, 2, 5, 12).await?;
    reopened
        .resume_restart_maintenance(&admission, &s.spare, TIMEOUT)
        .await?;
    reopened
        .resume_restart_maintenance(&admission, &s.spare, TIMEOUT)
        .await?;
    pending(&s, p, false).await?;
    let mut walk = prepare_resume(&s, &request).await?;
    ensure!(walk.phase() == FleetRestartResumePhase::Resumed);
    consume_resume(&s, &mut walk).await?;
    let root = reopened.read(TIMEOUT).await?;
    let complete = FleetRestartResumeCompleteRequest {
        identity: request_identity(194, root.revision(), request.identity.operation),
        resume: request.identity.clone(),
    };
    if lost {
        s.f.store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
    }
    let result = walk.complete_resume(&complete, TIMEOUT).await;
    if lost {
        ensure!(result.is_err());
        ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    } else {
        result?;
    }
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = reopened.read(TIMEOUT).await?;
    let done = reopened
        .restart_resume_completion(&root, &complete, TIMEOUT)
        .await?;
    ensure!(
        done.intact()
            && done.retained_copies() == 0
            && done.replaced_histories() == 0
            && done.applied_revision() == complete.identity.expected_revision + 1
    );
    ensure!(
        reopened
            .restart_resumed(&root, ProposalId::new(request.identity.operation)?, TIMEOUT)
            .await?
            .context("terminal operation")?
            .identity()
            == done.identity()
    );
    ensure!(
        reopened
            .authorize_host(&root, s.nodes[p].spec.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Active
    );
    let capacity = reopened
        .authorize_capacity(&root, s.nodes[p].spec.node, TIMEOUT)
        .await?;
    ensure!(capacity.receive_replicas() && capacity.attestation() == &request.attestation);
    trust(&s).await?;
    write_read(&s.f, &s.cells, 3, 3, 15).await?;
    ensure!(!s.nodes[p].node.is_closed());
    // Only the terminal cut released the slot: another real operation can claim it.
    let boot = s.nodes[p]
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("resumed boot")?;
    let next = FleetRestartRequest {
        identity: request_identity(196, root.revision(), [197; 16]),
        node: s.nodes[p].spec.node,
        expected_process: ProcessGeneration::new(boot.process)?,
        grace_seconds: 120,
    };
    reopened
        .begin_restart_maintenance(&root, &next, std::slice::from_ref(&s.namespace), TIMEOUT)
        .await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_resume_primary_restores_capacity_only_after_fresh_completion() -> Result<()>
{
    Box::pin(resumed(true, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_resume_follower_recovers_lost_consent_and_completion() -> Result<()> {
    Box::pin(resumed(false, true)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_resume_missing_voter_refuses_opening_and_retains_debt() -> Result<()> {
    let (mut s, p, request) = Box::pin(setup(false)).await?;
    let admission = consent_resume(&s, &request).await?;
    trust(&s).await?;
    let victim = (p + 1) % 3;
    stopped(s.nodes.remove(victim).node).await?;
    ensure!(s
        .f
        .catalog
        .resume_restart_maintenance(&admission, &s.spare, TIMEOUT)
        .await
        .is_err());
    let p = if victim < p { p - 1 } else { p };
    pending(&s, p, true).await?;
    ensure!(!s.nodes[p].node.is_closed());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_resume_expired_live_walk_cannot_publish_consent() -> Result<()> {
    let (s, p, request) = Box::pin(setup(false)).await?;
    let mut walk = prepare_resume(&s, &request).await?;
    consume_resume(&s, &mut walk).await?;
    s.f.domain
        .run(async { cellar::asyncrt::sleep(Duration::from_millis(5050)).await })
        .await?;
    ensure!(matches!(
        walk.admit_resume(&request, TIMEOUT).await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == request.identity.expected_revision);
    pending(&s, p, true).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_resume_root_drift_requires_fresh_same_scope_consent() -> Result<()> {
    let (s, p, mut request) = Box::pin(setup(false)).await?;
    let admission = consent_resume(&s, &request).await?;
    let extra = s.declare_extra(&s.f.catalog).await?;
    trust(&s).await?;
    ensure!(s
        .f
        .catalog
        .resume_restart_maintenance(&admission, &s.spare, TIMEOUT)
        .await
        .is_err());
    pending(&s, p, true).await?;
    // Complete the new namespace inventory, then renew with a new human identity.
    adopt_new_empty(&s, &extra)
        .await
        .context("import new namespace after resume consent")?;
    trust(&s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    request.identity = request_identity(191, root.revision(), request.identity.operation);
    let mut walk =
        s.f.catalog
            .prepare_restart_resume(
                &root,
                &request,
                &[s.namespace.clone(), extra.clone()],
                &s.spare,
                TIMEOUT,
            )
            .await?;
    consume_resume(&s, &mut walk).await?;
    let admission = walk
        .admit_resume(&request, TIMEOUT)
        .await
        .context("publish refreshed resume consent")?;
    trust(&s).await?;
    s.f.catalog
        .resume_restart_maintenance(&admission, &s.spare, TIMEOUT)
        .await?;
    pending(&s, p, false).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut walk =
        s.f.catalog
            .prepare_restart_resume(
                &root,
                &request,
                &[s.namespace.clone(), extra],
                &s.spare,
                TIMEOUT,
            )
            .await?;
    consume_resume(&s, &mut walk).await?;
    let completion = FleetRestartResumeCompleteRequest {
        identity: request_identity(194, root.revision(), request.identity.operation),
        resume: request.identity.clone(),
    };
    walk.complete_resume(&completion, TIMEOUT)
        .await
        .context("publish current resume completion")?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_resume_rechecks_and_hands_off_regained_leadership() -> Result<()> {
    let (s, p, request) = Box::pin(setup(false)).await?;
    let mut warm = prepare_resume(&s, &request).await?;
    consume_resume(&s, &mut warm).await?;
    drop(warm);
    let source = primary(&s.f, &s.cells).await?;
    ensure!(source != p);
    let state = s.cells[source]
        .inspect()
        .await?
        .member
        .context("surviving leader")?;
    s.cells[source]
        .transfer_leadership(
            LeadershipTransfer {
                target: MemberId::new(p as u64 + 1)?,
                expected_term: state.term,
                expected_revision: state.membership_revision,
            },
            TIMEOUT,
        )
        .await?;
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while !s.cells[p]
                .inspect()
                .await?
                .member
                .is_some_and(|m| m.is_leader)
            {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        }))
        .await???;
    pending(&s, p, true).await?;
    let mut walk = prepare_resume(&s, &request).await?;
    ensure!(consume_resume(&s, &mut walk).await? > 0);
    let admission = walk
        .admit_resume(&request, TIMEOUT)
        .await
        .context("publish refreshed resume consent")?;
    trust(&s).await?;
    s.f.catalog
        .resume_restart_maintenance(&admission, &s.spare, TIMEOUT)
        .await?;
    pending(&s, p, false).await?;
    ensure!(
        !s.cells[p]
            .inspect()
            .await?
            .member
            .context("returned member")?
            .is_leader
    );
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_resume_forged_projection_wrong_child_and_scope_refuse() -> Result<()> {
    use sha2::{Digest, Sha256};
    let (s, p, request) = Box::pin(setup(false)).await?;
    let admission = consent_resume(&s, &request).await?;
    trust(&s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let key = format!("restart-resume/{}", hex::encode(request.identity.operation));
    let bytes =
        s.f.catalog
            .get(&root, FleetTable::Operations, &key, TIMEOUT)
            .await?
            .context("resume intent")?;
    let mut forged = FleetMutation {
        id: [193; 16],
        operation: request.identity.operation,
        principal: request.identity.principal.clone(),
        expected_revision: root.revision(),
        changes: vec![FleetChange {
            table: FleetTable::Operations,
            key,
            value: Some(bytes),
        }],
    };
    ensure!(matches!(
        s.f.catalog.commit(&root, &forged, TIMEOUT).await,
        Err(FleetCatalogError::Invalid("restart resume authority"))
    ));
    let node_key = hex::encode(request.attestation.boot.node);
    let bytes =
        s.f.catalog
            .get(&root, FleetTable::Nodes, &node_key, TIMEOUT)
            .await?
            .context("returned node")?;
    let epoch = s.nodes[p]
        .host
        .observe_node(TIMEOUT)
        .await?
        .current_enrollment
        .context("returned enrollment")?
        .epoch;
    let payload = std::str::from_utf8(&bytes[13..bytes.len() - 32])?;
    let changed = payload
        .replacen("\"state\":\"RestartPending\"", "\"state\":\"Active\"", 1)
        .replacen(
            &format!(
                "\"operation\":{}",
                serde_json::to_string(&request.identity.operation)?
            ),
            "\"operation\":null",
            1,
        )
        .replacen(
            &format!("\"epoch\":{epoch}"),
            &format!("\"epoch\":{}", epoch + 2),
            1,
        );
    ensure!(changed != payload);
    let mut encoded = bytes[..9].to_vec();
    encoded.extend_from_slice(&(changed.len() as u32).to_be_bytes());
    encoded.extend_from_slice(changed.as_bytes());
    encoded.extend_from_slice(&Sha256::digest(&encoded));
    forged.changes = vec![FleetChange {
        table: FleetTable::Nodes,
        key: node_key,
        value: Some(encoded),
    }];
    ensure!(matches!(
        s.f.catalog.commit(&root, &forged, TIMEOUT).await,
        Err(FleetCatalogError::Invalid("restart resume authority"))
    ));
    for field in ["request", "operation", "finalization"] {
        let mut wrong = serde_json::to_value(admission.resume_request()?)?;
        wrong["selector"][field] = serde_json::json!(vec![193; 16]);
        ensure!(s
            .spare
            .request_peer_fleet_stop(serde_json::from_value(wrong)?, root.revision(), TIMEOUT)
            .await
            .is_err());
    }
    let mut wrong_scope = request.clone();
    wrong_scope.resume = ProposalId::new([193; 16])?;
    ensure!(matches!(
        s.f.catalog
            .restart_resume_admission(&root, &wrong_scope, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    pending(&s, p, true).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_resume_expired_consent_renews_actual_proof_before_opening() -> Result<()> {
    let (s, p, request) = Box::pin(setup(false)).await?;
    let admission = consent_resume(&s, &request).await?;
    s.f.domain
        .run(async { cellar::asyncrt::sleep(Duration::from_millis(5050)).await })
        .await?;
    trust(&s).await?;
    let survivors = s
        .cells
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != p)
        .map(|(_, c)| c.clone())
        .collect::<Vec<_>>();
    write_read(&s.f, &survivors, 2, 5, 12).await?;
    s.f.catalog
        .resume_restart_maintenance(&admission, &s.spare, TIMEOUT)
        .await?;
    pending(&s, p, false).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_resume_drift_after_opening_holds_slot_until_renewed_completion() -> Result<()>
{
    let (s, p, mut request) = Box::pin(setup(false)).await?;
    let admission = consent_resume(&s, &request).await?;
    trust(&s).await?;
    s.f.catalog
        .resume_restart_maintenance(&admission, &s.spare, TIMEOUT)
        .await?;
    let mut stale = prepare_resume(&s, &request).await?;
    consume_resume(&s, &mut stale).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let complete = FleetRestartResumeCompleteRequest {
        identity: request_identity(194, root.revision(), request.identity.operation),
        resume: request.identity.clone(),
    };
    // Change a rooted policy input after live verification, without forging any
    // physical or quorum evidence and without touching the held disruption slot.
    s.f.catalog
        .commit(
            &root,
            &FleetMutation {
                id: [195; 16],
                operation: [196; 16],
                principal: "policy-test".into(),
                expected_revision: root.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Policy,
                    key: "resume-input-probe".into(),
                    value: Some(vec![1]),
                }],
            },
            TIMEOUT,
        )
        .await?;
    ensure!(matches!(
        stale.complete_resume(&complete, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    pending(&s, p, false).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    request.identity = request_identity(191, root.revision(), request.identity.operation);
    let mut fresh = prepare_resume(&s, &request).await?;
    ensure!(fresh.phase() == FleetRestartResumePhase::Resumed);
    consume_resume(&s, &mut fresh).await?;
    let refreshed = fresh.admit_resume(&request, TIMEOUT).await?;
    // Repeated physical delivery finishes only its already-synced exact effect.
    trust(&s).await?;
    s.f.catalog
        .resume_restart_maintenance(&refreshed, &s.spare, TIMEOUT)
        .await?;
    let mut terminal = prepare_resume(&s, &request).await?;
    consume_resume(&s, &mut terminal).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let complete = FleetRestartResumeCompleteRequest {
        identity: request_identity(194, root.revision(), request.identity.operation),
        resume: request.identity.clone(),
    };
    terminal.complete_resume(&complete, TIMEOUT).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(s
        .f
        .catalog
        .authorize_capacity(&root, s.nodes[p].spec.node, TIMEOUT)
        .await?
        .receive_replicas());
    s.close().await
}

async fn adopt_new_empty(s: &Setup, namespace: &NamespaceCatalog) -> Result<()> {
    let operation = ProposalId::new([202; 16])?;
    let cut =
        s.f.domain
            .run(namespace.freeze_births(operation, TIMEOUT))
            .await??;
    ensure!(cut.is_empty());
    let root = s.f.catalog.read(TIMEOUT).await?;
    let begin = request_identity(200, root.revision(), operation.bytes());
    s.f.catalog
        .begin_namespace_import(&root, &begin, namespace, &cut, TIMEOUT)
        .await?;
    for id in 201..=204 {
        let root = s.f.catalog.read(TIMEOUT).await?;
        let advance = request_identity(id, root.revision(), operation.bytes());
        s.f.catalog
            .advance_namespace_import(&root, &advance, namespace, None, TIMEOUT)
            .await?;
    }
    Ok(())
}
