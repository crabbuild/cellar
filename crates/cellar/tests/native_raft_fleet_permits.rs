// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact shared-group membership permits through rooted managed movement APIs.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, *};
use managed_fixture::{
    archives, inspect_current, movement, request, start_node, ArchiveSetup, FirstGroupSetup,
    ManagedCluster,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[path = "support/raft_catalog_fault.rs"]
pub mod catalog_fault;

#[path = "support/raft_storage_fault.rs"]
pub mod storage_fault;

#[path = "support/cell_group_managed_fixture.rs"]
#[allow(dead_code, unused_imports)]
mod managed_fixture;

#[path = "support/raft_fleet_permits_shared.rs"]
mod fixture;
use fixture::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_delayed_after_cancel() -> Result<()> {
    qualify(false, async |f| {
        let reserved = f.reserve(210).await?;
        let fenced = f
            .group(f.primary)?
            .fence_fleet_permit(reserved.permit.clone(), TIMEOUT)
            .await?;
        let guard = status(&fenced)?;
        ensure!(guard.consumed_sequence == 1 && guard.phase == Some(FleetPermitPhase::Fenced));
        drop(fenced);
        ensure!(f.strong(f.primary).await? == 5);
        f.settle(&reserved, 230, FleetPermitPhase::Fenced).await?;
        // Settlement keeps the historical permit. Even a freshly read
        // authorization cannot restart the quorum-fenced sequence.
        let reconciled = f.catalog_permit().await?;
        ensure!(f
            .group(f.primary)?
            .begin_managed_repair(
                reserved.group.clone(),
                reconciled,
                reserved.plan.clone(),
                TIMEOUT,
            )
            .await
            .is_err());
        ensure!(f.begin(f.primary, &reserved).await.is_err());
        let fenced = f
            .group(f.primary)?
            .fence_fleet_permit(reserved.permit.clone(), TIMEOUT)
            .await?;
        ensure!(status(&fenced)? == guard && fenced.membership().high_water() == 3);
        ensure!(fenced.membership().repair_status()?.is_none());
        drop(fenced);

        let next = f.reserve(212).await?;
        ensure!(next.expected.sequence() == 2);
        verify_phase(
            &f.begin(f.primary, &next).await?,
            &next,
            FleetPermitPhase::Seeding,
        )?;
        ensure!(f
            .group(f.primary)?
            .fence_fleet_permit(reserved.permit, TIMEOUT)
            .await
            .is_err());
        f.advance(&next, RepairStep::AbortLearner, FleetPermitPhase::Aborted)
            .await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_repair_step_follows_new_quorum_leader() -> Result<()> {
    qualify(false, async |f| {
        let reserved = f.reserve(210).await?;
        let source = f.primary;
        let began = f.begin(source, &reserved).await?;
        verify_phase(&began, &reserved, FleetPermitPhase::Seeding)?;
        let term = began.status().term;
        drop(began);
        f.stop(source).await?;
        let (leader, successor_term) = f.leader().await?;
        ensure!(leader != source && successor_term > term);
        f.advance(
            &reserved,
            RepairStep::AbortLearner,
            FleetPermitPhase::Aborted,
        )
        .await?;
        ensure!(f.strong(leader).await? == 5);
        f.ensure_pending(&reserved).await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_crash_recovery() -> Result<()> {
    qualify(false, async |f| {
        let reserved = f.reserve(210).await?;
        let calls = f.counts();
        // The spare's original boot owns the reservation throughout this voter
        // restart. Changing it would require independent capacity settlement.
        f.restart_voters(40).await?;
        let selected = f.group(f.primary)?.quorum_membership(TIMEOUT).await?;
        ensure!(status(&selected)?.consumed_sequence == 0 && status(&selected)?.permit.is_none());
        drop(selected);
        let reopened_permit = f.catalog_permit().await?;

        let lost = tokio::spawn(f.group(f.primary)?.begin_managed_repair(
            reserved.group.clone(),
            reopened_permit,
            reserved.plan.clone(),
            TIMEOUT,
        ));
        let consumed = f
            .wait_phase(f.primary, &reserved, FleetPermitPhase::Seeding)
            .await?;
        // The independent quorum observation owns the evidence. Discard the
        // original reply even if its producer already finished.
        lost.abort();
        let _ = lost.await;
        f.restart_voters(50).await?;
        let restored = f
            .wait_phase(f.primary, &reserved, FleetPermitPhase::Seeding)
            .await?;
        ensure!(restored == consumed);
        let resumed = f.begin(f.primary, &reserved).await?;
        ensure!(status(&resumed)? == consumed);
        drop(resumed);
        ensure!(f.counts() == calls);

        let terminal = f
            .advance(
                &reserved,
                RepairStep::AbortLearner,
                FleetPermitPhase::Aborted,
            )
            .await?;
        ensure!(f.strong(f.primary).await? == 5);
        let refused = f
            .settle(&reserved, 230, FleetPermitPhase::Aborted)
            .await
            .expect_err("new voter boots released the old physical debit");
        ensure!(
            matches!(
                refused.downcast_ref::<FleetCatalogError>(),
                Some(FleetCatalogError::Unconfirmed)
            ),
            "retained boot settlement: {refused:#}"
        );
        f.ensure_pending(&reserved).await?;
        f.restart_voters(60).await?;
        let replay = f
            .advance(
                &reserved,
                RepairStep::AbortLearner,
                FleetPermitPhase::Aborted,
            )
            .await?;
        ensure!(replay == terminal);
        let delayed = f
            .group(f.primary)?
            .begin_managed_repair(
                reserved.group.clone(),
                reserved.permit.clone(),
                reserved.plan.clone(),
                TIMEOUT,
            )
            .await;
        ensure!(matches!(
            delayed,
            Err(CellGroupControlError::Refused(
                RuntimeError::StaleCompletion
            ))
        ));
        ensure!(f.counts() == calls);
        f.ensure_pending(&reserved).await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_joint_cancel() -> Result<()> {
    qualify(false, async |f| {
        let reserved = f.reserve(210).await?;
        verify_phase(
            &f.begin(f.primary, &reserved).await?,
            &reserved,
            FleetPermitPhase::Seeding,
        )?;
        f.activate(&reserved, f.primary, false).await?;
        let joint = f
            .advance(
                &reserved,
                RepairStep::PromoteAndRemove,
                FleetPermitPhase::Joint,
            )
            .await?;
        ensure!(
            f.wait_phase(SPARE, &reserved, FleetPermitPhase::Joint)
                .await?
                == joint
        );
        let primary = f.primary;
        let group = f.group(primary)?;
        ensure!(group
            .fence_fleet_permit(reserved.permit.clone(), TIMEOUT)
            .await
            .is_err());
        let revision = group.observe(TIMEOUT).await?.membership_revision;
        ensure!(group
            .advance_managed_repair(
                reserved.group.clone(),
                reserved.permit.clone(),
                RepairStep::AbortLearner,
                revision,
                TIMEOUT,
            )
            .await
            .is_err());

        // Keep a majority of the incoming voters while losing the old majority.
        for voter in 0..SPARE {
            if voter != primary {
                f.stop(voter).await?;
            }
        }
        ensure!(group
            .advance_managed_repair(
                reserved.group.clone(),
                reserved.permit.clone(),
                RepairStep::FinishJoint,
                revision,
                SHORT,
            )
            .await
            .is_err());
        ensure!(group.observe(TIMEOUT).await?.membership_revision == revision);
        f.ensure_pending(&reserved).await?;
        for index in [primary, SPARE] {
            ensure!(f
                .group(index)?
                .observe_fleet_membership(reserved.group.clone(), SHORT)
                .await
                .is_err());
        }

        f.stop(SPARE).await?;
        f.reopen(SPARE, 45).await?;
        let retained = f.group(SPARE)?;
        ensure!(retained.observe(TIMEOUT).await?.membership_revision == revision);
        let refused = f.read(SPARE, QueryConsistency::Linearizable, SHORT).await;
        let refused = refused.expect_err("retained joint read bypassed the missing old majority");
        ensure!(
            matches!(
                refused
                    .downcast_ref::<Arc<CellDispatchError>>()
                    .map(Arc::as_ref),
                Some(CellDispatchError::Runtime(
                    RuntimeError::DeadlineExceeded | RuntimeError::NotReady
                ))
            ),
            "retained joint read outcome: {refused:?}"
        );
        ensure!(retained
            .observe_fleet_membership(reserved.group.clone(), SHORT)
            .await
            .is_err());

        for voter in 0..SPARE {
            if voter != primary {
                f.reopen(voter, 50 + voter as u8).await?;
            }
        }
        let terminal = f
            .advance(
                &reserved,
                RepairStep::FinishJoint,
                FleetPermitPhase::Complete,
            )
            .await?;
        let fresh = f
            .group(f.primary)?
            .observe_fleet_membership(reserved.group.clone(), TIMEOUT)
            .await?;
        ensure!(status(&fresh)? == terminal);
        ensure!(fresh.configuration().voters_outgoing.is_empty());
        ensure!(fresh.status().applied.index >= fresh.barrier().index);
        ensure!(f.strong(f.primary).await? == 5);
        // The retained learner changed boot. Completion of consensus does
        // not release the original destination's physical capacity debit.
        f.ensure_pending(&reserved).await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_snapshot_roundtrip() -> Result<()> {
    for from_s3 in [false, true] {
        qualify(from_s3, async |f| {
            let reserved = f.reserve(210).await?;
            let began = f.begin(f.primary, &reserved).await?;
            let before = status(&began)?;
            drop(began);
            let receipt = f.write(2, 1, Durability::S3).await?;
            let target = f.activate(&reserved, f.primary, from_s3).await?;
            ensure!(target == before);
            let read = f
                .read(
                    SPARE,
                    QueryConsistency::AtLeast(receipt.version_token),
                    TIMEOUT,
                )
                .await?;
            ensure!(read.result == 6_i64.to_be_bytes() && read.served_by == MemberId::new(4)?);
            ensure!(f.counts()[SPARE] == 0, "seed reran application commands");
            f.advance(
                &reserved,
                RepairStep::PromoteAndRemove,
                FleetPermitPhase::Joint,
            )
            .await?;
            let complete = f
                .advance(
                    &reserved,
                    RepairStep::FinishJoint,
                    FleetPermitPhase::Complete,
                )
                .await?;
            ensure!(
                f.wait_phase(SPARE, &reserved, FleetPermitPhase::Complete)
                    .await?
                    == complete
            );
            ensure!(f.activate(&reserved, f.primary, from_s3).await? == complete);
            ensure!(f
                .group(f.primary)?
                .fence_fleet_permit(reserved.permit, TIMEOUT)
                .await
                .is_err());
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_quorumless_fence() -> Result<()> {
    qualify(false, async |f| {
        let reserved = f.reserve(210).await?;
        let group = f.group(f.primary)?;
        let before = group.observe(TIMEOUT).await?.membership_revision;
        for voter in 0..SPARE {
            if voter != f.primary {
                f.stop(voter).await?;
            }
        }
        ensure!(group
            .fence_fleet_permit(reserved.permit.clone(), SHORT)
            .await
            .is_err());
        ensure!(group
            .begin_managed_repair(
                reserved.group.clone(),
                reserved.permit.clone(),
                reserved.plan.clone(),
                SHORT,
            )
            .await
            .is_err());
        ensure!(group.observe(TIMEOUT).await?.membership_revision == before);
        f.ensure_pending(&reserved).await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_no_manual_bypass() -> Result<()> {
    qualify(false, async |f| {
        let reserved = f.reserve(210).await?;
        let source = f.group(f.primary)?;
        let mut changed = reserved.plan.clone();
        changed.policy_revision += 1;
        ensure!(matches!(
            source
                .begin_managed_repair(
                    reserved.group.clone(),
                    reserved.permit.clone(),
                    changed,
                    TIMEOUT,
                )
                .await,
            Err(CellGroupControlError::Refused(_))
        ));
        ensure!(source
            .advance_managed_repair(
                reserved.group.clone(),
                reserved.permit.clone(),
                RepairStep::AbortLearner,
                reserved.plan.expected_revision,
                TIMEOUT,
            )
            .await
            .is_err());
        ensure!(
            f.activate(&reserved, f.primary, false).await.is_err(),
            "unconsumed permit seeded a learner"
        );
        ensure!(f.group(SPARE).is_err());
        verify_phase(
            &f.begin(f.primary, &reserved).await?,
            &reserved,
            FleetPermitPhase::Seeding,
        )?;
        let mut wrong = f.activation(f.primary, false)?;
        wrong.member = MemberId::new(5)?;
        ensure!(f.cluster.hosts[SPARE]
            .activate_managed_learner_group(reserved.group.clone(), reserved.permit.clone(), wrong)
            .await
            .is_err());
        ensure!(f.group(SPARE).is_err());
        let seeded = f.activate(&reserved, f.primary, false).await?;
        ensure!(seeded.phase == Some(FleetPermitPhase::Seeding));
        f.advance(
            &reserved,
            RepairStep::AbortLearner,
            FleetPermitPhase::Aborted,
        )
        .await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_legacy_startup_cannot_seed_managed_history() -> Result<()> {
    for from_s3 in [false, true] {
        qualify(from_s3, async |f| {
            let reserved = f.reserve(210).await?;
            verify_phase(
                &f.begin(f.primary, &reserved).await?,
                &reserved,
                FleetPermitPhase::Seeding,
            )?;
            f.write(2, 1, Durability::S3).await?;
            f.stop(SPARE).await?;
            let config = &f.cluster.fixtures[SPARE];
            let node = start_node(
                f.cluster.roots[SPARE].path(),
                config.application.clone(),
                &config.spec.label,
            )
            .await?;
            let (mut options, services) = config.config(&node, LogOpenMode::Resume, 45).await?;
            options.host.managed = None;
            options.groups.push(RaftGroupAssignment {
                history: f.cluster.history,
                placement: RaftGroupPlacement::Replica,
            });
            archives::configure(&mut options);
            let before = f.catalog().read(TIMEOUT).await?.revision();
            let refused = node.start_raft_cells(options, services).await;
            node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(
                refused.is_err(),
                "unmanaged startup attached a managed replica"
            );
            ensure!(f.catalog().read(TIMEOUT).await?.revision() == before);
            f.ensure_spare_unclaimed().await?;
            ensure!(f.counts()[SPARE] == 0);
            f.ensure_pending(&reserved).await?;
            f.advance(
                &reserved,
                RepairStep::AbortLearner,
                FleetPermitPhase::Aborted,
            )
            .await?;
            Ok(())
        })
        .await?;
    }
    Ok(())
}
