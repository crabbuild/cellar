// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;
use crate::application::consensus::{CellId, ClusterId, IncarnationId, NamespaceId};

// Framework schema creation belongs to the current private image transaction.
fn database() -> Result<Connection> {
    let db = Connection::open_in_memory()?;
    db.execute_batch("BEGIN IMMEDIATE")?;
    Ok(db)
}

fn history(incarnation: u8) -> Result<CellHistory> {
    Ok(CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("Parent:shared")?,
        incarnation: IncarnationId::new([incarnation; 16])?,
    })
}

fn reserve_at(db: &Connection, h: &CellHistory) -> Result<SandboxReservation> {
    Ok(super::super::sandbox_catalog::reserve_at(
        db,
        h.cell.as_str(),
        &"a".repeat(32),
        "job",
        "SANDBOX",
        &"b".repeat(64),
        &|name| format!("Sandbox:{name}"),
    )?
    .1)
}

#[test]
fn shared_sandbox_cannot_replace_a_retained_history_or_parent_pin() -> Result<()> {
    let db = database()?;
    let h = history(3)?;
    owner(&db, &h, None, true)?;
    assert!(owner(&db, &history(4)?, None, true).is_err());
    assert!(owner(&db, &h, Some([5; 32]), true).is_err());
    owner(&db, &h, None, false)?;
    let child = database()?;
    owner(&child, &h, Some([5; 32]), true)?;
    assert!(owner(&child, &h, Some([6; 32]), true).is_err());
    owner(&child, &h, Some([5; 32]), false)?;
    // A lost authority record cannot be replaced from otherwise valid metadata.
    let retained = database()?;
    reserve_at(&retained, &h)?;
    assert!(owner(&retained, &h, None, false).is_err());
    assert!(owner(&retained, &h, None, true).is_err());
    Ok(())
}

#[tokio::test]
async fn terminal_sandbox_keeps_confirmation_work_and_rolls_back_stale_completion() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let db = database()?;
            let h = history(3)?;
            owner(&db, &h, None, true)?;
            db.execute_batch(super::super::work::SCHEMA)?;
            let allocation = reserve_at(&db, &h)?.allocation;
            super::super::sandbox_work::register_in(
                &db,
                h.cell.as_str(),
                &allocation,
                &"c".repeat(64),
            )?;
            let (_, deleting) = super::super::sandbox_catalog::transition_at(
                &db,
                h.cell.as_str(),
                &allocation,
                SandboxAllocationState::Deleting,
            )?;
            super::super::sandbox_catalog::transition_at(
                &db,
                h.cell.as_str(),
                &deleting,
                SandboxAllocationState::Deleted,
            )?;
            super::super::sandbox_catalog::retire_at(&db, h.cell.as_str(), &"a".repeat(32), false)?;
            let terminal = super::super::sandbox_catalog::retire_at(
                &db,
                h.cell.as_str(),
                &"a".repeat(32),
                true,
            )?;
            assert!(confirmation_pending(&db, &terminal)?);
            assert_eq!(
                crate::application::consensus::native_work::get(&db, "sandbox-cleanup")?,
                Some(0)
            );
            let mut wrong = terminal.clone();
            wrong.revision += 1;
            assert!(
                super::super::sandbox_catalog::transaction(&db, |db| confirm_in(
                    db,
                    &h,
                    &wrong,
                    Durability::Quorum
                ))
                .is_err()
            );
            assert!(confirmation_pending(&db, &terminal)?);
            let aborted: Result<()> = super::super::sandbox_catalog::transaction(&db, |db| {
                confirm_in(db, &h, &terminal, Durability::Quorum)?;
                anyhow::bail!("caller transaction rolls back")
            });
            assert!(aborted.is_err());
            assert!(confirmation_pending(&db, &terminal)?);
            assert_eq!(
                crate::application::consensus::native_work::get(&db, "sandbox-cleanup")?,
                Some(0)
            );
            assert!(
                super::super::sandbox_catalog::transaction(&db, |db| confirm_in(
                    db,
                    &history(4)?,
                    &terminal,
                    Durability::Quorum
                ))
                .is_err()
            );
            super::super::sandbox_catalog::transaction(&db, |db| {
                confirm_in(db, &h, &terminal, Durability::Quorum)
            })?;
            assert!(!confirmation_pending(&db, &terminal)?);
            assert!(!query_cleanup_pending(&db, &h)?);
            assert_eq!(
                crate::application::consensus::native_work::get(&db, "sandbox-cleanup")?,
                None
            );
            // Strengthening an already proved Quorum tombstone creates new
            // S3 proof debt. Stale Quorum work cannot claim or clear that debt.
            super::super::sandbox_catalog::transaction(&db, |db| {
                assert!(strengthen_minimum(db, Durability::S3)?);
                super::super::sandbox_work::rearm(db, h.cell.as_str())
            })?;
            assert_eq!(next_minimum(&db)?, Durability::S3);
            assert!(query_cleanup_pending(&db, &h)?);
            let wake = crate::application::consensus::native_work::get(&db, "sandbox-cleanup")?;
            assert!(wake.is_some());
            assert!(check_work_minimum(&db, Durability::Quorum).is_err());
            assert!(
                super::super::sandbox_catalog::transaction(&db, |db| confirm_in(
                    db,
                    &h,
                    &terminal,
                    Durability::Quorum
                ))
                .is_err()
            );
            assert_eq!(
                crate::application::consensus::native_work::get(&db, "sandbox-cleanup")?,
                wake
            );
            assert!(query_cleanup_pending(&db, &h)?);
            let aborted: Result<()> = super::super::sandbox_catalog::transaction(&db, |db| {
                confirm_in(db, &h, &terminal, Durability::S3)?;
                anyhow::bail!("caller rolls back S3 confirmation")
            });
            assert!(aborted.is_err());
            assert_eq!(next_minimum(&db)?, Durability::S3);
            assert!(query_cleanup_pending(&db, &h)?);
            super::super::sandbox_catalog::transaction(&db, |db| {
                confirm_in(db, &h, &terminal, Durability::S3)
            })?;
            assert!(!query_cleanup_pending(&db, &h)?);
            assert!(!strengthen_minimum(&db, Durability::Quorum)?);
            assert_eq!(next_minimum(&db)?, Durability::S3);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn idle_sandbox_turn_preserves_its_successor_discovery() -> Result<()> {
    use crate::application::consensus::native_work;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let db = database()?;
            let h = history(3)?;
            owner(&db, &h, None, true)?;
            db.execute_batch(super::super::work::SCHEMA)?;
            let allocation = reserve_at(&db, &h)?.allocation;
            super::super::sandbox_work::register_in(
                &db,
                h.cell.as_str(),
                &allocation,
                &"c".repeat(64),
            )?;
            // An already due installation makes the real callback claim explicit,
            // without sleeping for the idle interval or changing domain time.
            super::super::work::set_in(
                &db,
                h.cell.as_str(),
                cellar_logic::work::Reason::SandboxCleanup,
                0,
            )?;
            native_work::arm_maintenance(&db, "sandbox-cleanup", 0)?;
            let old = native_work::due(&db, 0)?.context("idle Sandbox wake missing")?;
            assert!(native_work::claim(&db, &old, 0)?);
            assert!(super::super::sandbox_work::claim_at(&db, h.cell.as_str(), 0)?.is_none());
            native_work::complete(&db, &old, old.lease_ms)?;
            assert!(native_work::get(&db, "sandbox-cleanup")?.is_some());
            assert!(native_work::due(&db, crate::asyncrt::wall_ms().try_into()?)?.is_none());
            assert!(!native_work::claim(&db, &old, 0)?);
            assert_eq!(
                super::super::sandbox_catalog::read_parent(&db)?
                    .context("parent missing")?
                    .state,
                SandboxParentState::Active
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}
