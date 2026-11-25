// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Queued runtime metadata retains a known source after controller detachment.
use super::*;
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

fn history(id: u8) -> GroupHistory {
    GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([id; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    }
}

fn queued_metadata_after_detachment(removed: bool) {
    let group = history(3);
    let foreign = history(5);
    let roster = GroupRoster::new([group].into_iter(), 2);
    assert!(roster.contains(&group).unwrap());
    assert!(!roster.detached(&group));
    assert!(!roster.detached(&foreign));

    // The worker still owns the applier while physical producers join. Its
    // event guard must not infer completed detachment from this intermediate
    // state, or from a history that this controller never registered.
    assert!(!roster.begin_retirement(group).unwrap());
    assert!(!roster.detached(&group));
    assert!(matches!(
        roster.finish_reclamation(&group),
        Err(RuntimeError::NotReady)
    ));

    roster.finish_retirement(group, removed).unwrap();
    assert!(!roster.contains(&group).unwrap());
    assert!(
        roster.detached(&group),
        "detached metadata must remain attributable: removed={removed}"
    );
    assert!(!roster.detached(&foreign));
    assert!(matches!(
        roster.reserve(group),
        Err(RuntimeError::AlreadyAttached)
    ));

    // Releasing the volatile roster does not authorize any future event.
    // The controller then needs its separate durable engine-fence lookup.
    roster.finish_reclamation(&group).unwrap();
    assert!(!roster.detached(&group));
    assert!(!roster.detached(&foreign));
}

#[test]
fn retired_source_metadata_remains_known_until_reclamation() {
    queued_metadata_after_detachment(false);
}

#[test]
fn removed_member_metadata_remains_known_until_reclamation() {
    queued_metadata_after_detachment(true);
}

#[test]
fn shutdown_eligibility_does_not_release_retirement_debt() {
    let group = history(3);
    let pending = history(5);
    let failed = history(6);
    let roster = GroupRoster::new([group].into_iter(), 3);
    assert!(!roster.begin_retirement(group).unwrap());
    assert!(roster.shutdown_states_valid());
    assert!(!roster.detached(&group));
    assert!(!roster.contains(&group).unwrap());
    assert_eq!(roster.len(), 1);
    assert!(matches!(
        roster.reserve(group),
        Err(RuntimeError::AlreadyAttached)
    ));
    assert!(matches!(
        roster.require_reclaimable(&group, false),
        Err(RuntimeError::NotReady)
    ));
    assert!(matches!(
        roster.finish_reclamation(&group),
        Err(RuntimeError::NotReady)
    ));

    let reservation = roster.reserve(pending).unwrap();
    assert!(!roster.shutdown_states_valid());
    drop(reservation);
    assert!(roster.shutdown_states_valid());
    let mut registration = roster.reserve(failed).unwrap();
    registration.activate().unwrap();
    roster
        .quarantine(failed, CellGroupControllerError::Invalid("failed owner"))
        .unwrap();
    assert!(!roster.shutdown_states_valid());
}
