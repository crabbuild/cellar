// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;
use serde_json::{json, Value};

fn pending() -> Value {
    json!({
        "catalog_id":hex::encode([1;32]), "operation_id":hex::encode([2;16]),
        "request_id":hex::encode([3;16]), "node_id":hex::encode([4;16]),
        "expected_revision":1, "observed_revision":1, "phase":"verifying_before_stop",
        "checked_histories":0, "total_histories":1, "applied_revision":null,
        "candidate_digest":null, "closure_digest":null, "process":null,
        "stop_id":null, "fence_id":null, "terminal":false,
        "blocker":{"stage":"history_quorum","history":hex::encode([5;32]),"reason":"unavailable"}
    })
}

#[test]
fn fleet_verification_waiting_preserves_unknown_and_refused_failures() {
    for error in [
        FleetCatalogError::Unconfirmed,
        FleetCatalogError::Conflict,
        FleetCatalogError::Corrupt("invalid proof"),
        FleetCatalogError::Closed,
        FleetCatalogError::Storage(anyhow::anyhow!("lost store response")),
        FleetCatalogError::Observation(InspectionError::Refused),
    ] {
        assert!(RaftFleetVerificationBlocker::observation(
            &error,
            RaftFleetVerificationStage::HistoryQuorum,
            Some(hex::encode([5; 32])),
        )
        .is_none());
    }
    for error in [
        InspectionError::Unavailable,
        InspectionError::Unreachable,
        InspectionError::Overloaded,
        InspectionError::StalePeer,
    ] {
        assert!(RaftFleetVerificationBlocker::observation(
            &FleetCatalogError::Observation(error),
            RaftFleetVerificationStage::HistoryQuorum,
            Some(hex::encode([5; 32])),
        )
        .is_some());
    }
}

#[test]
fn fleet_verification_waiting_cannot_masquerade_as_admission() -> anyhow::Result<()> {
    let original: RaftFleetEvacuationProgress = serde_json::from_value(pending())?;
    original.validate()?;
    let mut finalizing = original.clone();
    finalizing.blocker = None;
    finalizing.phase = Phase::Finalizing;
    finalizing.checked_histories = 1;
    finalizing.applied_revision = Some(2);
    finalizing.observed_revision = 2;
    finalizing.candidate_digest = Some(hex::encode([6; 32]));
    finalizing.process = Some(hex::encode([7; 16]));
    finalizing.validate()?;
    finalizing.blocker = original.blocker.clone();
    assert!(finalizing.validate().is_err());
    let mut forged = original.clone();
    forged.checked_histories = 1;
    assert!(forged.validate().is_err());
    forged.blocker.as_mut().unwrap().stage = RaftFleetVerificationStage::TerminalQuorum;
    assert!(forged.validate().is_err());
    forged.blocker.as_mut().unwrap().history = None;
    assert!(forged.validate().is_err());
    forged.phase = Phase::VerifyingRemoval;
    forged.stop_id = Some(hex::encode([8; 16]));
    forged.validate()?;
    let mut missing = pending();
    missing.as_object_mut().unwrap().remove("blocker");
    assert!(serde_json::from_value::<RaftFleetEvacuationProgress>(missing).is_err());
    Ok(())
}

#[test]
fn fleet_verification_waiting_status_refuses_mixed_root_and_terminal_state() -> anyhow::Result<()> {
    let value = json!({"catalog_id":hex::encode([1;32]),"operation_id":hex::encode([2;16]),
        "node_id":hex::encode([4;16]),"state":"Draining","observed_revision":1,
        "exclusion_revision":1,"finalization":null,"removal":null,"cancellation":null,
        "pending_movements":[],"verification":pending()});
    let mut status: RaftFleetEvacuationStatus = serde_json::from_value(value)?;
    status.validate()?;
    status.observed_revision = 2;
    assert!(status.validate().is_err());
    status.observed_revision = 1;
    status.state = RaftFleetNodeState::Finalizing;
    assert!(status.validate().is_err());
    Ok(())
}
