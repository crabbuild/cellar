// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::{
    ClusterId, GroupId, IncarnationId, LogIndex, LogPosition, NamespaceId,
};

fn prepared() -> CellPreparedCommand {
    CellPreparedCommand::new(
        vec![1],
        ProposalId::new([1; 16]).unwrap(),
        Term::new(1),
        GroupHistory {
            cluster: ClusterId::new([2; 16]).unwrap(),
            namespace: NamespaceId::new([3; 16]).unwrap(),
            group: GroupId::new([4; 16]).unwrap(),
            incarnation: IncarnationId::new([5; 16]).unwrap(),
        },
        Arc::new(()),
    )
}

#[test]
fn cancellation_seals_every_delayed_submission_clone() {
    let command = prepared();
    let delayed = command.clone();
    let proof = command.cancel_before_submission().unwrap().unwrap();
    assert!(delayed.matches_rejection(&proof));
    assert!(matches!(
        delayed.submit(&command.submission.engine, command.group, || panic!(
            "sealed clone reached append"
        )),
        Ok(CellSubmissionOutcome::Rejected(_))
    ));
}

#[test]
fn attempted_error_cannot_turn_into_cancellation_or_capacity_proof() {
    let command = prepared();
    assert!(command
        .submit(&command.submission.engine, command.group, || Err(
            RuntimeError::Protocol("uncertain append".into())
        ))
        .is_err());
    assert!(command.cancel_before_submission().unwrap().is_none());
    assert!(matches!(
        command.submit(&command.submission.engine, command.group, || Ok(None)),
        Err(RuntimeError::Overloaded)
    ));
    assert!(command.rejection().unwrap().is_none());
    assert!(command.cancel_before_submission().unwrap().is_none());
}

#[test]
fn entering_or_accepted_submission_cannot_be_cancelled() {
    let command = prepared();
    let accepted = CellGroupProposal {
        group: command.group,
        position: LogPosition {
            index: LogIndex::new(2),
            term: Term::new(1),
        },
        digest: [9; 32],
    };
    assert!(matches!(
        command.submit(&command.submission.engine, command.group, || {
            assert!(command.cancel_before_submission().unwrap().is_none());
            Ok(Some(accepted))
        }),
        Ok(CellSubmissionOutcome::Accepted(_))
    ));
    assert!(command.cancel_before_submission().unwrap().is_none());
}
