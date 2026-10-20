//! Command ownership through cancellation, failover and delayed proof delivery.
use cellar_logic::consensus::*;
use cellar_logic::raft_command::{Cancellation, CommandState, LeadershipObservation};

fn replica() -> ReplicaIdentity {
    ReplicaIdentity {
        scope: GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: "Counter:one".into(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        node: NodeId::new([5; 16]).unwrap(),
        member: MemberId::new(1).unwrap(),
        disk: DiskLineage::new([6; 16]).unwrap(),
        process: ProcessGeneration::new([7; 16]).unwrap(),
    }
}

fn position(index: u64, term: u64) -> LogPosition {
    LogPosition {
        index: LogIndex::new(index),
        term: Term::new(term),
    }
}

fn progress(index: u64, archived: u64) -> ReplicaProgress {
    ReplicaProgress::checkpoint(replica(), LogIndex::new(index), LogIndex::new(archived)).unwrap()
}

fn leadership(sequence: u64, term: u64, index: u64) -> LeadershipObservation {
    LeadershipObservation {
        replica: replica(),
        sequence,
        term: Term::new(term),
        leader: Some(replica().member),
        is_leader: true,
        protocol_ready: true,
        awaiting_ready: false,
        awaiting_apply: false,
        barrier: position(index, term),
    }
}

fn client(sequence: u64) -> CommandId {
    CommandId::new(ClientId::new([8; 16]).unwrap(), sequence).unwrap()
}

fn core(minimum: Durability) -> (CommandState, RaftAuthority) {
    let mut state = CommandState::new(progress(8, 0), minimum).unwrap();
    let change = state.observe_leadership(leadership(1, 3, 8)).unwrap();
    (state, change.authority.unwrap())
}

fn admit(state: &mut CommandState, authority: &RaftAuthority) -> CommandIdentity {
    state
        .admit(
            authority,
            client(1),
            ProposalId::new([9; 16]).unwrap(),
            Durability::Quorum,
        )
        .unwrap()
}

fn submit(state: &mut CommandState, identity: &CommandIdentity) {
    state.mark_submitted(identity).unwrap();
    state
        .bind_entry(identity, position(9, 3), [10; 32])
        .unwrap();
}

fn proof(identity: &CommandIdentity, stage: ProofStage) -> OutputProof {
    OutputProof::Raft(Box::new(RaftProof {
        command: identity.clone(),
        entry_digest: [10; 32],
        position: position(9, 3),
        stage,
    }))
}

fn publish(state: &mut CommandState, identity: &CommandIdentity) {
    state.observe_progress(progress(9, 0)).unwrap();
    for stage in [
        ProofStage::Committed,
        ProofStage::Applied,
        ProofStage::Queryable,
    ] {
        state.observe_proof(&proof(identity, stage)).unwrap();
    }
}

#[test]
fn result_requires_exact_entry_and_verified_receipt() {
    let (mut state, authority) = core(Durability::Quorum);
    let identity = admit(&mut state, &authority);
    assert_eq!(state.finish(&identity), Err(Refusal::Unproven));
    submit(&mut state, &identity);
    // Even a covering verified image is insufficient without the exact
    // committed proposal and a receipt read from that immutable image.
    state.observe_progress(progress(20, 20)).unwrap();
    assert_eq!(state.finish(&identity), Err(Refusal::Unproven));
    let mut wrong = proof(&identity, ProofStage::Committed);
    let OutputProof::Raft(value) = &mut wrong else {
        unreachable!()
    };
    value.entry_digest = [11; 32];
    assert_eq!(state.observe_proof(&wrong), Err(Refusal::StaleCompletion));
    let OutputProof::Raft(value) = &mut wrong else {
        unreachable!()
    };
    value.position = position(10, 3);
    assert_eq!(state.observe_proof(&wrong), Err(Refusal::WrongHistory));
    for stage in [
        ProofStage::Committed,
        ProofStage::Applied,
        ProofStage::Queryable,
    ] {
        state.observe_proof(&proof(&identity, stage)).unwrap();
    }
    assert_eq!(state.finish(&identity), Err(Refusal::Unproven));
    assert_eq!(
        state.verify_receipt(&identity, client(2), position(9, 3), Durability::Quorum),
        Err(Refusal::StaleCompletion)
    );
    assert_eq!(
        state.verify_receipt(&identity, client(1), position(10, 3), Durability::Quorum),
        Err(Refusal::WrongHistory)
    );
    state
        .verify_receipt(&identity, client(1), position(9, 3), Durability::Quorum)
        .unwrap();
    let permit = state.finish(&identity).unwrap();
    assert_eq!(permit.identity(), &identity);
    assert_eq!(permit.command(), client(1));
    assert_eq!(permit.version().position, position(9, 3));
    assert_eq!(permit.durability(), Durability::Quorum);
    assert!(state.pending().is_none());
    assert_eq!(state.finish(&identity), Err(Refusal::StaleCompletion));
}

#[test]
fn namespace_minimum_and_stronger_retry_wait_for_archive() {
    for minimum in [Durability::Quorum, Durability::S3] {
        let (mut state, authority) = core(minimum);
        let identity = admit(&mut state, &authority);
        assert_eq!(state.receipt_durability(&identity), Ok(minimum));
        submit(&mut state, &identity);
        // A future stage cannot skip the earlier proof, even with high watermarks.
        state.observe_progress(progress(9, 0)).unwrap();
        assert_eq!(
            state.observe_proof(&proof(&identity, ProofStage::Queryable)),
            Err(Refusal::Unproven)
        );
        publish(&mut state, &identity);
        if minimum == Durability::S3 {
            assert_eq!(
                state.verify_receipt(&identity, client(1), position(9, 3), Durability::Quorum),
                Err(Refusal::WrongHistory)
            );
        }
        state
            .verify_receipt(&identity, client(1), position(9, 3), minimum)
            .unwrap();
        state.strengthen(&identity, Durability::S3).unwrap();
        state.strengthen(&identity, Durability::Quorum).unwrap();
        assert_eq!(state.finish(&identity), Err(Refusal::Unproven));
        assert_eq!(
            state.observe_proof(&proof(&identity, ProofStage::Archived)),
            Err(Refusal::Unproven)
        );
        state.observe_progress(progress(9, 9)).unwrap();
        state
            .observe_proof(&proof(&identity, ProofStage::Archived))
            .unwrap();
        assert_eq!(
            state.finish(&identity).unwrap().durability(),
            Durability::S3
        );
    }
}

#[test]
fn submitted_cancellation_retains_producer_and_command_slot() {
    let (mut state, authority) = core(Durability::Quorum);
    let identity = admit(&mut state, &authority);
    submit(&mut state, &identity);
    assert_eq!(state.cancel(&identity), Ok(Cancellation::Submitted));
    assert_eq!(state.pending(), Some(&identity));
    assert_eq!(state.discard_completed(&identity), Err(Refusal::Unproven));
    assert_eq!(
        state.reconcile_completed(&identity, &authority),
        Err(Refusal::Unproven)
    );
    assert_eq!(
        state.admit(
            &authority,
            client(2),
            ProposalId::new([12; 16]).unwrap(),
            Durability::Quorum
        ),
        Err(Refusal::Overloaded)
    );
    publish(&mut state, &identity);
    state
        .verify_receipt(&identity, client(1), position(9, 3), Durability::Quorum)
        .unwrap();
    // The producer can finish and free the slot even though its caller left.
    state.finish(&identity).unwrap();
    state.observe_leadership(leadership(2, 3, 9)).unwrap();
    let next = admit(&mut state, &authority);
    assert_ne!(next.operation, identity.operation);
    assert_eq!(state.cancel(&identity), Err(Refusal::StaleCompletion));
}

#[test]
fn private_cancellation_waits_for_callback_join_and_discard() {
    let (mut state, authority) = core(Durability::Quorum);
    let identity = admit(&mut state, &authority);
    assert_eq!(state.cancel(&identity), Ok(Cancellation::DiscardPrivate));
    assert_eq!(
        state.mark_submitted(&identity),
        Err(Refusal::StaleCompletion)
    );
    assert_eq!(
        state.admit(
            &authority,
            client(2),
            ProposalId::new([12; 16]).unwrap(),
            Durability::Quorum
        ),
        Err(Refusal::Overloaded)
    );
    state.discard_completed(&identity).unwrap();
    let next = admit(&mut state, &authority);
    assert_ne!(identity.operation, next.operation);
    assert_eq!(
        state.discard_completed(&identity),
        Err(Refusal::StaleCompletion)
    );
    assert_eq!(state.pending(), Some(&next));
}

#[test]
fn revoked_term_never_reactivates_from_a_delayed_status_reply() {
    let (mut state, authority) = core(Durability::Quorum);
    let identity = admit(&mut state, &authority);
    let mut follower = leadership(3, 3, 8);
    follower.is_leader = false;
    let change = state.observe_leadership(follower).unwrap();
    assert_eq!(change.retired, Some(identity.clone()));
    assert!(change.authority.is_none());
    assert_eq!(
        state.observe_leadership(leadership(2, 3, 8)),
        Err(Refusal::StaleCompletion)
    );
    // Even a newly numbered observation cannot grant another activation in a
    // term where this process already lost its leader authority.
    assert!(state
        .observe_leadership(leadership(4, 3, 8))
        .unwrap()
        .authority
        .is_none());
    assert_eq!(
        state.mark_submitted(&identity),
        Err(Refusal::StaleCompletion)
    );
    state.observe_progress(progress(9, 0)).unwrap();
    let next = state
        .observe_leadership(leadership(5, 4, 9))
        .unwrap()
        .authority
        .unwrap();
    assert_ne!(next.activation, authority.activation);
    assert!(next.term > authority.term);
    state.discard_completed(&identity).unwrap();
    assert_eq!(
        state.admit(
            &authority,
            client(1),
            ProposalId::new([9; 16]).unwrap(),
            Durability::Quorum
        ),
        Err(Refusal::StaleCompletion)
    );
    admit(&mut state, &next);
}

#[test]
fn unresolved_submission_needs_new_term_and_applied_barrier_to_reconcile() {
    let (mut state, authority) = core(Durability::Quorum);
    let identity = admit(&mut state, &authority);
    submit(&mut state, &identity);
    let next = state.observe_leadership(leadership(2, 4, 10)).unwrap();
    assert_eq!(next.retired, Some(identity.clone()));
    assert!(next.authority.is_none());
    assert_eq!(state.pending(), Some(&identity));
    state.observe_progress(progress(10, 0)).unwrap();
    let next = state
        .observe_leadership(leadership(3, 4, 10))
        .unwrap()
        .authority
        .unwrap();
    assert_eq!(state.finish(&identity), Err(Refusal::StaleCompletion));
    assert_eq!(state.discard_completed(&identity), Err(Refusal::Unproven));
    assert_eq!(
        state.admit(
            &next,
            client(2),
            ProposalId::new([12; 16]).unwrap(),
            Durability::Quorum
        ),
        Err(Refusal::Overloaded)
    );
    state.reconcile_completed(&identity, &next).unwrap();
    admit(&mut state, &next);
    assert_eq!(
        state.observe_proof(&proof(&identity, ProofStage::Queryable)),
        Err(Refusal::StaleCompletion)
    );
}

#[test]
fn delayed_proofs_cannot_cross_any_authority_or_operation_identity() {
    let (mut state, authority) = core(Durability::Quorum);
    let identity = admit(&mut state, &authority);
    submit(&mut state, &identity);
    state.observe_progress(progress(9, 0)).unwrap();
    for field in 0..13 {
        let mut wrong = proof(&identity, ProofStage::Committed);
        let OutputProof::Raft(value) = &mut wrong else {
            unreachable!()
        };
        let replica = &mut value.command.authority.replica;
        match field {
            0 => replica.scope.cluster = ClusterId::new([12; 16]).unwrap(),
            1 => replica.scope.namespace = NamespaceId::new([12; 16]).unwrap(),
            2 => replica.scope.cell = "Counter:other".into(),
            3 => replica.scope.group = GroupId::new([12; 16]).unwrap(),
            4 => replica.scope.incarnation = IncarnationId::new([12; 16]).unwrap(),
            5 => replica.node = NodeId::new([12; 16]).unwrap(),
            6 => replica.member = MemberId::new(2).unwrap(),
            7 => replica.disk = DiskLineage::new([12; 16]).unwrap(),
            8 => replica.process = ProcessGeneration::new([12; 16]).unwrap(),
            9 => value.command.authority.term = Term::new(4),
            10 => value.command.authority.activation = ActivationGeneration::new(2),
            11 => value.command.operation = OperationId::new(2),
            _ => value.command.proposal = ProposalId::new([12; 16]).unwrap(),
        }
        assert_eq!(state.observe_proof(&wrong), Err(Refusal::StaleCompletion));
        assert_eq!(state.finish(&identity), Err(Refusal::Unproven));
    }
    assert_eq!(
        state.observe_proof(&OutputProof::Fleet {
            epoch: 99,
            ticket: 100
        }),
        Err(Refusal::WrongMode)
    );
    publish(&mut state, &identity);
    state
        .verify_receipt(&identity, client(1), position(9, 3), Durability::Quorum)
        .unwrap();
    let mut follower = leadership(2, 4, 9);
    follower.leader = None;
    state.observe_leadership(follower).unwrap();
    assert_eq!(state.finish(&identity), Err(Refusal::StaleCompletion));
    assert_eq!(state.close(), Some(identity.clone()));
    assert_eq!(state.pending(), Some(&identity));
    assert_eq!(
        state.observe_proof(&proof(&identity, ProofStage::Queryable)),
        Err(Refusal::Closed)
    );
}

#[test]
fn admission_needs_current_term_and_verified_queryable_progress() {
    let mut state = CommandState::new(progress(8, 0), Durability::Quorum).unwrap();
    let mut stale_barrier = leadership(1, 4, 8);
    stale_barrier.barrier.term = Term::new(3);
    assert!(state
        .observe_leadership(stale_barrier)
        .unwrap()
        .authority
        .is_none());
    let mut unready = progress(8, 0);
    unready.persist(LogIndex::new(9)).unwrap();
    unready.commit(LogIndex::new(9)).unwrap();
    unready.apply(LogIndex::new(9)).unwrap();
    state.observe_progress(unready.clone()).unwrap();
    assert!(state
        .observe_leadership(leadership(2, 4, 9))
        .unwrap()
        .authority
        .is_none());
    unready.publish(LogIndex::new(9)).unwrap();
    state.observe_progress(unready).unwrap();
    let authority = state
        .observe_leadership(leadership(3, 4, 9))
        .unwrap()
        .authority
        .unwrap();
    assert_eq!(
        state.observe_progress(progress(8, 0)),
        Err(Refusal::ProgressRegression)
    );
    let mut foreign = replica();
    foreign.process = ProcessGeneration::new([12; 16]).unwrap();
    assert_eq!(
        state.observe_progress(
            ReplicaProgress::checkpoint(foreign, LogIndex::new(100), LogIndex::new(0)).unwrap()
        ),
        Err(Refusal::WrongHistory)
    );
    assert_eq!(state.authority(), Some(&authority));
    let mut tail = progress(9, 0);
    tail.persist(LogIndex::new(10)).unwrap();
    state.observe_progress(tail).unwrap();
    assert_eq!(
        state.admit(
            &authority,
            client(1),
            ProposalId::new([9; 16]).unwrap(),
            Durability::Quorum
        ),
        Err(Refusal::LeaderNotReady)
    );
    state.observe_progress(progress(9, 0)).unwrap();
    admit(&mut state, &authority);
}

#[test]
fn retry_uses_the_verified_receipt_cut_without_reproposing_or_relabeling() {
    let (mut state, authority) = core(Durability::Quorum);
    let identity = admit(&mut state, &authority);
    let version = CommittedVersion {
        scope: replica().scope,
        position: position(7, 2),
    };
    assert_eq!(
        state.recover_receipt(&identity, client(2), version.clone(), Durability::Quorum),
        Err(Refusal::StaleCompletion)
    );
    let mut foreign = version.clone();
    foreign.scope.incarnation = IncarnationId::new([12; 16]).unwrap();
    assert_eq!(
        state.recover_receipt(&identity, client(1), foreign, Durability::Quorum),
        Err(Refusal::WrongHistory)
    );
    let mut impossible = version.clone();
    impossible.position = position(8, 2);
    assert_eq!(
        state.recover_receipt(&identity, client(1), impossible, Durability::Quorum),
        Err(Refusal::WrongHistory)
    );
    state
        .recover_receipt(&identity, client(1), version.clone(), Durability::S3)
        .unwrap();
    assert_eq!(
        state.mark_submitted(&identity),
        Err(Refusal::StaleCompletion)
    );
    assert_eq!(state.finish(&identity), Err(Refusal::Unproven));
    state.observe_progress(progress(8, 7)).unwrap();
    let permit = state.finish(&identity).unwrap();
    assert_eq!(permit.version(), &version);
    assert_eq!(permit.durability(), Durability::S3);

    let identity = admit(&mut state, &authority);
    state
        .recover_receipt(&identity, client(1), version.clone(), Durability::Quorum)
        .unwrap();
    state.strengthen(&identity, Durability::S3).unwrap();
    assert_eq!(state.receipt_durability(&identity), Ok(Durability::Quorum));
    assert_eq!(state.cancel(&identity), Ok(Cancellation::DiscardPrivate));
    assert_eq!(state.finish(&identity), Err(Refusal::Unproven));
    state.discard_completed(&identity).unwrap();
}

#[test]
fn protocol_work_during_a_private_callback_prevents_submission() {
    for blocked in 0..3 {
        let (mut state, authority) = core(Durability::Quorum);
        let identity = admit(&mut state, &authority);
        let mut observation = leadership(2, 3, 8);
        match blocked {
            0 => observation.protocol_ready = false,
            1 => observation.awaiting_ready = true,
            _ => observation.awaiting_apply = true,
        }
        assert_eq!(
            state.observe_leadership(observation).unwrap().authority,
            Some(authority.clone())
        );
        assert_eq!(
            state.mark_submitted(&identity),
            Err(Refusal::LeaderNotReady)
        );
        assert_eq!(state.pending(), Some(&identity));
        state.observe_leadership(leadership(3, 3, 8)).unwrap();
        state.mark_submitted(&identity).unwrap();
    }
}
