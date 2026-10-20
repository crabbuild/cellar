//! Deterministic boundary fixture using the production pure decision contracts.
//! This fixture does not simulate elections or substitute for real Raft tests.

use cellar_logic::consensus::*;

pub fn scope() -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:alpha".into(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    }
}

pub fn command() -> CommandIdentity {
    CommandIdentity {
        authority: RaftAuthority {
            replica: ReplicaIdentity {
                scope: scope(),
                node: NodeId::new([5; 16]).unwrap(),
                member: MemberId::new(1).unwrap(),
                disk: DiskLineage::new([6; 16]).unwrap(),
                process: ProcessGeneration::new([7; 16]).unwrap(),
            },
            term: Term::new(3),
            activation: ActivationGeneration::new(1),
        },
        operation: OperationId::new(1),
        proposal: ProposalId::new([8; 16]).unwrap(),
    }
}

pub fn position() -> LogPosition {
    LogPosition {
        index: LogIndex::new(2),
        term: Term::new(3),
    }
}

pub fn proof(stage: ProofStage) -> OutputProof {
    OutputProof::Raft(Box::new(RaftProof {
        command: command(),
        entry_digest: [9; 32],
        position: position(),
        stage,
    }))
}

pub struct BoundaryModel {
    pub now: u64,
    pub progress: ReplicaProgress,
    pub gate: CommandGate,
    pub pending_sync: Option<LogIndex>,
    pub completion_queue: std::collections::VecDeque<OutputProof>,
}

impl BoundaryModel {
    pub fn new(required: Durability) -> Self {
        Self {
            now: 0,
            progress: ReplicaProgress::new(command().authority.replica).unwrap(),
            gate: CommandGate::new(command(), [9; 32], required).unwrap(),
            pending_sync: None,
            completion_queue: std::collections::VecDeque::new(),
        }
    }

    pub fn append_without_sync(&mut self) {
        self.pending_sync = Some(position().index);
    }

    pub fn finish_sync(&mut self, succeeds: bool) -> Result<(), Refusal> {
        if !succeeds {
            return Err(Refusal::Unproven);
        }
        let index = self.pending_sync.take().ok_or(Refusal::Unproven)?;
        self.progress.persist(index)
    }

    pub fn deliver(&mut self, stage: ProofStage) -> Result<(), Refusal> {
        let mut candidate = self.progress.clone();
        match stage {
            ProofStage::Committed => candidate.commit(position().index)?,
            ProofStage::Applied => {
                candidate.apply(LogIndex::new(1))?;
                candidate.apply(position().index)?;
            }
            ProofStage::Queryable => candidate.publish(position().index)?,
            ProofStage::Archived => candidate.archive(position().index)?,
        }
        self.gate.observe(&proof(stage), &candidate)?;
        self.progress = candidate;
        Ok(())
    }

    pub fn tick(&mut self) {
        self.now += 1;
    }
}
