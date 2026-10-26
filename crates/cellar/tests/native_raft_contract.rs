// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public contracts for Raft identity, framing and output admission.
#[path = "support/raft_model.rs"]
mod model;

use cellar::application::consensus::{
    CellUpdate, EnvelopeLimits, LtxImage, LtxUpdate, ProtocolError,
};
use cellar_logic::consensus::*;
use cellar_logic::NamespaceMode;
use model::*;
use sha2::{Digest, Sha256};

#[test]
fn mode_isolation() {
    let fleet = OutputProof::Fleet {
        epoch: 3,
        ticket: 2,
    };
    let raft = proof(ProofStage::Committed);
    assert_eq!(NamespaceMode::default(), NamespaceMode::Fleet);
    assert_eq!(
        fleet.validate_mode(NamespaceMode::Raft),
        Err(Refusal::WrongMode)
    );
    assert_eq!(
        raft.validate_mode(NamespaceMode::Fleet),
        Err(Refusal::WrongMode)
    );
    fleet.validate_mode(NamespaceMode::Fleet).unwrap();
    raft.validate_mode(NamespaceMode::Raft).unwrap();
    let mut model = BoundaryModel::new(Durability::Quorum);
    assert_eq!(
        model.gate.observe(&fleet, &model.progress),
        Err(Refusal::WrongMode)
    );
    assert!(!model.gate.releasable());
}

#[test]
fn incarnation_token_scope() {
    let required = CommittedVersion {
        scope: scope(),
        position: position(),
    };
    let mut served = required.clone();
    served.position.index = LogIndex::new(100);
    assert_eq!(served.covers(&required), Ok(true));
    for field in 0..5 {
        let mut unrelated = served.clone();
        match field {
            0 => unrelated.scope.cluster = ClusterId::new([11; 16]).unwrap(),
            1 => unrelated.scope.namespace = NamespaceId::new([11; 16]).unwrap(),
            2 => unrelated.scope.cell = "Counter:beta".into(),
            3 => unrelated.scope.group = GroupId::new([11; 16]).unwrap(),
            _ => unrelated.scope.incarnation = IncarnationId::new([11; 16]).unwrap(),
        }
        assert_eq!(unrelated.covers(&required), Err(Refusal::WrongHistory));
    }
    served.position.index = required.position.index;
    served.position.term = Term::new(4);
    assert_eq!(served.covers(&required), Err(Refusal::WrongHistory));
    served.position = LogPosition::default();
    assert_eq!(served.covers(&required), Ok(false));
}

#[test]
fn speculative_visibility_model() {
    for required in [Durability::Quorum, Durability::S3] {
        let mut model = BoundaryModel::new(required);
        model.append_without_sync();
        model.tick();
        assert_eq!(model.finish_sync(false), Err(Refusal::Unproven));
        assert_eq!(model.deliver(ProofStage::Committed), Err(Refusal::Unproven));
        assert_eq!(model.deliver(ProofStage::Queryable), Err(Refusal::Unproven));
        assert!(!model.gate.releasable());
        model.finish_sync(true).unwrap();
        model.deliver(ProofStage::Committed).unwrap();
        assert_eq!(model.deliver(ProofStage::Archived), Err(Refusal::Unproven));
        assert!(!model.gate.releasable());
        model.deliver(ProofStage::Applied).unwrap();
        assert!(!model.gate.releasable());
        model.deliver(ProofStage::Queryable).unwrap();
        assert_eq!(model.gate.releasable(), required == Durability::Quorum);
        model.gate.strengthen(Durability::S3);
        model.gate.strengthen(Durability::Quorum);
        assert!(!model.gate.releasable());
        model.deliver(ProofStage::Archived).unwrap();
        assert!(model.gate.releasable());
        assert_eq!(model.gate.position(), Some(position()));
    }
    let mut progress = ReplicaProgress::new(command().authority.replica).unwrap();
    progress.persist(LogIndex::new(10)).unwrap();
    progress.commit(LogIndex::new(3)).unwrap();
    assert_eq!(
        progress.apply(LogIndex::new(2)),
        Err(Refusal::NonContiguous)
    );
    assert_eq!(
        progress.replace_suffix(LogIndex::new(2)),
        Err(Refusal::Unproven)
    );
    progress.replace_suffix(LogIndex::new(4)).unwrap();
    assert_eq!(progress.persisted(), LogIndex::new(4));
}

#[test]
fn late_completion_model() {
    let mut model = BoundaryModel::new(Durability::Quorum);
    model.append_without_sync();
    model.finish_sync(true).unwrap();
    model.deliver(ProofStage::Committed).unwrap();
    model.deliver(ProofStage::Applied).unwrap();
    let mut other = command().authority.replica;
    other.scope.cell = "Counter:another-cell".into();
    let foreign =
        ReplicaProgress::checkpoint(other, LogIndex::new(100), LogIndex::new(100)).unwrap();
    assert_eq!(
        model.gate.observe(&proof(ProofStage::Queryable), &foreign),
        Err(Refusal::WrongHistory)
    );
    assert!(!model.gate.releasable());
    for field in 0..10 {
        let OutputProof::Raft(mut stale) = proof(ProofStage::Queryable) else {
            unreachable!()
        };
        match field {
            0 => stale.command.authority.term = Term::new(4),
            1 => stale.command.authority.activation = ActivationGeneration::new(2),
            2 => {
                stale.command.authority.replica.process = ProcessGeneration::new([12; 16]).unwrap()
            }
            3 => stale.command.authority.replica.disk = DiskLineage::new([12; 16]).unwrap(),
            4 => stale.command.authority.replica.node = NodeId::new([12; 16]).unwrap(),
            5 => stale.command.authority.replica.member = MemberId::new(2).unwrap(),
            6 => stale.command.operation = OperationId::new(2),
            7 => stale.command.proposal = ProposalId::new([12; 16]).unwrap(),
            8 => stale.entry_digest = [12; 32],
            _ => {
                stale.command.authority.replica.scope.incarnation =
                    IncarnationId::new([12; 16]).unwrap()
            }
        }
        model.completion_queue.push_back(OutputProof::Raft(stale));
    }
    while let Some(stale) = model.completion_queue.pop_front() {
        assert_eq!(
            model.gate.observe(&stale, &model.progress),
            Err(Refusal::StaleCompletion)
        );
        assert!(!model.gate.releasable());
    }
    let authority = command().authority;
    assert_eq!(
        authority.admit(Some(authority.replica.member), position(), &model.progress),
        Err(Refusal::LeaderNotReady)
    );
    model.deliver(ProofStage::Queryable).unwrap();
    authority
        .admit(Some(authority.replica.member), position(), &model.progress)
        .unwrap();
    assert_eq!(
        authority.admit(None, position(), &model.progress),
        Err(Refusal::NotLeader)
    );
    model.gate.retire();
    assert!(!model.gate.releasable());
    assert_eq!(
        model
            .gate
            .observe(&proof(ProofStage::Queryable), &model.progress),
        Err(Refusal::StaleCompletion)
    );
}

fn update() -> CellUpdate {
    CellUpdate {
        scope: scope(),
        application_digest: [21; 32],
        proposal: command().proposal,
        base: LtxImage {
            epoch: 1,
            txid: 8,
            checksum: 0,
            commitment: [22; 32].into(),
        },
        result: LtxImage {
            epoch: 1,
            txid: 10,
            checksum: 0,
            commitment: [23; 32].into(),
        },
        files: vec![
            LtxUpdate {
                min_txid: 9,
                max_txid: 9,
                page_count: 1,
                decoded_bytes: 4096,
                bytes: vec![1, 2, 3],
            },
            LtxUpdate {
                min_txid: 10,
                max_txid: 10,
                page_count: 1,
                decoded_bytes: 4096,
                bytes: vec![4, 5],
            },
        ],
    }
}

fn rewrite_header(bytes: &[u8], transform: impl FnOnce(String) -> String) -> Vec<u8> {
    let length = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let header = transform(String::from_utf8(bytes[20..20 + length].to_vec()).unwrap());
    let mut rewritten = bytes[..8].to_vec();
    rewritten.extend_from_slice(&(header.len() as u32).to_be_bytes());
    rewritten.extend_from_slice(&bytes[12..20]);
    rewritten.extend_from_slice(header.as_bytes());
    rewritten.extend_from_slice(&bytes[20 + length..bytes.len() - 32]);
    let digest = Sha256::digest(&rewritten);
    rewritten.extend_from_slice(&digest);
    rewritten
}

#[test]
fn envelope_bounds() {
    let limits = EnvelopeLimits::default();
    let update = update();
    let bytes = update.encode(limits).unwrap();
    assert_eq!(CellUpdate::decode(&bytes, limits).unwrap(), update);
    for end in 0..bytes.len() {
        assert!(CellUpdate::decode(&bytes[..end], limits).is_err());
    }
    let mut extra = bytes.clone();
    extra.push(0);
    assert_eq!(
        CellUpdate::decode(&extra, limits),
        Err(ProtocolError::Framing)
    );
    let mut corrupt = bytes.clone();
    corrupt[25] ^= 1;
    assert_eq!(
        CellUpdate::decode(&corrupt, limits),
        Err(ProtocolError::Digest)
    );
    let mut excessive = bytes.clone();
    excessive[12..20].copy_from_slice(&u64::MAX.to_be_bytes());
    assert_eq!(
        CellUpdate::decode(&excessive, limits),
        Err(ProtocolError::Limit)
    );
    let malformed = [
        rewrite_header(&bytes, |header| {
            header.replacen("\"scope\":", "\"scope\":{},\"scope\":", 1)
        }),
        rewrite_header(&bytes, |header| {
            header.replacen("\"cell\":", "\"cell\":\"forged\",\"cell\":", 1)
        }),
        rewrite_header(&bytes, |header| {
            header.replacen("\"epoch\":\"1\"", "\"epoch\":1", 1)
        }),
        rewrite_header(&bytes, |header| {
            header.replacen("\"epoch\":\"1\"", "\"epoch\":\"01\"", 1)
        }),
        rewrite_header(&bytes, |header| {
            header.replacen("\"txid\":\"8\"", "\"txid\":\"18446744073709551616\"", 1)
        }),
        rewrite_header(&bytes, |header| {
            header.replacen("\"offset\":\"3\"", "\"offset\":\"2\"", 1)
        }),
        rewrite_header(&bytes, |header| {
            header.replacen(
                "\"decoded_bytes\":\"4096\"",
                "\"decoded_bytes\":\"67108865\"",
                1,
            )
        }),
        rewrite_header(&bytes, |header| {
            header.replacen("\"min_txid\":\"10\"", "\"min_txid\":\"11\"", 1)
        }),
        rewrite_header(&bytes, |header| {
            header.replacen("{", "{\"required_future_field\":true,", 1)
        }),
    ];
    for malformed in malformed {
        assert!(CellUpdate::decode(&malformed, limits).is_err());
    }
    assert_eq!(
        CellUpdate::decode(
            &bytes,
            EnvelopeLimits {
                max_files: 1,
                ..limits
            }
        ),
        Err(ProtocolError::Limit)
    );
    assert_eq!(
        CellUpdate::decode(
            &bytes,
            EnvelopeLimits {
                max_header_bytes: 1,
                ..limits
            }
        ),
        Err(ProtocolError::Limit)
    );
    assert_eq!(
        CellUpdate::decode(
            &bytes,
            EnvelopeLimits {
                max_decoded_bytes: 8191,
                ..limits
            }
        ),
        Err(ProtocolError::Limit)
    );
}

#[test]
fn birth_retry_identity() {
    let command = command();
    let claim = BootstrapClaim {
        scope: scope(),
        member: command.authority.replica.member,
        disk: command.authority.replica.disk,
        birth_digest: [31; 32],
    };
    assert_eq!(
        claim.reconcile(None, None),
        Ok(BootstrapAction::InitializeLocal)
    );
    assert_eq!(
        claim.reconcile(Some(&claim), None),
        Ok(BootstrapAction::EstablishClaim)
    );
    // Lost CAS response: read-back resolves the already-created claim. It does
    // not allocate another group/member or initialize another term/vote store.
    assert_eq!(
        claim.reconcile(Some(&claim), Some(&claim)),
        Ok(BootstrapAction::Resume)
    );
    assert_eq!(
        claim.reconcile(None, Some(&claim)),
        Err(Refusal::LostStableState)
    );
    let mut other_disk = claim.clone();
    other_disk.disk = DiskLineage::new([32; 16]).unwrap();
    assert_eq!(
        other_disk.reconcile(Some(&other_disk), Some(&claim)),
        Err(Refusal::WrongHistory)
    );
    let mut other_birth = claim.clone();
    other_birth.birth_digest = [32; 32];
    assert_eq!(
        other_birth.reconcile(Some(&claim), Some(&claim)),
        Err(Refusal::WrongHistory)
    );
}
