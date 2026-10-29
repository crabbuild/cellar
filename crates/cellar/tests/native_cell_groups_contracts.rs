// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cell-group protocol boundary tests. These do not qualify a running group.

use cellar::application::consensus::verification::cell_groups::*;
use cellar::application::consensus::NamespaceId;
use cellar::application::consensus::{
    ClusterId, GroupId, IncarnationId, LogIndex, LogPosition, ProposalId, Term, VersionError,
    VersionKeys, VersionSigningKey, VersionToken,
};
use std::time::Duration;

fn observation() -> CellVersionObservation {
    let history = CellHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: CellId::new("alpha").unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    };
    let group = GroupHistory {
        cluster: history.cluster,
        namespace: history.namespace,
        group: GroupId::new([4; 16]).unwrap(),
        incarnation: IncarnationId::new([5; 16]).unwrap(),
    };
    CellVersionObservation {
        version: CellVersion {
            history,
            sequence: MutationSequence::new(7).unwrap(),
            image: CellImage {
                epoch: 1,
                txid: 10,
                checksum: 17,
                commitment: CellImageCommitment::Materialized([8; 32]),
            },
        },
        witness: CommitWitness {
            group,
            position: LogPosition {
                index: LogIndex::new(100),
                term: Term::new(9),
            },
            proposal: ProposalId::new([6; 16]).unwrap(),
        },
    }
}

fn keys() -> VersionKeys {
    VersionKeys::new(
        VersionSigningKey::new([7; 32], 100_000).unwrap(),
        vec![],
        Duration::from_secs(10),
    )
    .unwrap()
}

#[test]
fn version_survives_transfer_not_disaster() {
    let keys = keys();
    let original = observation();
    let schema = [42; 32];
    let token = keys.issue_cell(&original, schema, 100).unwrap();
    assert!(VersionToken::from_encoded(token.as_str()).is_err());
    let decoded = keys
        .verify_cell(&token, &original.version.history, schema, 101)
        .unwrap();
    assert_eq!(decoded, original);
    let mut moved = original.clone();
    moved.witness.group.group = GroupId::new([10; 16]).unwrap();
    moved.witness.group.incarnation = IncarnationId::new([11; 16]).unwrap();
    moved.witness.position = LogPosition {
        index: LogIndex::new(2),
        term: Term::new(1),
    };
    moved.version.sequence = moved.version.sequence.checked_next().unwrap();
    moved.version.image.txid += 1;
    let successor = keys.issue_cell(&moved, schema, 102).unwrap();
    let successor = keys
        .verify_cell(&successor, &original.version.history, schema, 103)
        .unwrap();
    assert!(successor.version.covers(&decoded.version).unwrap());
    assert!(!decoded.version.covers(&successor.version).unwrap());
    for field in 0..4 {
        let mut other = original.version.history.clone();
        match field {
            0 => other.cluster = ClusterId::new([12; 16]).unwrap(),
            1 => other.namespace = NamespaceId::new([12; 16]).unwrap(),
            2 => other.cell = CellId::new("beta").unwrap(),
            _ => other.incarnation = IncarnationId::new([12; 16]).unwrap(),
        }
        assert_eq!(
            keys.verify_cell(&token, &other, schema, 103),
            Err(VersionError::WrongHistory)
        );
    }
    assert_eq!(
        keys.verify_cell(&token, &original.version.history, [43; 32], 103),
        Err(VersionError::WrongSchema)
    );
    assert_eq!(
        keys.verify_cell(&token, &original.version.history, schema, 10_100),
        Err(VersionError::Expired)
    );
    assert_eq!(
        keys.verify_cell(&token, &original.version.history, schema, 99),
        Err(VersionError::Expired)
    );
}

#[test]
fn cell_token_bounds_tampering_and_commitment_domains() {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let keys = keys();
    let mut original = observation();
    original.version.history.cell = CellId::new("a").unwrap();
    let shortest = keys.issue_cell(&original, [42; 32], 100).unwrap();
    assert_eq!(
        URL_SAFE_NO_PAD.decode(shortest.as_str()).unwrap().len(),
        368
    );
    assert_eq!(
        keys.verify_cell(&shortest, &original.version.history, [42; 32], 101)
            .unwrap(),
        original
    );
    original.version.history.cell = CellId::new("é".repeat(512)).unwrap();
    original.version.image.commitment = CellImageCommitment::Extents {
        root: [9; 32],
        manifest_bytes: 112,
    };
    let token = keys.issue_cell(&original, [42; 32], 100).unwrap();
    assert_eq!(
        keys.verify_cell(&token, &original.version.history, [42; 32], 101)
            .unwrap(),
        original
    );
    let bytes = URL_SAFE_NO_PAD.decode(token.as_str()).unwrap();
    for offset in [0, 8, 40, 120, bytes.len() - 65, bytes.len() - 1] {
        let mut changed = bytes.clone();
        changed[offset] ^= 1;
        if let Ok(tampered) = CellVersionToken::from_encoded(&URL_SAFE_NO_PAD.encode(changed)) {
            assert!(keys
                .verify_cell(&tampered, &original.version.history, [42; 32], 101)
                .is_err());
        }
    }
    for end in 0..bytes.len() {
        if let Ok(truncated) =
            CellVersionToken::from_encoded(&URL_SAFE_NO_PAD.encode(&bytes[..end]))
        {
            assert!(keys
                .verify_cell(&truncated, &original.version.history, [42; 32], 101)
                .is_err());
        }
    }
    assert_eq!(
        CellVersionToken::from_encoded(&"a".repeat(2049)),
        Err(VersionError::Limit)
    );
    original.version.image.commitment = CellImageCommitment::Extents {
        root: [9; 32],
        manifest_bytes: 113,
    };
    assert_eq!(
        keys.issue_cell(&original, [42; 32], 100),
        Err(VersionError::Invalid)
    );
    original.version.image.commitment = CellImageCommitment::Materialized([9; 32]);
    original.witness.group.namespace = NamespaceId::new([15; 16]).unwrap();
    assert_eq!(
        keys.issue_cell(&original, [42; 32], 100),
        Err(VersionError::Invalid)
    );
}

#[test]
fn identity_and_hash_boundaries() {
    let namespace = NamespaceId::new([2; 16]).unwrap();
    let alpha = CellId::new("alpha").unwrap();
    let key = cell_routing_key(namespace, &alpha);
    // Independent SHA-256 golden vector for the specified preimage, including NUL.
    assert_eq!(
        hex::encode(key.digest()),
        "9125bede939b7512fcd2b4567a7c60ee066a3baf5847a265088e48449e408f1b"
    );
    assert_ne!(
        key,
        cell_routing_key(NamespaceId::new([3; 16]).unwrap(), &alpha)
    );
    let tied = CellKey::from_digest(key.digest(), CellId::new("beta").unwrap());
    assert!(key < tied);
    let (left, right) = CellRange::all().split(tied.clone()).unwrap();
    assert!(left.contains(&key) && !right.contains(&key));
    assert!(!left.contains(&tied) && right.contains(&tied));
    assert_eq!(left.merge(&right).unwrap(), CellRange::all());
    assert!(right.merge(&left).is_err());
    assert!(CellRange::new(CellBound::PositiveInfinity, CellBound::NegativeInfinity).is_err());
    assert!(CellId::new("").is_err());
    assert!(CellId::new("a\nb").is_err());
    assert!(CellId::new("a".repeat(1025)).is_err());
    assert!(MutationSequence::new(u64::MAX)
        .unwrap()
        .checked_next()
        .is_err());
}

#[allow(dead_code)]
#[path = "support/raft_ltx_fixture.rs"]
mod image_fixture;

fn group_route() -> GroupRoute {
    GroupRoute {
        history: observation().witness.group,
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [22; 32],
        schema_digest: [42; 32],
    }
}

fn binding(route: &GroupRoute) -> GroupEntryBinding {
    GroupEntryBinding {
        group: route.history,
        route_epoch: route.epoch,
        birth_digest: route.birth_digest,
        schema_digest: route.schema_digest,
        proposal: ProposalId::new([6; 16]).unwrap(),
    }
}

fn ltx_cut() -> image_fixture::Cut {
    let before = image_fixture::image(
        None,
        "CREATE TABLE value(n INTEGER); INSERT INTO value VALUES (1);",
    )
    .unwrap();
    let after = image_fixture::image(Some(&before), "UPDATE value SET n=2;").unwrap();
    image_fixture::Cut::new(&before, &after, 1, true, true)
}

fn image_value(image: cellar_ltx::apply::ImageState) -> cellar::application::consensus::LtxImage {
    cellar::application::consensus::LtxImage {
        epoch: image.epoch,
        txid: image.txid.0,
        checksum: image.checksum,
        commitment: image.digest.into(),
    }
}

fn mutation_entry(cut: &image_fixture::Cut) -> CellGroupEntry<'_> {
    CellGroupEntry {
        binding: binding(&group_route()),
        command: GroupCommand::Mutation(GroupMutation {
            cell: observation().version.history,
            sequence: MutationSequence::new(2).unwrap(),
            base: image_value(cut.base),
            result: image_value(cut.result),
            files: vec![GroupLtx {
                min_txid: cut.base.txid.0 + 1,
                max_txid: cut.result.txid.0,
                page_count: cut.pages.len() as u64,
                decoded_bytes: cut.pages.len() as u64 * 512,
                bytes: &cut.bytes,
            }],
        }),
    }
}

fn rehash(bytes: &mut [u8]) {
    use sha2::{Digest, Sha256};
    let end = bytes.len() - 32;
    let hash = Sha256::digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&hash);
}

#[test]
fn envelope_rejects_wrong_cell_and_domain() {
    use cellar::application::consensus::{
        CellUpdate, EnvelopeLimits, GroupScope, LtxUpdate, ProtocolError,
    };
    let cut = ltx_cut();
    assert!(cellar_ltx::ltx::decode_file(&cut.bytes).is_ok());
    let route = group_route();
    let limits = GroupEntryLimits::default();
    let entry = mutation_entry(&cut);
    let bytes = entry.encode(&route, limits).unwrap();
    let decoded = CellGroupEntry::decode(&bytes, &route, limits).unwrap();
    assert_eq!(decoded, entry);
    let GroupCommand::Mutation(update) = &decoded.command else {
        panic!("mutation changed kind")
    };
    let current = CellVersion {
        history: observation().version.history,
        sequence: MutationSequence::new(1).unwrap(),
        image: image_value(cut.base).into(),
    };
    assert!(update.check_base(&current).is_ok());
    for field in 0..6 {
        let mut other = current.clone();
        match field {
            0 => other.history.cell = CellId::new("beta").unwrap(),
            1 => other.history.incarnation = IncarnationId::new([90; 16]).unwrap(),
            2 => other.history.namespace = NamespaceId::new([90; 16]).unwrap(),
            3 => other.sequence = MutationSequence::new(2).unwrap(),
            4 => other.image.checksum ^= 1,
            _ => other.image.commitment = CellImageCommitment::Materialized([90; 32]),
        }
        assert_eq!(update.check_base(&other), Err(ProtocolError::Chain));
    }
    for field in 0..7 {
        let mut other = route.clone();
        match field {
            0 => other.history.cluster = ClusterId::new([90; 16]).unwrap(),
            1 => other.history.namespace = NamespaceId::new([90; 16]).unwrap(),
            2 => other.history.group = GroupId::new([90; 16]).unwrap(),
            3 => other.history.incarnation = IncarnationId::new([90; 16]).unwrap(),
            4 => other.epoch = RouteEpoch::new(2).unwrap(),
            5 => other.schema_digest = [90; 32],
            _ => other.birth_digest = [90; 32],
        }
        assert_eq!(
            CellGroupEntry::decode(&bytes, &other, limits),
            Err(ProtocolError::Identity)
        );
    }
    let mut outside = route.clone();
    outside.range = CellRange::all()
        .split(cell_routing_key(
            route.history.namespace,
            &current.history.cell,
        ))
        .unwrap()
        .0;
    assert_eq!(
        CellGroupEntry::decode(&bytes, &outside, limits),
        Err(ProtocolError::Identity)
    );
    let old = CellUpdate {
        scope: GroupScope {
            cluster: route.history.cluster,
            namespace: route.history.namespace,
            group: route.history.group,
            incarnation: route.history.incarnation,
            cell: current.history.cell.as_str().into(),
        },
        application_digest: route.schema_digest,
        proposal: entry.binding.proposal,
        base: image_value(cut.base),
        result: image_value(cut.result),
        files: vec![LtxUpdate {
            min_txid: 2,
            max_txid: 2,
            page_count: cut.pages.len() as u64,
            decoded_bytes: cut.pages.len() as u64 * 512,
            bytes: cut.bytes.clone(),
        }],
    }
    .encode(EnvelopeLimits::default())
    .unwrap();
    assert_eq!(
        CellGroupEntry::decode(&old, &route, limits),
        Err(ProtocolError::Protocol)
    );
    assert!(CellUpdate::decode(&bytes, EnvelopeLimits::default()).is_err());
    let mut unknown = bytes.clone();
    unknown[24] ^= 1; // New-format framing with an unrecognized semantic domain.
    rehash(&mut unknown);
    assert_eq!(
        CellGroupEntry::decode(&unknown, &route, limits),
        Err(ProtocolError::Protocol)
    );
}

#[test]
fn codec_limits_before_allocation() {
    use cellar::application::consensus::ProtocolError;
    let cut = ltx_cut();
    let route = group_route();
    let limits = GroupEntryLimits::default();
    let bytes = mutation_entry(&cut).encode(&route, limits).unwrap();
    let header_len = u32::from_be_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let decoded = CellGroupEntry::decode(&bytes, &route, limits).unwrap();
    let GroupCommand::Mutation(update) = decoded.command else {
        panic!("not a mutation")
    };
    assert_eq!(
        update.files[0].bytes.as_ptr(),
        bytes[24 + header_len..].as_ptr()
    );
    for end in 0..bytes.len() {
        assert!(
            CellGroupEntry::decode(&bytes[..end], &route, limits).is_err(),
            "truncation {end}"
        );
    }
    for field in 0..5 {
        let mut bounded = limits;
        match field {
            0 => bounded.envelope.max_header_bytes = 1,
            1 => bounded.envelope.max_payload_bytes = 1,
            2 => bounded.envelope.max_decoded_bytes = 1,
            3 => bounded.envelope.max_pages = 1,
            _ => bounded.envelope.max_files = 0,
        }
        assert_eq!(
            CellGroupEntry::decode(&bytes, &route, bounded),
            Err(ProtocolError::Limit)
        );
    }
    let file_count = 24 + 185 + 16 + 2 + "alpha".len() + 8 + 2 * 61;
    let malformed_fields = [
        (12, u32::MAX.to_be_bytes().to_vec()),
        (16, u64::MAX.to_be_bytes().to_vec()),
        (file_count, u32::MAX.to_be_bytes().to_vec()),
        (file_count + 4, u64::MAX.to_be_bytes().to_vec()),
        (file_count + 4 + 24, u64::MAX.to_be_bytes().to_vec()),
        (file_count + 4 + 32, u64::MAX.to_be_bytes().to_vec()),
    ];
    for (offset, value) in malformed_fields {
        let mut oversized = bytes.clone();
        oversized[offset..offset + value.len()].copy_from_slice(&value);
        rehash(&mut oversized);
        assert_eq!(
            CellGroupEntry::decode(&oversized, &route, limits),
            Err(ProtocolError::Limit),
            "field {offset}"
        );
    }
    let mut unknown = bytes.clone();
    unknown[24 + 184] = 255;
    rehash(&mut unknown);
    assert_eq!(
        CellGroupEntry::decode(&unknown, &route, limits),
        Err(ProtocolError::Protocol)
    );
    let mut marker = bytes.clone();
    marker[8..12].copy_from_slice(&2_u32.to_be_bytes());
    rehash(&mut marker);
    assert_eq!(
        CellGroupEntry::decode(&marker, &route, limits),
        Err(ProtocolError::Protocol)
    );
    // Fixed-position fields occur exactly once. An appended duplicate group
    // identity cannot be reinterpreted as an override or optional metadata.
    let mut duplicate = bytes.clone();
    duplicate.splice(
        24 + header_len..24 + header_len,
        bytes[56..120].iter().copied(),
    );
    duplicate[12..16].copy_from_slice(&((header_len + 64) as u32).to_be_bytes());
    rehash(&mut duplicate);
    assert_eq!(
        CellGroupEntry::decode(&duplicate, &route, limits),
        Err(ProtocolError::Framing)
    );
    let mut sequence = bytes.clone();
    let at = 24 + 185 + 16 + 2 + "alpha".len();
    sequence[at..at + 8].copy_from_slice(&1_u64.to_be_bytes());
    rehash(&mut sequence);
    assert_eq!(
        CellGroupEntry::decode(&sequence, &route, limits),
        Err(ProtocolError::Chain)
    );
    let mut corrupt = bytes.clone();
    corrupt[24 + header_len] ^= 1;
    assert_eq!(
        CellGroupEntry::decode(&corrupt, &route, limits),
        Err(ProtocolError::Digest)
    );
}

#[test]
fn group_administration_roundtrips_and_golden() {
    use cellar::application::consensus::{DiskLineage, MemberId, NodeId, ProtocolError};
    let route = group_route();
    let limits = GroupEntryLimits::default();
    let transition = TransitionId::new([77; 16]).unwrap();
    let initial = image_fixture::image(None, "CREATE TABLE seed(n INTEGER);").unwrap();
    let cut = image_fixture::Cut::new(&[], &initial, 0, true, true);
    let (left, right) = CellRange::all()
        .split(cell_routing_key(
            route.history.namespace,
            &CellId::new("alpha").unwrap(),
        ))
        .unwrap();
    let children: Vec<_> = [left, right]
        .into_iter()
        .enumerate()
        .map(|(i, range)| {
            let mut child = route.clone();
            child.history.group = GroupId::new([40 + i as u8; 16]).unwrap();
            child.history.incarnation = IncarnationId::new([50 + i as u8; 16]).unwrap();
            child.birth_digest = [60 + i as u8; 32];
            child.range = range;
            child
        })
        .collect();
    let seal = GroupCommand::Seal {
        transition,
        authorization: [66; 32],
        children: children.clone(),
    };
    let membership = GroupCommand::Membership {
        previous: MembershipRevision::new(1).unwrap(),
        next: MembershipRevision::new(2).unwrap(),
        transition: GroupMembershipTransition::JointExplicit,
        changes: vec![GroupMemberChange {
            member: MemberId::new(1).unwrap(),
            node: NodeId::new([7; 16]).unwrap(),
            disk: DiskLineage::new([8; 16]).unwrap(),
            role: GroupMemberRole::Voter,
        }],
    };
    let commands = vec![
        GroupCommand::Enroll {
            cell: observation().version.history,
            image: image_value(cut.result),
            files: vec![GroupLtx {
                min_txid: 1,
                max_txid: 1,
                page_count: cut.pages.len() as u64,
                decoded_bytes: cut.pages.len() as u64 * 512,
                bytes: &cut.bytes,
            }],
        },
        membership.clone(),
        GroupCommand::Membership {
            previous: MembershipRevision::new(2).unwrap(),
            next: MembershipRevision::new(3).unwrap(),
            transition: GroupMembershipTransition::LeaveJoint,
            changes: vec![],
        },
        GroupCommand::Checkpoint {
            position: observation().witness.position,
            manifest: [25; 32],
            membership: MembershipRevision::new(1).unwrap(),
        },
        seal.clone(),
        GroupCommand::Install {
            transition,
            sources: children
                .iter()
                .map(|child| GroupSeal {
                    group: child.history,
                    position: observation().witness.position,
                    manifest: [26; 32],
                })
                .collect(),
            manifest: [27; 32],
        },
        GroupCommand::Activate {
            transition,
            publication: [88; 32],
            manifest: [27; 32],
        },
        GroupCommand::PreparePark {
            generation: ParkGeneration::new(1).unwrap(),
            term: Term::new(9),
            membership: MembershipRevision::new(1).unwrap(),
        },
        GroupCommand::Retire {
            transition,
            publication: [88; 32],
        },
    ];
    for command in commands {
        let entry = CellGroupEntry {
            binding: binding(&route),
            command,
        };
        let bytes = entry.encode(&route, limits).unwrap();
        assert_eq!(
            CellGroupEntry::decode(&bytes, &route, limits).unwrap(),
            entry
        );
        assert_eq!(&bytes[8..12], &1_u32.to_be_bytes());
        if matches!(entry.command, GroupCommand::Retire { .. }) {
            assert_eq!(bytes.len(), 289);
            assert_eq!(
                hex::encode(&bytes[bytes.len() - 32..]),
                "77aa30713c91abfafa483e9709b1680271a04873894bbd60bd9374114ed24000"
            );
        }
    }
    let mut repeated = membership;
    for transition in [
        GroupMembershipTransition::Auto,
        GroupMembershipTransition::JointImplicit,
        GroupMembershipTransition::JointExplicit,
    ] {
        for role in [
            GroupMemberRole::Voter,
            GroupMemberRole::Learner,
            GroupMemberRole::Remove,
        ] {
            let mut command = repeated.clone();
            if let GroupCommand::Membership {
                transition: mode,
                changes,
                ..
            } = &mut command
            {
                *mode = transition;
                changes[0].role = role;
            }
            let entry = CellGroupEntry {
                binding: binding(&route),
                command,
            };
            let bytes = entry.encode(&route, limits).unwrap();
            assert_eq!(
                CellGroupEntry::decode(&bytes, &route, limits).unwrap(),
                entry
            );
        }
    }
    let mut two_members = repeated.clone();
    if let GroupCommand::Membership { changes, .. } = &mut two_members {
        changes.push(GroupMemberChange {
            member: MemberId::new(2).unwrap(),
            node: NodeId::new([9; 16]).unwrap(),
            disk: DiskLineage::new([10; 16]).unwrap(),
            role: GroupMemberRole::Learner,
        });
    }
    let mut duplicated_member = CellGroupEntry {
        binding: binding(&route),
        command: two_members,
    }
    .encode(&route, limits)
    .unwrap();
    let second_member = 24 + 185 + 8 + 8 + 1 + 4 + 41;
    duplicated_member[second_member..second_member + 8].copy_from_slice(&1_u64.to_be_bytes());
    rehash(&mut duplicated_member);
    assert_eq!(
        CellGroupEntry::decode(&duplicated_member, &route, limits),
        Err(ProtocolError::Identity)
    );
    if let GroupCommand::Membership { changes, .. } = &mut repeated {
        changes.push(changes[0]);
    }
    assert_eq!(
        CellGroupEntry {
            binding: binding(&route),
            command: repeated
        }
        .encode(&route, limits),
        Err(ProtocolError::Identity)
    );
    let mut duplicate_child = seal.clone();
    if let GroupCommand::Seal { children, .. } = &mut duplicate_child {
        children[1].history.group = children[0].history.group;
    }
    assert!(CellGroupEntry {
        binding: binding(&route),
        command: duplicate_child
    }
    .encode(&route, limits)
    .is_err());
    let mut wrong_hash = seal.clone();
    if let GroupCommand::Seal { children, .. } = &mut wrong_hash {
        children[0].range = CellRange::new(
            CellBound::NegativeInfinity,
            CellBound::Key(CellKey::from_digest([0; 32], CellId::new("alpha").unwrap())),
        )
        .unwrap();
    }
    assert!(CellGroupEntry {
        binding: binding(&route),
        command: wrong_hash
    }
    .encode(&route, limits)
    .is_err());
    let mut bounded = limits;
    bounded.max_transition_groups = 1;
    let bytes = CellGroupEntry {
        binding: binding(&route),
        command: seal,
    }
    .encode(&route, limits)
    .unwrap();
    assert_eq!(
        CellGroupEntry::decode(&bytes, &route, bounded),
        Err(ProtocolError::Limit)
    );
    assert!(ParkGeneration::new(0).is_err());
    assert!(ParkGeneration::new(u64::MAX)
        .unwrap()
        .checked_next()
        .is_err());
    assert!(TransitionId::new([0; 16]).is_err());
}

#[test]
fn group_enrollment_contains_replayable_initial_image() {
    use cellar::application::consensus::{ImageCommitment, ProtocolError};
    use sha2::{Digest, Sha256};
    let initial = image_fixture::image(
        None,
        "CREATE TABLE seed(n INTEGER); INSERT INTO seed VALUES (17);",
    )
    .unwrap();
    let seed = image_fixture::Cut::new(&[], &initial, 0, true, true);
    let route = group_route();
    let limits = GroupEntryLimits::default();
    let entry = CellGroupEntry {
        binding: binding(&route),
        command: GroupCommand::Enroll {
            cell: observation().version.history,
            image: image_value(seed.result),
            files: vec![GroupLtx {
                min_txid: 1,
                max_txid: 1,
                page_count: seed.pages.len() as u64,
                decoded_bytes: seed.pages.len() as u64 * 512,
                bytes: &seed.bytes,
            }],
        },
    };
    let bytes = entry.encode(&route, limits).unwrap();
    let decoded = CellGroupEntry::decode(&bytes, &route, limits).unwrap();
    let GroupCommand::Enroll { image, files, .. } = decoded.command else {
        panic!("enrollment changed kind")
    };
    // A remote replica can reconstruct this initial SQLite image from the log
    // bytes alone, without the proposing host's disk or an object-store read.
    let recovered = cellar_ltx::ltx::decode_database_image(files[0].bytes).unwrap();
    assert_eq!(recovered, initial);
    assert_eq!(
        image.commitment,
        ImageCommitment::Sha256(Sha256::digest(&recovered).into())
    );
    let mut no_bytes = entry.clone();
    if let GroupCommand::Enroll { files, .. } = &mut no_bytes.command {
        files.clear();
    }
    assert_eq!(no_bytes.encode(&route, limits), Err(ProtocolError::Limit));
    let mut missing_prefix = entry;
    if let GroupCommand::Enroll { files, .. } = &mut missing_prefix.command {
        files[0].min_txid = 2;
    }
    assert_eq!(
        missing_prefix.encode(&route, limits),
        Err(ProtocolError::Chain)
    );
}
