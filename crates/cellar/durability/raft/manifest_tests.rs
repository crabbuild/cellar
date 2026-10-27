// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::member_state::GroupMembership;
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

#[test]
fn archive_configuration_set_order_is_canonical() -> Result<(), ArchiveError> {
    let mut first = ConfState {
        voters: vec![9, 1, 5],
        voters_outgoing: vec![3, 9, 1],
        learners: vec![11, 7],
        learners_next: vec![3],
        auto_leave: true,
        ..Default::default()
    };
    let encoded = encode_conf(&first)?;
    let raw = first
        .write_to_bytes()
        .map_err(|_| ArchiveError::Corrupt("test configuration"))?;
    assert!(decode_conf(&raw).is_err());
    first.voters.reverse();
    first.voters_outgoing.reverse();
    first.learners.reverse();
    first.learners_next.reverse();
    assert_eq!(encoded, encode_conf(&first)?);
    let canonical = decode_conf(&encoded)?;
    assert_eq!(canonical.voters, vec![1, 5, 9]);
    assert_eq!(canonical.voters_outgoing, vec![1, 3, 9]);
    assert_eq!(canonical.learners, vec![7, 11]);
    assert_eq!(canonical.learners_next, vec![3]);
    Ok(())
}

#[test]
fn archive_configuration_unknown_and_duplicate_members_refused() {
    let mut conf = ConfState {
        voters: vec![1, 1],
        ..Default::default()
    };
    assert!(encode_conf(&conf).is_err());
    conf.voters.pop();
    conf.mut_unknown_fields().add_varint(99, 1);
    assert!(encode_conf(&conf).is_err());
}

fn checkpoint() -> Manifest {
    let conf = ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    let position = Position { index: 10, term: 2 };
    let snapshot = Snapshot {
        position,
        image: Image {
            epoch: 1,
            txid: 1,
            checksum: 0,
            page_size: 4096,
            length: 4096,
            digest: [4; 32],
        },
        conf_state: encode_conf(&conf).unwrap(),
        membership: GroupMembership::initial(&conf),
        object: ObjectRef::new(
            ObjectKind::Snapshot,
            b"metadata-only fixture",
            Some(PositionRange {
                first: position,
                last: position,
            }),
        ),
    };
    Manifest {
        scope: (&GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: "Counter:manifest".into(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        })
            .into(),
        root_digest: [5; 32],
        birth_digest: [6; 32],
        schema_digest: [7; 32],
        position,
        image: snapshot.image,
        conf_state: snapshot.conf_state.clone(),
        membership: snapshot.membership.clone(),
        snapshot,
        tails: vec![],
        dependencies: vec![],
    }
}

fn validate(manifest: &Manifest) -> Result<(), ArchiveError> {
    manifest.validate(
        &manifest.scope.clone().decode().unwrap(),
        manifest.root_digest,
        manifest.birth_digest,
        manifest.schema_digest,
        ArchiveLimits::default(),
        &manifest.snapshot.membership,
    )
}

#[test]
fn marker_one_manifest_requires_explicit_final_configuration_and_membership() {
    let manifest = checkpoint();
    validate(&manifest).unwrap();
    for field in ["conf_state", "membership"] {
        let mut value = serde_json::to_value(&manifest).unwrap();
        value.as_object_mut().unwrap().remove(field);
        let bytes = encode(1, &value, 256 * 1024).unwrap();
        assert!(decode::<Manifest>(1, &bytes, 256 * 1024).is_err());
    }
}

#[test]
fn checkpoint_cannot_claim_a_configuration_or_member_revision_without_tails() {
    let mut manifest = checkpoint();
    let mut conf = decode_conf(&manifest.conf_state).unwrap();
    conf.voters.pop();
    manifest.conf_state = encode_conf(&conf).unwrap();
    // Each state is individually valid, but no archived transition connects
    // the claimed tip to the checkpoint.
    manifest
        .membership
        .validate(&conf, manifest.position.index)
        .unwrap();
    assert!(matches!(
        validate(&manifest),
        Err(ArchiveError::Corrupt("archive tip coverage"))
    ));

    let mut manifest = checkpoint();
    let mut membership = serde_json::to_value(&manifest.membership).unwrap();
    membership["revision"] = serde_json::json!(2);
    manifest.membership = serde_json::from_value(membership).unwrap();
    manifest
        .membership
        .validate(
            &decode_conf(&manifest.conf_state).unwrap(),
            manifest.position.index,
        )
        .unwrap();
    assert!(matches!(
        validate(&manifest),
        Err(ArchiveError::Corrupt("archive tip coverage"))
    ));
}
