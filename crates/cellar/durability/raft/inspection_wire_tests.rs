use super::*;
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};

fn snapshot() -> InspectionSnapshot {
    let conf = ::raft::prelude::ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    let membership = GroupMembership::initial(&conf);
    InspectionSnapshot {
        view: CellInspection {
            scope: cellar_logic::consensus::GroupScope {
                cluster: ClusterId::new([1; 16]).unwrap(),
                namespace: NamespaceId::new([2; 16]).unwrap(),
                cell: "Counter:inspection".into(),
                group: GroupId::new([3; 16]).unwrap(),
                incarnation: IncarnationId::new([4; 16]).unwrap(),
            },
            schema_digest: [5; 32],
            birth_digest: [6; 32],
            node: NodeInspection {
                identity: NodeIdentity {
                    node: NodeId::new([7; 16]).unwrap(),
                    disk: DiskLineage::new([8; 16]).unwrap(),
                    process: ProcessGeneration::new([9; 16]).unwrap(),
                },
                storage_instance: EngineInstanceId::new([10; 16]).unwrap(),
                profile: RaftNodeProfile {
                    pool: "native".into(),
                    region: "local".into(),
                    zone: "a".into(),
                    group_capacity: 8,
                    byte_capacity: 8192,
                    bytes_per_group: 1024,
                    leader_capacity: 8,
                    draining: false,
                },
                groups: 1,
                reserved_bytes: 1024,
            },
            occupancy: ReplicaOccupancy::Member(MemberId::new(1).unwrap()),
            member: Some(MemberInspection {
                member: MemberId::new(1).unwrap(),
                term: Term::new(2),
                leader: Some(MemberId::new(1).unwrap()),
                is_leader: true,
                membership_revision: LogIndex::new(1),
                member_high_water: 3,
                committed: LogIndex::new(3),
                applied: LogPosition {
                    index: LogIndex::new(2),
                    term: Term::new(1),
                },
                published: Some(LogPosition {
                    index: LogIndex::new(1),
                    term: Term::new(1),
                }),
                repair: None,
                sample_age_ms: 17,
            }),
            leader_load: None,
        },
        membership: Some(membership),
        configuration: Some(conf),
    }
}

#[test]
fn inspection_round_trip_preserves_durable_and_visible_cuts() {
    let snapshot = snapshot();
    let restored = decode(&encode(&snapshot).unwrap()).unwrap();
    assert_eq!(restored.view, snapshot.view);
    assert_eq!(restored.membership, snapshot.membership);
    assert_eq!(restored.configuration, snapshot.configuration);
    for role in [
        ReplicaOccupancy::Standby,
        ReplicaOccupancy::Initializing,
        ReplicaOccupancy::Retired(MemberId::new(1).unwrap()),
        ReplicaOccupancy::Occupied(MemberId::new(1).unwrap()),
        ReplicaOccupancy::Failed,
    ] {
        let mut empty = snapshot.clone();
        empty.view.occupancy = role;
        empty.view.member = None;
        empty.membership = None;
        empty.configuration = None;
        assert_eq!(decode(&encode(&empty).unwrap()).unwrap().view, empty.view);
    }
}

#[test]
fn inspection_refuses_missing_nulls_and_conflicting_identity_or_progress() {
    let valid = encode(&snapshot()).unwrap();
    let original: serde_json::Value = serde_json::from_slice(&valid).unwrap();
    for field in [
        "member",
        "observation",
        "membership",
        "configuration",
        "leader_load",
    ] {
        let mut value = original.clone();
        value.as_object_mut().unwrap().remove(field);
        assert!(
            decode(&serde_json::to_vec(&value).unwrap()).is_err(),
            "missing {field}"
        );
    }
    for (path, value) in [
        (vec!["format"], serde_json::json!(2)),
        (vec!["member"], serde_json::json!("0")),
        (vec!["member"], serde_json::json!("2")),
        (vec!["observation", "leader"], serde_json::json!("9")),
        (
            vec!["observation", "published", "term"],
            serde_json::json!("2"),
        ),
        (vec!["node"], serde_json::json!(vec![0; 16])),
        (vec!["observation", "committed"], serde_json::json!("1")),
        (vec!["observation", "revision"], serde_json::json!("2")),
        (
            vec!["observation", "published", "index"],
            serde_json::json!("4"),
        ),
        (
            vec!["observation", "applied", "term"],
            serde_json::json!("0"),
        ),
        (vec!["observation", "high_water"], serde_json::json!("03")),
        (
            vec!["configuration", "voters"],
            serde_json::json!([1, 1, 3]),
        ),
        (
            vec!["configuration", "voters"],
            serde_json::json!(vec![1; 33]),
        ),
        (
            vec!["profile", "bytes_per_group"],
            serde_json::json!(u64::MAX),
        ),
    ] {
        let mut changed = original.clone();
        let mut target = &mut changed;
        for key in path {
            target = &mut target[key];
        }
        *target = value;
        assert!(decode(&serde_json::to_vec(&changed).unwrap()).is_err());
    }
    let duplicate = String::from_utf8(valid.clone()).unwrap().replacen(
        "\"format\":1",
        "\"format\":1,\"format\":1",
        1,
    );
    assert!(decode(duplicate.as_bytes()).is_err());
    let mut trailing = valid;
    trailing.extend_from_slice(b"{}");
    assert!(decode(&trailing).is_err());
    assert!(decode(&vec![0; MAX_BODY + 1]).is_err());
}
