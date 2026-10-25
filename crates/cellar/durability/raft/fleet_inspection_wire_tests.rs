use super::*;
use crate::raft::fleet_catalog::FleetNodeState;
use crate::raft::host::RaftHostEnrollmentObservation;
use crate::raft::inspection::*;
use crate::raft::member_state::GroupMembership;
use cellar_logic::consensus::*;

fn snapshot() -> InspectionSnapshot {
    let conf = ::raft::prelude::ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    };
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:inspection".into(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    };
    let binding =
        FleetGroupBinding::new(IncarnationId::new([80; 16]).unwrap(), &scope, [81; 32]).unwrap();
    let membership = GroupMembership::managed_initial(&conf, binding).unwrap();
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
        publication: Some(crate::raft::host::Publication::Image(
            crate::raft::host::FleetPublishedImage::new(
                LogPosition {
                    index: LogIndex::new(1),
                    term: Term::new(1),
                },
                cellar_ltx::apply::ImageState {
                    epoch: 1,
                    txid: cellar_ltx::TXID(1),
                    checksum: 0,
                    page_size: 4096,
                    length: 4096,
                    digest: [7; 32],
                },
            )
            .unwrap(),
        )),
    }
}

fn observation() -> LocalObservation {
    let snapshot = Box::new(snapshot());
    let enrollment = RaftHostEnrollmentObservation {
        fleet: [80; 16],
        revision: 3,
        epoch: 2,
        physical_host: IncarnationId::new([90; 16]).unwrap(),
        state: FleetNodeState::Draining,
        capability_digest: [9; 32],
    };
    let node = Box::new(RaftHostObservation {
        fleet_boot: Some(super::super::fleet_stop::BootBinding {
            catalog: [81; 32],
            fleet: [80; 16],
            node: snapshot.view.node.identity.node.bytes(),
            disk: snapshot.view.node.identity.disk.bytes(),
            process: snapshot.view.node.identity.process.bytes(),
            engine: snapshot.view.node.storage_instance.bytes(),
            boot: [41; 16],
            epoch: 1,
            join_key: crate::peer_probe::PeerProbeSigner::from_secret([4; 32]).public_key_bytes(),
        }),
        startup_bounds: crate::raft::host::RaftHostStartupBounds {
            host_groups: 4096,
            consensus_groups: 4096,
            engine_groups: 4096,
            bounded_buffer_bytes: 1024,
        },
        node: snapshot.view.node.clone(),
        startup_enrollment: Some(enrollment.clone()),
        current_enrollment: Some(enrollment),
        fleet_trust_revision: Some(3),
        declared_namespaces: 1,
        registered_histories: 1,
        dormant_histories: 0,
        pending_history_registrations: 0,
        unattached_engine_histories: 0,
    });
    LocalObservation {
        node,
        snapshot,
        barrier: LogIndex::new(2),
    }
}

#[test]
fn fleet_quorum_wire_retains_exact_configuration_and_publication() {
    let view = observation();
    let restored = decode(&encode(&view).unwrap()).unwrap();
    assert_eq!(restored.node, view.node);
    assert_eq!(restored.barrier, view.barrier);
    assert_eq!(restored.snapshot.view, view.snapshot.view);
    assert_eq!(restored.snapshot.membership, view.snapshot.membership);
    assert_eq!(restored.snapshot.configuration, view.snapshot.configuration);
    assert_eq!(restored.snapshot.publication, view.snapshot.publication);
    assert!(
        restored
            .snapshot
            .view
            .member
            .unwrap()
            .published
            .unwrap()
            .index
            < restored.barrier
    );
}

#[test]
fn fleet_cut_wire_requires_explicit_coherent_publication() {
    use crate::raft::host::Publication;
    let mut view = observation();
    let valid = encode(&view).unwrap();
    let ordinary = crate::raft::inspection_wire::decode(
        &crate::raft::inspection_wire::encode(&view.snapshot).unwrap(),
    )
    .unwrap();
    assert!(ordinary.publication.is_none());
    let node = u32::from_be_bytes(valid[4..8].try_into().unwrap()) as usize;
    let group = u32::from_be_bytes(valid[8..12].try_into().unwrap()) as usize;
    let start = 20 + node + group;
    assert!(decode(&valid[..start]).is_err());
    for tag in [3, 255] {
        let mut malformed = valid.clone();
        malformed[start] = tag;
        assert!(decode(&malformed).is_err());
    }
    for offset in [1, 9, 17, 25, 45] {
        let mut malformed = valid.clone();
        malformed[start + offset..start + offset + 8].fill(0);
        assert!(decode(&malformed).is_err());
    }
    let mut contradictory = valid[..start].to_vec();
    contradictory.push(0);
    assert!(decode(&contradictory).is_err());
    view.snapshot.publication = Some(Publication::Pending);
    assert_eq!(
        decode(&encode(&view).unwrap())
            .unwrap()
            .snapshot
            .publication,
        Some(Publication::Pending)
    );
    view.snapshot.view.member.as_mut().unwrap().published = None;
    view.snapshot.publication = Some(Publication::Empty);
    assert_eq!(
        decode(&encode(&view).unwrap())
            .unwrap()
            .snapshot
            .publication,
        Some(Publication::Empty)
    );
    view.snapshot.publication = None;
    assert!(encode(&view).is_err());
}

#[test]
fn fleet_quorum_wire_refuses_bounds_trailing_bytes_and_impossible_barriers() {
    let valid = encode(&observation()).unwrap();
    for end in 0..valid.len() {
        assert!(decode(&valid[..end]).is_err());
    }
    assert!(decode(&vec![0; MAX_BODY + 1]).is_err());
    let mut trailing = valid.clone();
    trailing.push(0);
    assert!(decode(&trailing).is_err());
    for offset in [0, 4, 8] {
        let mut changed = valid.clone();
        changed[offset..offset + 4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode(&changed).is_err());
    }
    for barrier in [0_u64, 3, u64::MAX] {
        let mut changed = valid.clone();
        changed[12..20].copy_from_slice(&barrier.to_be_bytes());
        assert!(decode(&changed).is_err());
    }
    let mut wrong_process = observation();
    wrong_process.node.node.identity.process = ProcessGeneration::new([99; 16]).unwrap();
    assert!(encode(&wrong_process).is_err());
    let mut wrong_engine = observation();
    wrong_engine.node.node.storage_instance = EngineInstanceId::new([99; 16]).unwrap();
    assert!(encode(&wrong_engine).is_err());
    let mut bare = observation();
    bare.snapshot.membership = Some(GroupMembership::initial(
        bare.snapshot.configuration.as_ref().unwrap(),
    ));
    assert!(encode(&bare).is_err());
}
