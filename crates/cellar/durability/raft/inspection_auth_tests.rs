use super::*;
use cellar_logic::consensus::{
    DiskLineage, EngineInstanceId, GroupId, IncarnationId, ProcessGeneration,
};

fn request() -> RequestHead {
    let physical = |id: u8| PhysicalNode {
        identity: NodeIdentity {
            node: NodeId::new([id; 16]).unwrap(),
            disk: DiskLineage::new([id; 16]).unwrap(),
            process: ProcessGeneration::new([id; 16]).unwrap(),
        },
        storage_instance: EngineInstanceId::new([id; 16]).unwrap(),
    };
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:1".into(),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    };
    RequestHead {
        route: InspectionRoute::new(
            &scope,
            [5; 32],
            [6; 32],
            &physical(1),
            "node-1",
            &physical(2),
            "node-2",
        )
        .unwrap(),
        nonce: "12".into(),
        timeout_ms: 1000,
    }
}

#[test]
fn inspection_mac_rejects_other_purposes_channels_and_request_replay() {
    let auth = PeerAuth::new([8; 32], "node-1").unwrap();
    let options = TransportOptions::default();
    let exporter = [7; 32];
    let wire = Wire {
        auth: &auth,
        exporter: &exporter,
        options: &options,
        max_body: body::MAX_BODY,
    };
    let request = request();
    let head = wire
        .encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &[])
        .unwrap();
    assert!(wire
        .decode::<RequestHead>(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None)
        .is_ok());
    for purpose in [
        b"cellar-query-request-1".as_slice(),
        RESPONSE_PURPOSE,
        b"cellar-snapshot-pull-1",
    ] {
        assert!(matches!(
            wire.decode::<RequestHead>(REQUEST_MAGIC, purpose, &head, None),
            Err(TransportError::Authentication)
        ));
    }
    let wrong_channel = Wire {
        exporter: &[9; 32],
        ..wire
    };
    assert!(matches!(
        wrong_channel.decode::<RequestHead>(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None),
        Err(TransportError::Authentication)
    ));
    let reply = ReplyHead {
        route: request.route,
        nonce: request.nonce,
        status: 2,
        digest: Sha256::digest([]).into(),
    };
    let response = wire
        .encode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &head.tag, &reply, &[])
        .unwrap();
    assert!(wire
        .decode::<ReplyHead>(RESPONSE_MAGIC, RESPONSE_PURPOSE, &response, Some(&head.tag))
        .is_ok());
    assert!(matches!(
        wire.decode::<ReplyHead>(RESPONSE_MAGIC, RESPONSE_PURPOSE, &response, Some(&[0; 32])),
        Err(TransportError::Authentication)
    ));
    let mut tampered = response;
    tampered.header[0] ^= 1;
    assert!(matches!(
        wire.decode::<ReplyHead>(RESPONSE_MAGIC, RESPONSE_PURPOSE, &tampered, Some(&head.tag)),
        Err(TransportError::Authentication)
    ));
}

#[test]
fn inspection_physical_route_rejects_unscoped_and_zero_identities() {
    let route = request().route;
    let original = serde_json::to_value(&route).unwrap();
    for path in [
        ["source", "node"],
        ["source", "disk"],
        ["source", "process"],
        ["source", "storage_instance"],
        ["target", "node"],
        ["target", "disk"],
        ["target", "process"],
        ["target", "storage_instance"],
    ] {
        let mut value = original.clone();
        value[path[0]][path[1]] = serde_json::json!(vec![0; 16]);
        let changed: InspectionRoute = serde_json::from_value(value).unwrap();
        assert!(changed.decode().is_err());
    }
    for field in ["schema", "birth", "protocol"] {
        let mut value = original.clone();
        value[field] = serde_json::json!(vec![0; 32]);
        let changed: InspectionRoute = serde_json::from_value(value).unwrap();
        assert!(changed.decode().is_err());
    }
    let mut value = original;
    value["target"]["node"] = value["source"]["node"].clone();
    let changed: InspectionRoute = serde_json::from_value(value).unwrap();
    assert!(changed.decode().is_err());
}

#[test]
fn only_a_direct_failed_dial_is_unreachable() {
    for stage in [0, 2] {
        for failure in [
            TransportError::Io("injected".into()),
            TransportError::Timeout,
            TransportError::Protocol,
        ] {
            assert_eq!(classify(failure, stage), InspectionError::Unavailable);
        }
    }
    for failure in [
        TransportError::Io("injected".into()),
        TransportError::Timeout,
    ] {
        assert_eq!(classify(failure, 1), InspectionError::Unreachable);
    }
    assert_eq!(
        classify(TransportError::Overloaded, 1),
        InspectionError::Overloaded
    );
    assert_eq!(
        classify(TransportError::Refused, 1),
        InspectionError::Refused
    );
}

#[test]
fn authenticated_body_cannot_cross_history_or_physical_owner() {
    use crate::raft::inspection::*;
    let route = request().route;
    let (scope, _, target) = route.decode().unwrap();
    let snapshot = InspectionSnapshot {
        view: CellInspection {
            scope,
            schema_digest: route.schema(),
            birth_digest: route.birth(),
            node: NodeInspection {
                identity: target.identity,
                storage_instance: target.storage_instance,
                profile: RaftNodeProfile {
                    pool: "native".into(),
                    region: "local".into(),
                    zone: "a".into(),
                    group_capacity: 1,
                    byte_capacity: 1024,
                    bytes_per_group: 1024,
                    leader_capacity: 1,
                    draining: false,
                },
                groups: 0,
                reserved_bytes: 0,
            },
            occupancy: ReplicaOccupancy::Standby,
            member: None,
            leader_load: None,
        },
        membership: None,
        configuration: None,
    };
    validate_response(&route, &snapshot).unwrap();
    for field in 0..11 {
        let mut changed = snapshot.clone();
        match field {
            0 => changed.view.scope.cluster = ClusterId::new([99; 16]).unwrap(),
            1 => changed.view.scope.namespace = NamespaceId::new([99; 16]).unwrap(),
            2 => changed.view.scope.cell = "Counter:other".into(),
            3 => changed.view.scope.group = GroupId::new([99; 16]).unwrap(),
            4 => changed.view.scope.incarnation = IncarnationId::new([99; 16]).unwrap(),
            5 => changed.view.schema_digest = [99; 32],
            6 => changed.view.birth_digest = [99; 32],
            7 => changed.view.node.identity.node = NodeId::new([99; 16]).unwrap(),
            8 => changed.view.node.identity.disk = DiskLineage::new([99; 16]).unwrap(),
            9 => changed.view.node.identity.process = ProcessGeneration::new([99; 16]).unwrap(),
            _ => changed.view.node.storage_instance = EngineInstanceId::new([99; 16]).unwrap(),
        }
        let received = body::decode(&body::encode(&changed).unwrap()).unwrap();
        assert_eq!(
            validate_response(&route, &received),
            Err(TransportError::Identity)
        );
    }
}
