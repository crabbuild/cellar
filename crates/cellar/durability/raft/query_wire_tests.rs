use super::*;
use crate::application::consensus::{CommittedVersion, VersionSigningKey};
use cellar_logic::consensus::*;

fn identity(member: u64) -> ReplicaIdentity {
    ReplicaIdentity {
        scope: GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: "Counter:one".into(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        member: MemberId::new(member).unwrap(),
        node: NodeId::new([member as u8; 16]).unwrap(),
        disk: DiskLineage::new([member as u8 + 3; 16]).unwrap(),
        process: ProcessGeneration::new([7; 16]).unwrap(),
    }
}

fn route() -> Route {
    Route::new(
        identity(1).node_identity(),
        "node-1",
        &identity(2),
        "node-2",
        [9; 32],
    )
    .unwrap()
}

fn keys() -> VersionKeys {
    VersionKeys::new(
        VersionSigningKey::new([13; 32], 10000).unwrap(),
        vec![],
        Duration::from_millis(1000),
    )
    .unwrap()
}

fn call(consistency: QueryConsistency) -> QueryCall {
    QueryCall {
        contract: OperationContract {
            operation: "Get".into(),
            kind: OperationKind::Query,
            digest: [11; 32],
        },
        input: vec![1, 2, 3],
        options: QueryOptions {
            consistency,
            timeout: Duration::from_millis(500),
            ..Default::default()
        },
    }
}

fn result() -> QueryResult {
    let version = CommittedVersion {
        scope: identity(1).scope,
        position: LogPosition {
            index: LogIndex::new(7),
            term: Term::new(2),
        },
    };
    QueryResult {
        version_token: keys().issue(&version, [9; 32], 1000).unwrap(),
        result: vec![8, 7, 6],
        version,
        served_by: MemberId::new(2).unwrap(),
    }
}

#[test]
fn query_wire_preserves_explicit_constraints() {
    for consistency in [
        QueryConsistency::Stale,
        QueryConsistency::AtLeast(result().version_token),
        QueryConsistency::Linearizable,
    ] {
        for stale_policy in [
            StaleReadPolicy::RecentSource,
            StaleReadPolicy::DisconnectedHistorical,
        ] {
            let mut call = call(consistency.clone());
            call.options.stale_policy = stale_policy;
            let bytes = call.encode(4096).unwrap();
            let decoded = QueryCall::decode(&bytes, 4096).unwrap();
            assert_eq!(decoded.contract, call.contract);
            assert_eq!(decoded.input, call.input);
            assert_eq!(decoded.options.consistency, consistency);
            assert_eq!(decoded.options.timeout, call.options.timeout);
            assert_eq!(decoded.options.stale_policy, stale_policy);
            let mut trailing = bytes;
            trailing.push(0);
            assert!(QueryCall::decode(&trailing, 4096).is_err());
        }
    }
}

#[test]
fn query_wire_binds_channel_request_route_and_payload() {
    let auth = PeerAuth::new([17; 32], "node-1").unwrap();
    let options = TransportOptions::default();
    let body = call(QueryConsistency::Linearizable).encode(4096).unwrap();
    let request = RequestHead::new(route(), 7, &body);
    let head = request_head(&auth, &[3; 32], &request, &body, &options).unwrap();
    assert!(read_request(&head, &auth, &[3; 32], &options).is_ok());
    assert!(read_request(&head, &auth, &[4; 32], &options).is_err());
    for index in 0..body.len() {
        let mut modified = body.clone();
        modified[index] ^= 1;
        assert!(request.verify_body(&modified).is_err());
    }
    let value = result();
    let reply = encode_result(&Ok(value.clone()), 4096).unwrap();
    let response = response_head(&auth, &[3; 32], &request, &head.tag, &reply, &options).unwrap();
    assert_eq!(
        read_response(&response, &auth, &[3; 32], &request, &head.tag, &options).unwrap(),
        <[u8; 32]>::from(Sha256::digest(&reply))
    );
    assert!(read_response(&response, &auth, &[4; 32], &request, &head.tag, &options).is_err());
    assert!(read_response(&response, &auth, &[3; 32], &request, &[5; 32], &options).is_err());
    let other_request = RequestHead::new(route(), 8, &body);
    assert!(read_response(
        &response,
        &auth,
        &[3; 32],
        &other_request,
        &head.tag,
        &options
    )
    .is_err());
    let other_route = Route::new(
        identity(1).node_identity(),
        "node-1",
        &identity(3),
        "node-3",
        [9; 32],
    )
    .unwrap();
    assert!(read_response(
        &response,
        &auth,
        &[3; 32],
        &RequestHead::new(other_route, 7, &body),
        &head.tag,
        &options
    )
    .is_err());
    assert_eq!(
        decode_result(
            &reply,
            &value.version.scope,
            [9; 32],
            &keys(),
            1000,
            4096,
            MemberId::new(2).unwrap()
        )
        .unwrap()
        .unwrap(),
        value
    );
    assert!(decode_result(
        &reply,
        &value.version.scope,
        [10; 32],
        &keys(),
        1000,
        4096,
        MemberId::new(2).unwrap()
    )
    .is_err());
    let mut wrong = value.version.scope.clone();
    wrong.incarnation = IncarnationId::new([99; 16]).unwrap();
    assert!(decode_result(
        &reply,
        &wrong,
        [9; 32],
        &keys(),
        1000,
        4096,
        MemberId::new(2).unwrap()
    )
    .is_err());
}

#[test]
fn query_origin_binds_node_process_without_voting_identity() {
    let origin = NodeIdentity {
        node: NodeId::new([91; 16]).unwrap(),
        disk: DiskLineage::new([92; 16]).unwrap(),
        process: ProcessGeneration::new([93; 16]).unwrap(),
    };
    let target = identity(2);
    let route = Route::new(origin, "ingress", &target, "node-2", [9; 32]).unwrap();
    assert_eq!(route.decode().unwrap(), (origin, target.clone()));
    assert!(route.reversed().decode().is_err());
    let encoded = serde_json::to_value(&route).unwrap();
    assert!(encoded["source"]["member"].is_null());
    assert_eq!(encoded["target"]["member"], "2");
    // Even an authenticated query origin cannot reuse its route as the
    // member-bearing route required for a consensus message.
    assert!(
        serde_json::from_value::<crate::raft::transport_frame::Route>(encoded.clone()).is_err()
    );
    for member in [
        serde_json::Value::Null,
        serde_json::json!("0"),
        serde_json::json!("02"),
        serde_json::json!("+2"),
    ] {
        let mut modified = encoded.clone();
        modified["target"]["member"] = member;
        assert!(serde_json::from_value::<Route>(modified)
            .unwrap()
            .decode()
            .is_err());
    }
    let mut voting_origin = encoded.clone();
    voting_origin["source"]["member"] = serde_json::json!("1");
    assert!(serde_json::from_value::<Route>(voting_origin)
        .unwrap()
        .decode()
        .is_err());
    for endpoint in ["source", "target"] {
        for field in ["node", "disk", "process"] {
            let mut modified = encoded.clone();
            modified[endpoint][field] = serde_json::to_value([0_u8; 16]).unwrap();
            assert!(serde_json::from_value::<Route>(modified)
                .unwrap()
                .decode()
                .is_err());
        }
    }
    let auth = PeerAuth::new([17; 32], "ingress").unwrap();
    let options = TransportOptions::default();
    let body = call(QueryConsistency::Linearizable).encode(4096).unwrap();
    let request = RequestHead::new(route, 7, &body);
    let head = request_head(&auth, &[3; 32], &request, &body, &options).unwrap();
    let reply = encode_result(&Ok(result()), 4096).unwrap();
    let response = response_head(&auth, &[3; 32], &request, &head.tag, &reply, &options).unwrap();
    let mut changed_origins = Vec::new();
    for field in 0..3 {
        let mut changed = origin;
        match field {
            0 => changed.node = NodeId::new([94; 16]).unwrap(),
            1 => changed.disk = DiskLineage::new([94; 16]).unwrap(),
            _ => changed.process = ProcessGeneration::new([94; 16]).unwrap(),
        }
        changed_origins.push((changed, "ingress"));
    }
    changed_origins.push((origin, "other-ingress"));
    for (changed, label) in changed_origins {
        let other = RequestHead::new(
            Route::new(changed, label, &target, "node-2", [9; 32]).unwrap(),
            7,
            &body,
        );
        assert!(read_response(&response, &auth, &[3; 32], &other, &head.tag, &options).is_err());
    }
}

#[test]
fn query_wire_refuses_size_replay_and_consensus_domain_confusion() {
    let auth = PeerAuth::new([17; 32], "node-1").unwrap();
    let options = TransportOptions::default();
    let body = call(QueryConsistency::Stale).encode(4096).unwrap();
    assert!(request_head(
        &auth,
        &[3; 32],
        &RequestHead::new(route(), 0, &body),
        &body,
        &options
    )
    .is_err());
    let head = request_head(
        &auth,
        &[3; 32],
        &RequestHead::new(route(), 1, &body),
        &body,
        &options,
    )
    .unwrap();
    assert!(crate::raft::transport_frame::parse_prefix(
        &head.prefix,
        &options,
        crate::raft::transport::Lane::Data,
        false
    )
    .is_err());
    assert!(read_request(
        &head,
        &PeerAuth::new([18; 32], "node-1").unwrap(),
        &[3; 32],
        &options
    )
    .is_err());
    let mut prefix = head.prefix;
    prefix[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(parse_prefix(&prefix, &options, false).is_err());
    prefix = head.prefix;
    prefix[12..].copy_from_slice(&u64::MAX.to_be_bytes());
    assert!(parse_prefix(&prefix, &options, false).is_err());
    let mut call = call(QueryConsistency::Stale);
    call.options.timeout = Duration::ZERO;
    assert!(call.encode(4096).is_err());
    call.options.timeout = Duration::from_millis(500);
    call.contract.kind = OperationKind::Command;
    assert!(call.encode(4096).is_err());
    call.contract.kind = OperationKind::Query;
    call.input = vec![0; 4096];
    assert!(call.encode(4096).is_err());
}

#[test]
fn query_wire_failures_remain_typed() {
    for failure in [
        CommandFailure::VersionUnavailable,
        CommandFailure::WrongIncarnation,
        CommandFailure::UnsupportedCapability,
        CommandFailure::UnsupportedConsistency,
        CommandFailure::Overloaded,
        CommandFailure::CorruptState,
        CommandFailure::Unavailable,
    ] {
        let bytes = encode_result(&Err(failure.clone()), 4096).unwrap();
        assert_eq!(
            decode_result(
                &bytes,
                &identity(1).scope,
                [9; 32],
                &keys(),
                1000,
                4096,
                MemberId::new(2).unwrap()
            )
            .unwrap(),
            Err(failure)
        );
        assert!(decode_result(
            &bytes,
            &identity(1).scope,
            [9; 32],
            &keys(),
            1000,
            bytes.len() - 1,
            MemberId::new(2).unwrap()
        )
        .is_err());
    }
}
