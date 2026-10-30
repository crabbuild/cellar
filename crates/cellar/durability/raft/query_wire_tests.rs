// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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
        CommandFailure::StorageFull,
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

fn cell_observation() -> crate::raft::version::CellVersionObservation {
    use cellar_logic::cell_group::*;
    crate::raft::version::CellVersionObservation {
        version: CellVersion {
            history: CellHistory::try_from(&identity(1).scope).unwrap(),
            sequence: MutationSequence::new(7).unwrap(),
            image: CellImage {
                epoch: 1,
                txid: 10,
                checksum: 17,
                commitment: CellImageCommitment::Materialized([8; 32]),
            },
        },
        witness: CommitWitness {
            group: GroupHistory::from(&identity(1).scope),
            position: LogPosition {
                index: LogIndex::new(100),
                term: Term::new(9),
            },
            proposal: ProposalId::new([6; 16]).unwrap(),
        },
    }
}

fn cell_call(consistency: crate::raft::cell_groups::CellQueryConsistency) -> cell::CellQueryCall {
    cell::CellQueryCall {
        request: crate::raft::cell_groups::CellQueryRequest {
            invocation: ProposalId::new([91; 16]).unwrap(),
            contract: call(QueryConsistency::Stale).contract,
            input: vec![1, 2, 3],
            consistency,
        },
        timeout_ms: 700,
        route_epoch: 3,
        birth_digest: [92; 32],
    }
}

#[test]
fn shared_query_wire_preserves_cell_constraints_and_refuses_other_payloads() {
    use crate::raft::cell_groups::CellQueryConsistency;
    let token = keys()
        .issue_cell(&cell_observation(), [9; 32], 1000)
        .unwrap();
    for consistency in [
        CellQueryConsistency::Stale,
        CellQueryConsistency::Linearizable,
        CellQueryConsistency::AtLeast(token),
    ] {
        let call = cell_call(consistency);
        let bytes = call.encode(4096).unwrap();
        let decoded = cell::CellQueryCall::decode(&bytes, 4096).unwrap();
        assert_eq!(decoded.encode(4096).unwrap(), bytes);
        assert_eq!(decoded.request.invocation, call.request.invocation);
        assert_eq!(decoded.request.contract, call.request.contract);
        assert_eq!(decoded.request.input, call.request.input);
        assert_eq!(decoded.timeout_ms, 700);
        assert_eq!(decoded.route_epoch, 3);
        assert_eq!(decoded.birth_digest, [92; 32]);
        for end in 0..bytes.len() {
            assert!(cell::CellQueryCall::decode(&bytes[..end], 4096).is_err());
        }
        assert!(QueryCall::decode(&bytes, 4096).is_err());
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(cell::CellQueryCall::decode(&trailing, 4096).is_err());
        assert!(cell::CellQueryCall::decode(&bytes, bytes.len() - 1).is_err());
    }
    assert!(cell::CellQueryCall::decode(
        &call(QueryConsistency::Stale).encode(4096).unwrap(),
        4096
    )
    .is_err());
    let mut call = cell_call(CellQueryConsistency::Stale);
    call.request.contract.kind = OperationKind::Command;
    assert!(call.encode(4096).is_err());
    call.request.contract.kind = OperationKind::Query;
    call.timeout_ms = 0;
    assert!(call.encode(4096).is_err());
    call.timeout_ms = 700;
    call.route_epoch = 0;
    assert!(call.encode(4096).is_err());
    call.route_epoch = 3;
    call.birth_digest = [0; 32];
    assert!(call.encode(4096).is_err());
}

#[test]
fn shared_query_route_binds_physical_group_logical_cell_and_process() {
    use crate::raft::cell_groups::CellQueryConsistency;
    use cellar_logic::cell_group::{CellId, GroupHistory};
    let old = identity(2);
    let target = ReplicaIdentity {
        scope: GroupHistory::from(&old.scope),
        node: old.node,
        disk: old.disk,
        process: old.process,
        member: old.member,
    };
    let cell = cell_observation().version.history;
    let source = identity(1).node_identity();
    let route = Route::new_shared(source, "node-1", &target, "node-2", &cell, [9; 32]).unwrap();
    assert_eq!(
        route.decode_shared().unwrap(),
        (source, target.clone(), cell.clone())
    );
    assert!(route.decode().is_err());
    assert!(route.reversed().decode_shared().is_err());
    assert!(Route::new(source, "node-1", &old, "node-2", [9; 32])
        .unwrap()
        .decode_shared()
        .is_err());
    let value = serde_json::to_value(&route).unwrap();
    assert!(value["source"]["member"].is_null());
    assert!(serde_json::from_value::<crate::raft::transport_frame::Route>(value.clone()).is_err());
    for path in [
        ["source", "node"],
        ["source", "disk"],
        ["source", "process"],
        ["target", "node"],
        ["target", "disk"],
        ["target", "process"],
    ] {
        let mut changed = value.clone();
        changed[path[0]][path[1]] = serde_json::to_value([0u8; 16]).unwrap();
        assert!(serde_json::from_value::<Route>(changed)
            .unwrap()
            .decode_shared()
            .is_err());
    }
    let mut changed = value.clone();
    changed["source"]["member"] = serde_json::json!("1");
    assert!(serde_json::from_value::<Route>(changed)
        .unwrap()
        .decode_shared()
        .is_err());
    let mut changed = value.clone();
    changed["scope"]["identity"]["cell"] = serde_json::json!("bad\ncell");
    assert!(serde_json::from_value::<Route>(changed)
        .unwrap()
        .decode_shared()
        .is_err());
    let auth = PeerAuth::new([17; 32], "node-1").unwrap();
    let options = TransportOptions::default();
    let body = cell_call(CellQueryConsistency::Linearizable)
        .encode(4096)
        .unwrap();
    let request = RequestHead::new(route.clone(), 17, &body);
    let head = request_head(&auth, &[3; 32], &request, &body, &options).unwrap();
    assert!(read_request(&head, &auth, &[3; 32], &options).is_ok());
    assert!(read_request(&head, &auth, &[4; 32], &options).is_err());
    let reply = cell::encode_cell_result(&Err(CommandFailure::Overloaded), 4096).unwrap();
    let response = response_head(&auth, &[3; 32], &request, &head.tag, &reply, &options).unwrap();
    let mut other_cell = cell.clone();
    other_cell.cell = CellId::new("neighbor").unwrap();
    let mut other_group = target.clone();
    other_group.scope.group = GroupId::new([99; 16]).unwrap();
    let mut other_process = target.clone();
    other_process.process = ProcessGeneration::new([99; 16]).unwrap();
    for (target, cell) in [
        (&target, &other_cell),
        (&other_group, &cell),
        (&other_process, &cell),
    ] {
        let wrong = RequestHead::new(
            Route::new_shared(source, "node-1", target, "node-2", cell, [9; 32]).unwrap(),
            17,
            &body,
        );
        assert!(read_response(&response, &auth, &[3; 32], &wrong, &head.tag, &options).is_err());
    }
    for i in 0..body.len() {
        let mut altered = body.clone();
        altered[i] ^= 1;
        assert!(request.verify_body(&altered).is_err());
    }
    let mut wrong_namespace = cell;
    wrong_namespace.namespace = NamespaceId::new([99; 16]).unwrap();
    assert!(Route::new_shared(
        source,
        "node-1",
        &target,
        "node-2",
        &wrong_namespace,
        [9; 32]
    )
    .is_err());
}

#[test]
fn shared_query_result_authenticates_cell_progress_not_group_log_index() {
    let observation = cell_observation();
    let token = keys().issue_cell(&observation, [9; 32], 1000).unwrap();
    let mut bytes = b"CLGCQS01\0".to_vec();
    string(&mut bytes, token.as_str()).unwrap();
    bytes.extend_from_slice(&3u32.to_be_bytes());
    bytes.extend_from_slice(b"yes");
    let member = MemberId::new(2).unwrap();
    let decoded = cell::decode_cell_result(
        &bytes,
        &observation.version.history,
        [9; 32],
        &keys(),
        1000,
        4096,
        member,
    )
    .unwrap()
    .unwrap();
    assert_eq!(decoded.observation, observation);
    assert_eq!(decoded.observation.version.sequence.get(), 7);
    assert_eq!(decoded.observation.witness.position.index.get(), 100);
    assert_eq!(decoded.result, b"yes");
    assert_eq!(decoded.served_by, member);
    let mut wrong = observation.version.history.clone();
    wrong.incarnation = IncarnationId::new([99; 16]).unwrap();
    assert!(
        cell::decode_cell_result(&bytes, &wrong, [9; 32], &keys(), 1000, 4096, member).is_err()
    );
    assert!(cell::decode_cell_result(
        &bytes,
        &observation.version.history,
        [8; 32],
        &keys(),
        1000,
        4096,
        member
    )
    .is_err());
    assert!(cell::decode_cell_result(
        &bytes,
        &observation.version.history,
        [9; 32],
        &keys(),
        3000,
        4096,
        member
    )
    .is_err());
    assert!(decode_result(
        &bytes,
        &identity(1).scope,
        [9; 32],
        &keys(),
        1000,
        4096,
        member
    )
    .is_err());
    for end in 0..bytes.len() {
        assert!(cell::decode_cell_result(
            &bytes[..end],
            &observation.version.history,
            [9; 32],
            &keys(),
            1000,
            4096,
            member
        )
        .is_err());
    }
    for error in [
        CommandFailure::WrongIncarnation,
        CommandFailure::Overloaded,
        CommandFailure::VersionUnavailable,
        CommandFailure::Unavailable,
        CommandFailure::StorageFull,
        CommandFailure::CorruptState,
        CommandFailure::UnsupportedCapability,
        CommandFailure::UnsupportedConsistency,
    ] {
        let bytes = cell::encode_cell_result(&Err(error.clone()), 4096).unwrap();
        assert!(
            matches!(cell::decode_cell_result(&bytes, &observation.version.history, [9; 32], &keys(), 1000, 4096, member).unwrap(), Err(actual) if actual == error)
        );
    }
}

fn command_call() -> command::CellCommandCall {
    command::CellCommandCall {
        request: crate::raft::cell_groups::CellCommandRequest {
            id: CommandId::new(ClientId::new([71; 16]).unwrap(), 3).unwrap(),
            contract: OperationContract {
                operation: "Add".into(),
                digest: [72; 32],
                kind: OperationKind::Command,
            },
            input: vec![1, 2, 3],
            durability: Durability::Quorum,
            retry_until_ms: 9000,
        },
        inputs: crate::storage::CommandInputs {
            now_unix_millis: 1000,
            random: [73; 32],
        },
        timeout_ms: 1000,
        route_epoch: 3,
        birth_digest: [74; 32],
    }
}

#[test]
fn shared_command_wire_preserves_inputs_and_refuses_query_payloads() {
    let call = command_call();
    let bytes = call.encode(4096).unwrap();
    let decoded = command::CellCommandCall::decode(&bytes, 4096).unwrap();
    assert_eq!(decoded.encode(4096).unwrap(), bytes);
    assert_eq!(decoded.inputs.random, call.inputs.random);
    assert_eq!(decoded.inputs.now_unix_millis, call.inputs.now_unix_millis);
    assert_eq!(decoded.request.id, call.request.id);
    assert!(cell::CellQueryCall::decode(&bytes, 4096).is_err());
    assert!(command::CellCommandCall::decode(
        &cell_call(crate::raft::cell_groups::CellQueryConsistency::Stale)
            .encode(4096)
            .unwrap(),
        4096
    )
    .is_err());
    for end in 0..bytes.len() {
        assert!(command::CellCommandCall::decode(&bytes[..end], 4096).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(command::CellCommandCall::decode(&trailing, 4096).is_err());
    assert!(command::CellCommandCall::decode(&bytes, bytes.len() - 1).is_err());
    let mut call = call;
    call.request.durability = Durability::S3;
    assert!(call.encode(4096).is_err());
}

#[test]
fn shared_command_purpose_is_authenticated_and_cannot_reuse_read_reply() {
    use cellar_logic::cell_group::GroupHistory;
    let old = identity(2);
    let target = ReplicaIdentity {
        scope: GroupHistory::from(&old.scope),
        member: old.member,
        node: old.node,
        disk: old.disk,
        process: old.process,
    };
    let cell = cell_observation().version.history;
    let read = Route::new_shared(
        identity(1).node_identity(),
        "node-1",
        &target,
        "node-2",
        &cell,
        [9; 32],
    )
    .unwrap();
    let write = read.clone().for_command().unwrap();
    assert!(write.is_command());
    assert!(!read.is_command());
    let auth = PeerAuth::new([17; 32], "node-1").unwrap();
    let options = TransportOptions::default();
    let body = command_call().encode(4096).unwrap();
    let request = RequestHead::new(write.clone(), 3, &body);
    let head = request_head(&auth, &[3; 32], &request, &body, &options).unwrap();
    let mut value = serde_json::to_value(&request).unwrap();
    value["route"]["command"] = serde_json::json!(false);
    let changed = FrameHead {
        prefix: head.prefix,
        header: serde_json::to_vec(&value).unwrap(),
        tag: head.tag,
        payload_len: head.payload_len,
    };
    assert!(read_request(&changed, &auth, &[3; 32], &options).is_err());
    let response_body =
        command::encode_result(&Err(CommandFailure::CommandConflict), 4096).unwrap();
    let response = response_head(
        &auth,
        &[3; 32],
        &request,
        &head.tag,
        &response_body,
        &options,
    )
    .unwrap();
    let read_request = RequestHead::new(read, 3, &body);
    assert!(read_response(
        &response,
        &auth,
        &[3; 32],
        &read_request,
        &head.tag,
        &options
    )
    .is_err());
    let mut missing = serde_json::to_value(write).unwrap();
    missing.as_object_mut().unwrap().remove("command");
    assert!(serde_json::from_value::<Route>(missing).is_err());
}

#[test]
fn shared_command_results_bind_identity_history_and_typed_refusals() {
    let call = command_call();
    let observation = cell_observation();
    let cell = &observation.version.history;
    let token = keys().issue_cell(&observation, [9; 32], 1000).unwrap();
    let mut body = b"CLGCCS01".to_vec();
    body.push(0);
    body.extend_from_slice(&call.request.id.client.bytes());
    body.extend_from_slice(&call.request.id.sequence().to_be_bytes());
    body.extend_from_slice(&9000u64.to_be_bytes());
    string(&mut body, token.as_str()).unwrap();
    body.extend_from_slice(&2u32.to_be_bytes());
    body.extend_from_slice(b"ok");
    let decoded = command::decode_result(
        &body,
        (call.request.id, cell),
        [9; 32],
        &keys(),
        1001,
        4096,
        MemberId::new(2).unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(decoded.value.observation, observation);
    assert_eq!(decoded.value.result, b"ok");
    let wrong = CommandId::new(call.request.id.client, 4).unwrap();
    assert!(command::decode_result(
        &body,
        (wrong, cell),
        [9; 32],
        &keys(),
        1001,
        4096,
        MemberId::new(2).unwrap()
    )
    .is_err());
    let mut other = cell.clone();
    other.incarnation = IncarnationId::new([78; 16]).unwrap();
    assert!(command::decode_result(
        &body,
        (call.request.id, &other),
        [9; 32],
        &keys(),
        1001,
        4096,
        MemberId::new(2).unwrap()
    )
    .is_err());
    for failure in [
        CommandFailure::CommandConflict,
        CommandFailure::CommandExpired,
        CommandFailure::SequenceGap { next: 9 },
        CommandFailure::NotLeader {
            leader: Some(MemberId::new(2).unwrap()),
        },
    ] {
        let body = command::encode_result(&Err(failure.clone()), 4096).unwrap();
        let decoded = command::decode_result(
            &body,
            (call.request.id, cell),
            [9; 32],
            &keys(),
            1001,
            4096,
            MemberId::new(2).unwrap(),
        )
        .unwrap();
        assert_eq!(decoded.err(), Some(failure));
    }
}
