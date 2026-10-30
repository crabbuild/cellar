// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use ::raft::prelude::Entry;
use cellar_logic::consensus::*;

fn replica(member: u64) -> ReplicaIdentity {
    ReplicaIdentity {
        scope: GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: "Counter:1".into(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        member: MemberId::new(member).unwrap(),
        node: NodeId::new([member as u8; 16]).unwrap(),
        disk: DiskLineage::new([member as u8; 16]).unwrap(),
        process: ProcessGeneration::new([9; 16]).unwrap(),
    }
}

fn request() -> (RequestHeader, Vec<u8>) {
    let mut message = Message {
        from: 1,
        to: 2,
        term: 2,
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    let body = message.write_to_bytes().unwrap();
    let route = Route::new(&replica(1), "node-1", &replica(2), "node-2", [5; 32]).unwrap();
    (RequestHeader::new(route, 1, Lane::Control, &body), body)
}

#[test]
fn response_requires_channel_request_and_route() {
    let auth = PeerAuth::new([8; 32], "node-1").unwrap();
    let options = TransportOptions::default();
    let exporter = [7; 32];
    let (request, body) = request();
    let head = request_head(&auth, &exporter, &request, body.len(), &options).unwrap();
    let response = response_head(
        &auth,
        &exporter,
        &request,
        &head.tag,
        Disposition::Accepted,
        &options,
    )
    .unwrap();
    assert_eq!(
        response
            .response(&auth, &exporter, &request, &head.tag)
            .unwrap(),
        Disposition::Accepted
    );
    assert_eq!(
        response.response(&auth, &[6; 32], &request, &head.tag),
        Err(TransportError::Authentication)
    );
    assert_eq!(
        response.response(&auth, &exporter, &request, &[0; 32]),
        Err(TransportError::Authentication)
    );
    assert_eq!(
        response.response(
            &PeerAuth::new([9; 32], "node-1").unwrap(),
            &exporter,
            &request,
            &head.tag
        ),
        Err(TransportError::Authentication)
    );
    let mut wrong = request.clone();
    wrong.route.target.process = [33; 16];
    assert_eq!(
        response.response(&auth, &exporter, &wrong, &head.tag),
        Err(TransportError::Authentication)
    );
    let mut tampered = response;
    tampered.header[0] ^= 1;
    assert_eq!(
        tampered.response(&auth, &exporter, &request, &head.tag),
        Err(TransportError::Authentication)
    );
}

#[test]
fn request_binds_body_prefix_and_channel() {
    let auth = PeerAuth::new([8; 32], "node-1").unwrap();
    let options = TransportOptions::default();
    let exporter = [7; 32];
    let (request, mut body) = request();
    let mut head = request_head(&auth, &exporter, &request, body.len(), &options).unwrap();
    assert!(head.request(&auth, &exporter).is_ok());
    assert!(head.request(&auth, &[6; 32]).is_err());
    body[0] ^= 1;
    assert_eq!(
        request.verify_body(&body, &options),
        Err(TransportError::Authentication)
    );
    head.prefix[19] ^= 1;
    assert!(head.request(&auth, &exporter).is_err());
}

#[test]
fn response_domain_cannot_authenticate_request() {
    let auth = PeerAuth::new([8; 32], "node-1").unwrap();
    let options = TransportOptions::default();
    let (request, body) = request();
    let mut head = request_head(&auth, &[7; 32], &request, body.len(), &options).unwrap();
    head.tag = auth.raft_tag(RESPONSE, &[&[7; 32], &head.prefix, &head.header]);
    assert!(head.request(&auth, &[7; 32]).is_err());
}

#[test]
fn prefix_rejects_large_lengths_before_header_allocation() {
    let options = TransportOptions::default();
    let mut bytes = prefix(b"CLRP", 1024, 0, &options).unwrap();
    bytes[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        parse_prefix(&bytes, &options, Lane::Control, false),
        Err(TransportError::Limit)
    );
    bytes[8..12].copy_from_slice(&1024_u32.to_be_bytes());
    bytes[12..20].copy_from_slice(&u64::MAX.to_be_bytes());
    assert_eq!(
        parse_prefix(&bytes, &options, Lane::Data, false),
        Err(TransportError::Limit)
    );
    bytes[12..20].copy_from_slice(&(options.max_control_bytes as u64 + 1).to_be_bytes());
    assert_eq!(
        parse_prefix(&bytes, &options, Lane::Control, false),
        Err(TransportError::Limit)
    );
}

#[test]
fn protobuf_preflight_bounds_decoded_entries_and_snapshot_members() {
    assert_eq!(
        scan_message(&[0x3a, 0, 0x3a, 0], 1),
        Err(TransportError::Limit)
    );
    assert_eq!(scan_message(&[0x3a, 0, 0x3a, 0], 2), Ok(()));
    assert_eq!(
        scan_message(&[0x08, 3, 0x08, 3], 2),
        Err(TransportError::Protocol)
    );
    assert_eq!(scan_message(&[0x08, 19], 2), Err(TransportError::Protocol));
    assert_eq!(
        scan_message(&[0x3b, 0x3c], 2),
        Err(TransportError::Protocol)
    );
    assert_eq!(
        scan_message(&[0x3a, 0xff, 0xff], 2),
        Err(TransportError::Protocol)
    );
    assert_eq!(
        scan_message(&[0x08, 0x80, 0], 2),
        Err(TransportError::Protocol)
    );
    let mut snapshot = ::raft::prelude::Snapshot::default();
    snapshot.mut_metadata().mut_conf_state().voters = vec![1; 129];
    let mut message = Message::default();
    message.set_snapshot(snapshot);
    assert_eq!(
        scan_message(&message.write_to_bytes().unwrap(), 2),
        Err(TransportError::Limit)
    );
}

#[test]
fn lane_is_derived_from_actual_message() {
    let mut message = Message::default();
    message.set_msg_type(MessageType::MsgHeartbeat);
    assert_eq!(classify(&message).unwrap(), Lane::Control);
    message.entries.push(Entry::default());
    assert_eq!(classify(&message), Err(TransportError::Protocol));
    message.set_msg_type(MessageType::MsgAppend);
    assert_eq!(classify(&message).unwrap(), Lane::Data);
    message.entries.clear();
    assert_eq!(classify(&message).unwrap(), Lane::Control);
    message.set_msg_type(MessageType::MsgPropose);
    assert_eq!(classify(&message), Err(TransportError::Protocol));
}

#[test]
fn strict_header_identity_sequence_and_protocol() {
    let (mut header, _) = request();
    assert!(header.validate().is_ok());
    header.sequence = "01".into();
    assert_eq!(header.validate(), Err(TransportError::Protocol));
    header.sequence = "0".into();
    assert_eq!(header.validate(), Err(TransportError::Replay));
    header.sequence = u64::MAX.to_string();
    assert!(header.validate().is_ok());
    header.route.target.process = [0; 16];
    assert_eq!(header.validate(), Err(TransportError::Identity));
    header.route.target.process = [1; 16];
    header.protocol[0] ^= 1;
    assert_eq!(header.validate(), Err(TransportError::Protocol));
    let (header, _) = request();
    let mut bytes = serde_json::to_string(&header).unwrap();
    bytes.insert_str(1, "\"sequence\":\"2\",");
    assert!(serde_json::from_str::<RequestHeader>(&bytes).is_err());
}

#[test]
fn physical_history_is_typed_authenticated_and_refuses_old_wire() {
    let physical = |n| {
        let peer = replica(n);
        ReplicaIdentity {
            scope: GroupHistory::from(&peer.scope),
            member: peer.member,
            node: peer.node,
            disk: peer.disk,
            process: peer.process,
        }
    };
    let route = Route::new_group(&physical(1), "node-1", &physical(2), "node-2", [5; 32]).unwrap();
    assert!(route.decode().is_err());
    assert_eq!(route.decode_group().unwrap(), (physical(1), physical(2)));
    let (mut request, body) = request();
    request.route = route.clone();
    let auth = PeerAuth::new([8; 32], "node-1").unwrap();
    let options = TransportOptions::default();
    let exporter = [7; 32];
    let head = request_head(&auth, &exporter, &request, body.len(), &options).unwrap();
    let decoded = head.request(&auth, &exporter).unwrap();
    assert!(decoded.verify_body(&body, &options).is_ok());
    let response = response_head(
        &auth,
        &exporter,
        &request,
        &head.tag,
        Disposition::Accepted,
        &options,
    )
    .unwrap();
    let mut changed = request.clone();
    let mut target = physical(2);
    target.scope.incarnation = IncarnationId::new([77; 16]).unwrap();
    let mut source = physical(1);
    source.scope = target.scope;
    changed.route = Route::new_group(&source, "node-1", &target, "node-2", [5; 32]).unwrap();
    assert_eq!(
        response.response(&auth, &exporter, &changed, &head.tag),
        Err(TransportError::Authentication)
    );
    let mut invalid = route.clone();
    invalid.scope = History::Group([[0; 16]; 4]);
    assert!(invalid.validate().is_err());
    let mut old = serde_json::to_value(route).unwrap();
    old["scope"] = serde_json::to_value(WireScope::from(&replica(1).scope)).unwrap();
    assert!(serde_json::from_value::<Route>(old).is_err());
    request.protocol = Sha256::digest(
        b"cellar-raft-peer-1:tls13-exporter:hmac-sha256:protobuf2:bounded-strict:extent-stream",
    )
    .into();
    assert_eq!(request.validate(), Err(TransportError::Protocol));
}
