// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::fleet_stop::{FleetStopAction, StopSelector};

fn request() -> (RequestHead, Vec<u8>) {
    let endpoint = |id: u8| serde_json::json!({"node":vec![id;16],"disk":vec![id+10;16],"process":vec![id+20;16],"storage_instance":vec![id+30;16],"label":format!("node-{id}")});
    let route:NodeRoute=serde_json::from_value(serde_json::json!({"fleet":vec![80;16],"catalog":vec![81;32],"cluster":vec![1;16],"protocol":crate::application::consensus::protocol::protocol_identity(),"minimum_revision":"3","source":endpoint(4),"target":endpoint(5)})).unwrap();
    let boot = |id: u8| BootBinding {
        catalog: [81; 32],
        fleet: [80; 16],
        node: [id; 16],
        disk: [id + 10; 16],
        engine: [id + 30; 16],
        boot: [id + 40; 16],
        process: [id + 20; 16],
        epoch: 1,
        join_key: crate::peer_probe::PeerProbeSigner::from_secret([id; 32]).public_key_bytes(),
    };
    let stop = FleetStopRequest::new(
        StopSelector {
            purpose: crate::raft::fleet_stop::LifecyclePurpose::Evacuation,
            boot: boot(5),
            epoch: 2,
            operation: [90; 16],
            finalization: [91; 16],
            request: [92; 16],
        },
        FleetStopAction::Stop,
    )
    .unwrap();
    let bytes = serde_json::to_vec(&stop).unwrap();
    (
        RequestHead {
            route,
            origin: boot(4),
            nonce: "12".into(),
            timeout_ms: 1000,
            digest: Sha256::digest(&bytes).into(),
        },
        bytes,
    )
}

#[test]
fn fleet_stop_mac_binds_purpose_channel_payload_and_reply() {
    let auth = PeerAuth::new([8; 32], "node-4").unwrap();
    let options = TransportOptions::default();
    let wire = Wire {
        auth: &auth,
        exporter: &[7; 32],
        options: &options,
        max_body: MAX_BODY,
    };
    let (request, bytes) = request();
    let head = wire
        .encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &bytes)
        .unwrap();
    let decoded: RequestHead = wire
        .decode(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None)
        .unwrap();
    assert!(decoded.decode_body(&bytes).is_ok());
    for purpose in [
        b"cellar-node-inspection-request-1".as_slice(),
        b"cellar-inspection-request-1",
        RESPONSE_PURPOSE,
    ] {
        assert!(matches!(
            wire.decode::<RequestHead>(REQUEST_MAGIC, purpose, &head, None),
            Err(TransportError::Authentication)
        ));
    }
    let other = Wire {
        exporter: &[9; 32],
        ..wire
    };
    assert!(matches!(
        other.decode::<RequestHead>(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None),
        Err(TransportError::Authentication)
    ));
    let mut changed = bytes.clone();
    changed[0] ^= 1;
    assert!(matches!(
        decoded.decode_body(&changed),
        Err(TransportError::Protocol)
    ));
    let reply = ReplyHead {
        route: request.route.clone(),
        nonce: request.nonce.clone(),
        digest: request.digest,
        status: Status::Accepted,
    };
    let response = wire
        .encode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &head.tag, &reply, &[])
        .unwrap();
    let decoded: ReplyHead = wire
        .decode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &response, Some(&head.tag))
        .unwrap();
    assert_eq!(decoded.check(&request).unwrap(), Status::Accepted);
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
fn fleet_stop_wire_requires_exact_root_and_physical_endpoints() {
    let (head, bytes) = request();
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    for (field, len) in [
        ("catalog", 32),
        ("fleet", 16),
        ("node", 16),
        ("disk", 16),
        ("process", 16),
        ("engine", 16),
    ] {
        let mut changed = original.clone();
        changed["selector"]["boot"][field] = serde_json::json!(vec![120; len]);
        let stop: FleetStopRequest = serde_json::from_value(changed).unwrap();
        assert!(matches!(
            check_request(&head.route, &stop),
            Err(TransportError::Identity)
        ));
    }
    for field in ["catalog", "fleet", "node", "disk", "process", "engine"] {
        let mut changed = serde_json::to_value(&head.origin).unwrap();
        let n = if field == "catalog" { 32 } else { 16 };
        changed[field] = serde_json::json!(vec![120; n]);
        let bad = RequestHead {
            origin: serde_json::from_value(changed).unwrap(),
            ..request().0
        };
        assert!(matches!(
            bad.decode_body(&bytes),
            Err(TransportError::Identity)
        ));
    }
    assert!(serde_json::to_value(&head.route)
        .unwrap()
        .get("scope")
        .is_none());
}

#[test]
fn fleet_stop_wire_rejects_unbounded_or_noncanonical_work() {
    let (head, bytes) = request();
    for nonce in ["0", "012", "18446744073709551616"] {
        let bad = RequestHead {
            nonce: nonce.into(),
            ..request().0
        };
        assert!(bad.decode_body(&bytes).is_err());
    }
    for timeout_ms in [0, 60_001, u64::MAX] {
        let bad = RequestHead {
            timeout_ms,
            ..request().0
        };
        assert!(bad.decode_body(&bytes).is_err());
    }
    assert!(head.decode_body(&vec![0; MAX_BODY + 1]).is_err());
    let mut malformed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    malformed["extra"] = serde_json::json!(true);
    let changed = serde_json::to_vec(&malformed).unwrap();
    let bad = RequestHead {
        digest: Sha256::digest(&changed).into(),
        ..request().0
    };
    assert!(bad.decode_body(&changed).is_err());
    let mut reply = ReplyHead {
        route: head.route.clone(),
        nonce: head.nonce.clone(),
        digest: head.digest,
        status: Status::Accepted,
    };
    reply.digest[0] ^= 1;
    assert!(matches!(reply.check(&head), Err(TransportError::Replay)));
    reply.digest = head.digest;
    reply.nonce = "13".into();
    assert!(reply.check(&head).is_err());
    assert!(matches!(
        Status::Unconfirmed.result(),
        Err(FleetCatalogError::Unconfirmed)
    ));
    assert!(matches!(
        Status::Overloaded.result(),
        Err(FleetCatalogError::Overloaded)
    ));
    assert!(matches!(
        Status::Conflict.result(),
        Err(FleetCatalogError::Conflict)
    ));
}

#[test]
fn fleet_stop_receiver_budget_includes_header_residence() {
    let (mut head, _) = request();
    head.timeout_ms = 100;
    assert_eq!(head.deadline(100, 199).unwrap(), 200);
    for now in [99, 200, 500] {
        assert!(matches!(
            head.deadline(100, now),
            Err(TransportError::Timeout)
        ));
    }
    assert!(matches!(
        head.deadline(u64::MAX - 50, u64::MAX - 25),
        Err(TransportError::Limit)
    ));
}
