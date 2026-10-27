// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::fleet_catalog::{MovementAction, MovementCommand};

fn fixture() -> (RequestHead, Vec<u8>) {
    let endpoint = |id: u8| serde_json::json!({"node":vec![id;16],"disk":vec![id+10;16],"process":vec![id+20;16],"storage_instance":vec![id+30;16],"label":format!("node-{id}")});
    let route: NodeRoute = serde_json::from_value(serde_json::json!({
        "fleet":vec![80;16],"catalog":vec![81;32],"cluster":vec![1;16],
        "protocol":crate::application::consensus::protocol::protocol_identity(),
        "minimum_revision":"3","source":endpoint(4),"target":endpoint(5),
    }))
    .unwrap();
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
    let command = MovementCommand {
        reservation: [90; 16],
        resource_digest: [91; 32],
        target: boot(5),
        action: MovementAction::Begin,
    };
    let bytes = serde_json::to_vec(&FleetExecutionCommand::Movement(command)).unwrap();
    assert!(bytes.len() <= MAX_BODY);
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
fn fleet_move_wire_binds_channel_purpose_body_and_response() {
    let auth = PeerAuth::new([8; 32], "node-4").unwrap();
    let options = TransportOptions::default();
    let wire = Wire {
        auth: &auth,
        exporter: &[7; 32],
        options: &options,
        max_body: MAX_BODY,
    };
    let (request, bytes) = fixture();
    let head = wire
        .encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &request, &bytes)
        .unwrap();
    let decoded: RequestHead = wire
        .decode(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None)
        .unwrap();
    assert!(decoded.decode_body(&bytes).is_ok());
    for purpose in [
        b"cellar-fleet-stop-request-1".as_slice(),
        b"cellar-fleet-quorum-request-1",
        b"cellar-fleet-move-request-1",
        RESPONSE_PURPOSE,
    ] {
        assert!(wire
            .decode::<RequestHead>(REQUEST_MAGIC, purpose, &head, None)
            .is_err());
    }
    let other = Wire {
        exporter: &[9; 32],
        ..wire
    };
    assert!(other
        .decode::<RequestHead>(REQUEST_MAGIC, REQUEST_PURPOSE, &head, None)
        .is_err());
    let mut changed = bytes.clone();
    changed[0] ^= 1;
    assert!(decoded.decode_body(&changed).is_err());
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
    assert!(wire
        .decode::<ReplyHead>(RESPONSE_MAGIC, RESPONSE_PURPOSE, &response, Some(&[0; 32]))
        .is_err());
    let mut foreign = decoded;
    foreign.digest[0] ^= 1;
    assert!(foreign.check(&request).is_err());
}

#[test]
fn fleet_move_wire_refuses_wrong_boot_scope_and_invalid_action() {
    let (request, bytes) = fixture();
    let original: FleetExecutionCommand = serde_json::from_slice(&bytes).unwrap();
    for field in ["catalog", "fleet", "node", "disk", "process", "engine"] {
        let mut value = serde_json::to_value(&original).unwrap();
        let array = value["command"]["target"][field].as_array_mut().unwrap();
        array[0] = serde_json::json!(99);
        let bytes = serde_json::to_vec(&value).unwrap();
        let head = RequestHead {
            digest: Sha256::digest(&bytes).into(),
            ..fixture().0
        };
        assert!(head.decode_body(&bytes).is_err(), "{field}");
    }
    let FleetExecutionCommand::Movement(mut command) = original else {
        panic!("movement fixture");
    };
    command.action = MovementAction::Promote { revision: 0 };
    let bytes = serde_json::to_vec(&FleetExecutionCommand::Movement(command)).unwrap();
    let head = RequestHead {
        digest: Sha256::digest(&bytes).into(),
        ..request
    };
    assert!(head.decode_body(&bytes).is_err());
    let mut origin = fixture().0;
    origin.origin.process[0] ^= 1;
    assert!(origin.decode_body(&fixture().1).is_err());
}

#[test]
fn fleet_move_deadline_includes_connection_and_body_time() {
    let (mut request, bytes) = fixture();
    assert_eq!(request.deadline(10, 10).unwrap(), 1010);
    assert!(request.deadline(10, 1010).is_err());
    assert!(request.deadline(10, 9).is_err());
    request.timeout_ms = 0;
    assert!(request.decode_body(&bytes).is_err());
    request.timeout_ms = 60_001;
    assert!(request.decode_body(&bytes).is_err());
    assert!(request.deadline(u64::MAX, u64::MAX).is_err());
}

#[test]
fn fleet_action_wire_refuses_legacy_and_unknown_commands() {
    let (mut head, bytes) = fixture();
    let FleetExecutionCommand::Movement(movement) = serde_json::from_slice(&bytes).unwrap() else {
        panic!("movement fixture");
    };
    let legacy = serde_json::to_vec(&movement).unwrap();
    head.digest = Sha256::digest(&legacy).into();
    assert!(head.decode_body(&legacy).is_err());
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["kind"] = serde_json::json!("untracked_initial_member");
    let unknown = serde_json::to_vec(&value).unwrap();
    head.digest = Sha256::digest(&unknown).into();
    assert!(head.decode_body(&unknown).is_err());
}

#[test]
fn fleet_action_genesis_wire_binds_boot_and_input_pins() {
    let (mut head, bytes) = fixture();
    let original: FleetExecutionCommand = serde_json::from_slice(&bytes).unwrap();
    let command =
        FleetExecutionCommand::RestartGenesis(crate::raft::fleet_catalog::RestartActionContext {
            parent: [2; 16],
            operation: [3; 16],
            inputs: [4; 32],
            group: "ab".repeat(32),
            history: [5; 32],
            target: original.target().clone(),
        });
    let bytes = serde_json::to_vec(&command).unwrap();
    assert!(bytes.len() <= MAX_BODY);
    head.digest = Sha256::digest(&bytes).into();
    assert!(head.decode_body(&bytes).is_ok());
    for field in ["parent", "operation", "inputs", "history"] {
        let mut value = serde_json::to_value(&command).unwrap();
        for byte in value["command"][field].as_array_mut().unwrap() {
            *byte = serde_json::json!(0);
        }
        let bytes = serde_json::to_vec(&value).unwrap();
        head.digest = Sha256::digest(&bytes).into();
        assert!(head.decode_body(&bytes).is_err(), "{field}");
    }
    for field in ["catalog", "fleet", "node", "disk", "process", "engine"] {
        let mut value = serde_json::to_value(&command).unwrap();
        value["command"]["target"][field][0] = serde_json::json!(99);
        let bytes = serde_json::to_vec(&value).unwrap();
        head.digest = Sha256::digest(&bytes).into();
        assert!(head.decode_body(&bytes).is_err(), "{field}");
    }
    for group in ["0".repeat(63), "AB".repeat(32), "zz".repeat(32)] {
        let mut value = serde_json::to_value(&command).unwrap();
        value["command"]["group"] = serde_json::json!(group);
        let bytes = serde_json::to_vec(&value).unwrap();
        head.digest = Sha256::digest(&bytes).into();
        assert!(head.decode_body(&bytes).is_err());
    }
}

#[test]
fn fleet_action_restart_leadership_wire_pins_context_and_indices() {
    let (mut head, bytes) = fixture();
    let original: FleetExecutionCommand = serde_json::from_slice(&bytes).unwrap();
    let command = serde_json::json!({
        "kind":"restart_leadership", "command": {
            "context": {"parent":vec![2;16], "operation":vec![3;16], "inputs":vec![4;32],
                "group":"ab".repeat(32), "history":vec![5;32], "target":original.target()},
            "term":2, "revision":1, "through":1, "action":{"kind":"observe"}
        }
    });
    let bytes = serde_json::to_vec(&command).unwrap();
    assert!(bytes.len() <= MAX_BODY);
    head.digest = Sha256::digest(&bytes).into();
    assert!(head.decode_body(&bytes).is_ok());
    for field in ["term", "revision", "through"] {
        for n in [0, u64::MAX - 2, u64::MAX] {
            let mut changed = command.clone();
            changed["command"][field] = n.into();
            let bytes = serde_json::to_vec(&changed).unwrap();
            head.digest = Sha256::digest(&bytes).into();
            assert!(head.decode_body(&bytes).is_err(), "{field}={n}");
        }
    }
    let mut changed = command.clone();
    changed["command"]["context"]["target"]["process"][0] = 99.into();
    let bytes = serde_json::to_vec(&changed).unwrap();
    head.digest = Sha256::digest(&bytes).into();
    assert!(head.decode_body(&bytes).is_err());
    let mut valid = command.clone();
    let mut destination = serde_json::to_value(original.target()).unwrap();
    destination["node"][0] = 71.into();
    valid["command"]["action"] =
        serde_json::json!({"kind":"transfer", "destination":destination, "member":2});
    let bytes = serde_json::to_vec(&valid).unwrap();
    assert!(bytes.len() <= MAX_BODY);
    head.digest = Sha256::digest(&bytes).into();
    assert!(head.decode_body(&bytes).is_ok());
    let mut changed = command;
    changed["command"]["action"] =
        serde_json::json!({"kind":"transfer", "destination":original.target(), "member":2});
    let bytes = serde_json::to_vec(&changed).unwrap();
    head.digest = Sha256::digest(&bytes).into();
    assert!(head.decode_body(&bytes).is_err(), "same-node transfer");
}
