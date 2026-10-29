use super::*;

fn request() -> RequestHead {
    let endpoint = |id: u8| serde_json::json!({ "node":vec![id;16], "disk":vec![id+10;16], "process":vec![id+20;16], "storage_instance":vec![id+30;16], "label":format!("node-{id}") });
    let route:NodeRoute = serde_json::from_value(serde_json::json!({
        "fleet":vec![80;16], "catalog":vec![81;32], "cluster":vec![1;16], "protocol":crate::application::consensus::protocol::protocol_identity(),
        "minimum_revision":"3", "source":endpoint(4), "target":endpoint(5),
    })).unwrap();
    route.identities().unwrap();
    let binding = crate::raft::member_state::FleetGroupBinding::new(
        cellar_logic::consensus::IncarnationId::new([80; 16]).unwrap(),
        &cellar_logic::consensus::GroupScope {
            cluster: cellar_logic::consensus::ClusterId::new([1; 16]).unwrap(),
            namespace: cellar_logic::consensus::NamespaceId::new([2; 16]).unwrap(),
            group: cellar_logic::consensus::GroupId::new([3; 16]).unwrap(),
            incarnation: cellar_logic::consensus::IncarnationId::new([4; 16]).unwrap(),
            cell: "Counter:quorum".into(),
        },
        [81; 32],
    )
    .unwrap();
    let history: History = serde_json::from_value(serde_json::json!({
        "binding":binding, "birth":vec![6;32], "schema":vec![5;32],
    }))
    .unwrap();
    RequestHead {
        route,
        history,
        nonce: "12".into(),
        timeout_ms: 1000,
    }
}

#[test]
fn fleet_quorum_mac_is_bound_to_purpose_channel_and_request() {
    let auth = PeerAuth::new([8; 32], "node-4").unwrap();
    let options = TransportOptions::default();
    let wire = Wire {
        auth: &auth,
        exporter: &[7; 32],
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
        b"cellar-inspection-request-1".as_slice(),
        b"cellar-query-request-1",
        b"cellar-node-inspection-request-1",
        RESPONSE_PURPOSE,
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
        history: request.history,
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
fn fleet_quorum_request_refuses_foreign_history_catalog_and_scope() {
    let req = request();
    validate_history_route(&req.route, &req.history).unwrap();
    let original = serde_json::to_value(&req.history).unwrap();
    for field in ["fleet", "catalog", "cluster"] {
        let mut changed = original.clone();
        changed["binding"][field] = if field == "catalog" {
            serde_json::json!(vec![92; 32])
        } else {
            serde_json::json!(vec![92; 16])
        };
        let history: History = serde_json::from_value(changed).unwrap();
        assert!(validate_history_route(&req.route, &history).is_err());
    }
    for field in ["birth", "schema"] {
        let mut changed = original.clone();
        changed[field] = serde_json::json!(vec![0; 32]);
        let history: History = serde_json::from_value(changed).unwrap();
        assert!(validate_history_route(&req.route, &history).is_err());
    }
}
