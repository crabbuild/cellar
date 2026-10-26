use super::*;

fn request() -> RequestHead {
    let endpoint = |id: u8| serde_json::json!({ "node":vec![id;16], "disk":vec![id+10;16], "process":vec![id+20;16], "storage_instance":vec![id+30;16], "label":format!("node-{id}") });
    let route:NodeRoute = serde_json::from_value(serde_json::json!({
        "fleet":vec![80;16], "catalog":vec![81;32], "cluster":vec![1;16], "protocol":crate::application::consensus::protocol::protocol_identity(),
        "minimum_revision":"3", "source":endpoint(4), "target":endpoint(5),
    })).unwrap();
    route.identities().unwrap();
    RequestHead {
        route,
        nonce: "12".into(),
        timeout_ms: 1000,
        physical: false,
    }
}

#[test]
fn node_metadata_mac_is_bound_to_purpose_channel_and_request() {
    let auth = PeerAuth::new([8; 32], "node-4").unwrap();
    let options = TransportOptions::default();
    let wire = Wire {
        auth: &auth,
        exporter: &[7; 32],
        options: &options,
        max_body: body::MAX_BODY,
    };
    let mut physical_request = request();
    physical_request.physical = true;
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
        nonce: request.nonce,
        status: 2,
        digest: Sha256::digest([]).into(),
    };
    let response = wire
        .encode(RESPONSE_MAGIC, RESPONSE_PURPOSE, &head.tag, &reply, &[])
        .unwrap();
    let physical_head = wire
        .encode(REQUEST_MAGIC, REQUEST_PURPOSE, &[], &physical_request, &[])
        .unwrap();
    assert!(matches!(
        wire.decode::<ReplyHead>(
            RESPONSE_MAGIC,
            RESPONSE_PURPOSE,
            &response,
            Some(&physical_head.tag)
        ),
        Err(TransportError::Authentication)
    ));
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
fn node_route_requires_valid_fleet_processes_and_revision() {
    let original = serde_json::to_value(request().route).unwrap();
    assert!(original.get("scope").is_none());
    let mut missing = original.clone();
    missing.as_object_mut().unwrap().remove("catalog");
    assert!(serde_json::from_value::<NodeRoute>(missing).is_err());
    let mut zero = original.clone();
    zero["catalog"] = serde_json::json!(vec![0; 32]);
    assert!(serde_json::from_value::<NodeRoute>(zero)
        .unwrap()
        .identities()
        .is_err());
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
        let mut changed = original.clone();
        changed[path[0]][path[1]] = serde_json::json!(vec![0; 16]);
        assert!(serde_json::from_value::<NodeRoute>(changed)
            .unwrap()
            .identities()
            .is_err());
    }
    for field in ["fleet", "cluster"] {
        let mut changed = original.clone();
        changed[field] = serde_json::json!(vec![0; 16]);
        assert!(serde_json::from_value::<NodeRoute>(changed)
            .unwrap()
            .identities()
            .is_err());
    }
    for revision in ["0", "03", "18446744073709551615"] {
        let mut changed = original.clone();
        changed["minimum_revision"] = serde_json::json!(revision);
        assert!(serde_json::from_value::<NodeRoute>(changed)
            .unwrap()
            .identities()
            .is_err());
    }
    let mut changed = original.clone();
    changed["protocol"] = serde_json::json!(vec![0; 32]);
    assert!(serde_json::from_value::<NodeRoute>(changed)
        .unwrap()
        .identities()
        .is_err());
    let mut changed = original;
    changed["target"]["node"] = changed["source"]["node"].clone();
    assert!(serde_json::from_value::<NodeRoute>(changed)
        .unwrap()
        .identities()
        .is_err());
}
