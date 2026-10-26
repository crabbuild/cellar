// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};

fn value() -> StructuredValue {
    StructuredValue {
        root: CloneValue::Reference(0),
        objects: vec![
            Object::DenseArray {
                elements: vec![
                    CloneValue::Reference(1),
                    CloneValue::Reference(2),
                    CloneValue::Reference(4),
                ],
                properties: vec![],
            },
            Object::Object(vec![(CloneValue::from("call"), CloneValue::Reference(3))]),
            Object::DenseArray {
                elements: vec![CloneValue::Reference(3)],
                properties: vec![],
            },
            Object::Object(vec![]),
            Object::DenseArray {
                elements: vec![CloneValue::from("function")],
                properties: vec![],
            },
        ],
    }
}

fn encode(value: &StructuredValue) -> String {
    STANDARD.encode(clone::encode(value).unwrap())
}

#[test]
fn delegates_require_distinct_empty_markers_and_exact_counts() {
    let mut graph = value();
    assert!(markers(&encode(&graph), 1, 4096).is_ok());
    assert!(markers(&encode(&graph), 0, 4096).is_err());
    assert!(markers(&encode(&graph), 2, 4096).is_err());
    assert!(markers(&encode(&graph), 1, 4).is_err());
    graph.objects[3] = Object::Object(vec![(CloneValue::from("not-a-marker"), CloneValue::Null)]);
    assert!(markers(&encode(&graph), 1, 4096).is_err());
    graph = value();
    if let Object::DenseArray { elements, .. } = &mut graph.objects[2] {
        elements.push(CloneValue::Reference(3));
    }
    assert!(markers(&encode(&graph), 2, 4096).is_err());
}

#[test]
fn malformed_publications_and_non_array_arguments_refuse() {
    let valid = Publication {
        format: 1,
        group: "native-group".into(),
        count: 1,
        max_body_bytes: 4096,
        value: encode(&value()),
    };
    let encoded = serde_json::to_string(&valid).unwrap();
    let data = || cellar::engine_api::RpcData::Json(encoded.clone());
    assert!(publication(data(), 4096).is_ok());
    assert_eq!(publication(data(), 512).unwrap().max_body_bytes, 512);
    assert!(publication(data(), 4).is_err());
    let mut json: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    json["max_body_bytes"] = 0.into();
    assert!(publication(cellar::engine_api::RpcData::Json(json.to_string()), 4096).is_err());
    json["max_body_bytes"] = 4096.into();
    json["format"] = 2.into();
    assert!(publication(cellar::engine_api::RpcData::Json(json.to_string()), 4096).is_err());
    assert!(arguments(
        &encode(&StructuredValue::from_json(serde_json::json!({})).unwrap()),
        4096
    )
    .is_err());
    assert!(arguments(
        &encode(&StructuredValue::from_json(serde_json::json!([])).unwrap()),
        4096
    )
    .is_ok());
}

#[test]
fn stream_publications_require_one_known_kind_per_live_marker() {
    let mut graph = value();
    if let Object::DenseArray { elements, .. } = &mut graph.objects[4] {
        elements[0] = CloneValue::from("readable");
    }
    assert!(markers(&encode(&graph), 1, 4096).is_ok());
    if let Object::DenseArray { elements, .. } = &mut graph.objects[4] {
        elements[0] = CloneValue::from("writable");
    }
    assert!(markers(&encode(&graph), 1, 4096).is_ok());
    if let Object::DenseArray { elements, .. } = &mut graph.objects[4] {
        elements[0] = CloneValue::from("unknown-stream");
    }
    assert!(markers(&encode(&graph), 1, 4096).is_err());
    if let Object::DenseArray { elements, .. } = &mut graph.objects[4] {
        elements.clear();
    }
    assert!(markers(&encode(&graph), 1, 4096).is_err());
}

#[test]
fn input_sessions_require_argument_arrays_and_supported_transfer_markers() {
    let mut graph = value();
    assert!(inputs(&encode(&graph), 1, 4096).is_err());
    graph.objects[1] = Object::DenseArray {
        elements: vec![CloneValue::Reference(3)],
        properties: vec![],
    };
    assert!(inputs(&encode(&graph), 1, 4096).is_err());
    for kind in ["readable", "writable"] {
        graph.objects[4] = Object::DenseArray {
            elements: vec![CloneValue::from(kind)],
            properties: vec![],
        };
        assert!(inputs(&encode(&graph), 1, 4096).is_ok());
        assert!(inputs(&encode(&graph), 2, 4096).is_err());
    }
    graph.objects[1] = Object::Object(vec![]);
    assert!(inputs(&encode(&graph), 1, 4096).is_err());
}

#[test]
fn response_publications_validate_metadata_with_the_native_lease() {
    let head = serde_json::json!({
        "status": 206, "statusText": "Partial Content", "headers": [["set-cookie", "a=1"], ["set-cookie", "b=2"]],
        "body": true, "url": "https://example.test/file", "redirected": false, "type": "default"
    });
    let encoded = |head: serde_json::Value| {
        encode(&StructuredValue::from_json(serde_json::json!([[], [head], ["response"]])).unwrap())
    };
    assert!(markers(&encoded(head.clone()), 1, 4096).is_ok());
    assert!(inputs(&encoded(head.clone()), 1, 4096).is_ok());
    for (key, invalid) in [
        ("status", serde_json::json!(101)),
        ("status", serde_json::json!(204)),
        ("status", serde_json::json!(200.5)),
        ("statusText", serde_json::json!("invalid\r\nstatus")),
        ("statusText", serde_json::json!("invalid\u{0}status")),
        ("statusText", serde_json::json!("invalid\u{7f}status")),
        ("body", serde_json::json!("yes")),
        ("type", serde_json::json!("opaque")),
        ("headers", serde_json::json!([["invalid header", "value"]])),
        ("headers", serde_json::json!([["x-value", "bad\r\nvalue"]])),
        (
            "headers",
            serde_json::json!([["x-value", "x".repeat(32769)]]),
        ),
        ("url", serde_json::json!("x".repeat(8193))),
        ("unexpected", serde_json::json!(true)),
    ] {
        let mut value = head.clone();
        value[key] = invalid;
        assert!(
            markers(&encoded(value), 1, 65536).is_err(),
            "invalid field: {key}"
        );
    }
    let error = serde_json::json!({"status":0,"statusText":"","headers":[],"body":false,"url":"","redirected":false,"type":"error"});
    assert!(markers(&encoded(error), 1, 4096).is_ok());
}

#[test]
fn http_request_metadata_is_admitted_before_routing_and_callbacks_still_refuse() {
    let head = serde_json::json!({"method":"POST","url":"https://example.test/upload","headers":[["x-input","bytes"]],"body":true,"redirect":"manual","cache":"no-store","credentials":"omit","integrity":"","keepalive":false,"mode":"cors","referrer":"about:client","referrerPolicy":"","destination":""});
    let encoded = |head: serde_json::Value| {
        encode(&StructuredValue::from_json(serde_json::json!([[], [head], ["request"]])).unwrap())
    };
    assert!(markers(&encoded(head.clone()), 1, 4096).is_ok());
    assert!(inputs(&encoded(head.clone()), 1, 4096).is_ok());
    for (field, invalid) in [
        ("method", serde_json::json!("GET")),
        ("method", serde_json::json!("CONNECT")),
        ("method", serde_json::json!("bad method")),
        ("url", serde_json::json!("https://user:pass@example.test/")),
        ("url", serde_json::json!("data:text/plain,no")),
        ("url", serde_json::json!("relative")),
        ("mode", serde_json::json!("navigate")),
        ("redirect", serde_json::json!("invalid")),
        ("cache", serde_json::json!("only-if-cached")),
        ("credentials", serde_json::json!("invalid")),
        ("referrer", serde_json::json!("not a URL")),
        ("referrerPolicy", serde_json::json!("invalid")),
        ("keepalive", serde_json::json!(true)),
        ("headers", serde_json::json!([["bad header", "value"]])),
        ("headers", serde_json::json!([["x-value", "bad\r\nvalue"]])),
        ("integrity", serde_json::json!("x".repeat(8193))),
        ("body", serde_json::json!("yes")),
        ("cf", serde_json::json!({})),
    ] {
        let mut value = head.clone();
        value[field] = invalid;
        assert!(
            inputs(&encoded(value), 1, 65536).is_err(),
            "invalid field {field}"
        );
    }
    assert!(
        inputs(&encode(&value()), 1, 4096).is_err(),
        "function arguments remain unsupported"
    );
}
