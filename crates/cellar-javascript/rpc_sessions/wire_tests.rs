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
fn input_sessions_require_argument_arrays_and_stream_only_markers() {
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
