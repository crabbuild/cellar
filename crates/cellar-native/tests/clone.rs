// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use cellar_native::clone::{
    decode, decode_with_limits, encode, encode_with_limits, Limits, Object, StructuredValue, Value,
    ViewKind,
};

#[test]
fn composing_graphs_relocates_cycles_views_maps_and_error_causes() {
    let value = StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::Object(vec![
                ("self".into(), Value::Reference(0)),
                ("map".into(), Value::Reference(1)),
                ("view".into(), Value::Reference(2)),
                ("buffer".into(), Value::Reference(3)),
                ("error".into(), Value::Reference(4)),
            ]),
            Object::Map(vec![(Value::Reference(0), Value::Reference(2))]),
            Object::View {
                buffer: 3,
                kind: ViewKind::Uint8,
                offset: 1,
                length: 2,
                flags: 0,
            },
            Object::ArrayBuffer {
                bytes: vec![0, 1, 2, 3],
                max_length: None,
                immutable: false,
            },
            Object::Error {
                prototype: None,
                message: Some(vec![0xd800]),
                stack: None,
                cause: Some(Value::Reference(0)),
            },
        ],
    };
    let mut envelope =
        StructuredValue::from_json(serde_json::json!({"control":{"id":"outer"}})).unwrap();
    let name = "payload".encode_utf16().collect::<Vec<_>>();
    envelope.set_property(name.clone(), &value).unwrap();
    let decoded = decode(&encode(&envelope).unwrap()).unwrap();
    let payload = decoded.property(&name).unwrap().unwrap();
    let Value::Reference(root) = payload.root else {
        panic!("payload root")
    };
    assert_eq!(
        payload
            .property(&"self".encode_utf16().collect::<Vec<_>>())
            .unwrap()
            .unwrap()
            .root,
        Value::Reference(root)
    );
    let field = |name: &str| {
        payload
            .property(&name.encode_utf16().collect::<Vec<_>>())
            .unwrap()
            .unwrap()
            .root
    };
    let (
        Value::Reference(view),
        Value::Reference(buffer),
        Value::Reference(map),
        Value::Reference(error),
    ) = (field("view"), field("buffer"), field("map"), field("error"))
    else {
        panic!("fields")
    };
    assert!(
        matches!(payload.objects[view as usize],Object::View {buffer:b,offset:1,length:2,..} if b==buffer)
    );
    assert!(
        matches!(&payload.objects[map as usize],Object::Map(entries) if entries==&vec![(Value::Reference(root),Value::Reference(view))])
    );
    assert!(
        matches!(&payload.objects[error as usize],Object::Error {cause:Some(Value::Reference(c)),message:Some(m),..} if *c==root && m==&vec![0xd800])
    );
    let array = StructuredValue::array([value.clone(), value.clone()]).unwrap();
    let Object::DenseArray { elements, .. } = &array.objects[0] else {
        panic!("array")
    };
    assert_ne!(
        elements[0], elements[1],
        "independent imports preserve their own identity"
    );
    assert!(envelope.remove_property(&name).unwrap());
    assert!(!envelope.remove_property(&name).unwrap());
    assert!(envelope.property(&name).unwrap().is_none());
}

#[test]
fn graph_import_refuses_invalid_references_and_object_exhaustion_atomically() {
    let mut value = StructuredValue::from_json(serde_json::json!({"old":1})).unwrap();
    let before = value.clone();
    let invalid = StructuredValue {
        root: Value::Reference(0),
        objects: vec![Object::Object(vec![("bad".into(), Value::Reference(9))])],
    };
    assert!(value.set_property(vec![0xd800], &invalid).is_err());
    assert_eq!(value, before);
    let oversized = StructuredValue {
        root: Value::Undefined,
        objects: vec![Object::Date(0.0); Limits::default().objects],
    };
    assert!(value.import(&oversized).is_err());
    assert_eq!(value, before);
    assert!(StructuredValue::undefined()
        .set_property(vec![], &before)
        .is_err());
}

#[test]
fn native_values_retain_cycles_holes_and_utf16() {
    let value = StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::Object(vec![
                ("self".into(), Value::Reference(0)),
                ("array".into(), Value::Reference(1)),
            ]),
            Object::DenseArray {
                elements: vec![
                    Value::Hole,
                    Value::Undefined,
                    Value::String(vec![0xd800, 0x96ea]),
                    Value::Reference(0),
                ],
                properties: Vec::new(),
            },
        ],
    };
    assert_eq!(decode(&encode(&value).unwrap()).unwrap(), value);
}

#[test]
fn native_values_retain_shared_buffer_views() {
    let value = StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::DenseArray {
                elements: vec![
                    Value::Reference(1),
                    Value::Reference(3),
                    Value::Reference(2),
                ],
                properties: Vec::new(),
            },
            Object::View {
                buffer: 2,
                kind: ViewKind::Uint16,
                offset: 2,
                length: 4,
                flags: 0,
            },
            Object::ArrayBuffer {
                bytes: vec![1, 2, 3, 4, 5, 6, 7, 8],
                max_length: None,
                immutable: false,
            },
            Object::View {
                buffer: 2,
                kind: ViewKind::DataView,
                offset: 1,
                length: 5,
                flags: 0,
            },
        ],
    };
    let decoded = decode(&encode(&value).unwrap()).unwrap();
    let Object::DenseArray { elements, .. } = &decoded.objects[0] else {
        panic!("array");
    };
    let (Value::Reference(first), Value::Reference(second), Value::Reference(buffer)) =
        (&elements[0], &elements[1], &elements[2])
    else {
        panic!("references");
    };
    assert!(
        matches!(decoded.objects[*first as usize], Object::View { buffer: b, .. } if b == *buffer)
    );
    assert!(
        matches!(decoded.objects[*second as usize], Object::View { buffer: b, .. } if b == *buffer)
    );
}

#[test]
fn legacy_headers_varints_and_broken_version13_views() {
    assert_eq!(
        decode(&[255, 10, b'I', 1]).unwrap().root,
        Value::Number(-1.0)
    );
    assert_eq!(
        decode(&[255, 15, b'U', 255, 255, 255, 255, 15])
            .unwrap()
            .root,
        Value::Number(u32::MAX as f64)
    );
    assert!(decode(&[255, 15, b'U', 255, 255, 255, 255, 16]).is_err());
    assert!(decode(&[255, 17, b'0']).is_err());
    for suffix in [&[][..], &[0][..]] {
        let mut bytes = vec![255, 13, b'B', 2, 0x12, 0x34, b'V', b'W', 0, 2];
        bytes.extend(suffix);
        assert!(matches!(
            decode(&bytes).unwrap().objects[1],
            Object::View {
                kind: ViewKind::Uint16,
                length: 2,
                ..
            }
        ));
    }
    let invalid = [255, 15, b'B', 2, 0, 0, b'V', b'W', 1, 1, 0];
    assert!(decode(&invalid).is_err());
    let broken = [255, 13, b'~', 2, 4, 0x12, 0x34, b'V', b'W', 0, 2, 3];
    assert!(matches!(
        decode(&broken).unwrap().objects[1],
        Object::View { flags: 3, .. }
    ));
}

#[test]
fn corrupt_payloads_and_external_capabilities_fail_closed() {
    for bytes in [
        vec![],
        vec![255],
        vec![255, 15, b'^', 0],
        vec![255, 15, b'c', 1, 1],
        vec![255, 15, b'Z', 1],
        vec![255, 15, b'A', 255, 255, 255, 255, 15],
        vec![255, 15, b'o', b'{', 1],
        vec![255, 15, b'A', 0, b'$', 0, 1],
        vec![255, 15, b'0', b'0'],
        vec![255, 15, b'u', 0],
        vec![255, 15, b'\\', 0],
    ] {
        assert!(decode(&bytes).is_err(), "accepted {bytes:?}");
    }
    let graph = StructuredValue {
        root: Value::Reference(1),
        objects: vec![],
    };
    assert!(encode(&graph).is_err());
    let graph = StructuredValue {
        root: Value::Hole,
        objects: vec![],
    };
    assert!(encode(&graph).is_err());
}

#[test]
fn decoding_and_encoding_apply_resource_limits() {
    let graph = StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::DenseArray {
                elements: vec![Value::Reference(1)],
                properties: vec![],
            },
            Object::Object(vec![]),
        ],
    };
    let bytes = encode(&graph).unwrap();
    assert!(decode_with_limits(
        &bytes,
        Limits {
            bytes: 1,
            ..Limits::default()
        }
    )
    .is_err());
    assert!(decode_with_limits(
        &bytes,
        Limits {
            objects: 1,
            ..Limits::default()
        }
    )
    .is_err());
    assert!(decode_with_limits(
        &bytes,
        Limits {
            values: 1,
            ..Limits::default()
        }
    )
    .is_err());
    assert!(decode_with_limits(
        &bytes,
        Limits {
            depth: 0,
            ..Limits::default()
        }
    )
    .is_err());
    assert!(encode_with_limits(
        &graph,
        Limits {
            bytes: 1,
            ..Limits::default()
        }
    )
    .is_err());
    assert!(encode_with_limits(
        &graph,
        Limits {
            depth: 0,
            ..Limits::default()
        }
    )
    .is_err());
}
