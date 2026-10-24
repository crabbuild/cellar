// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use cellar_native::clone::{Object, StructuredValue, Value};
use cellar_native::sql::{BindValue, Error};
use serde_json::json;

#[test]
fn sql_bind_values_retain_byte_and_number_semantics() {
    assert_eq!(BindValue::from(f64::NAN).into_json(), json!(null));
    assert_eq!(BindValue::from(f64::INFINITY).into_json(), json!(null));
    assert_eq!(BindValue::from(true).into_json(), json!(1));
    assert_eq!(
        BindValue::from_json(json!(false)).unwrap().into_json(),
        json!(0)
    );
    assert_eq!(
        BindValue::typed_elements([-1.0, 1.5, 257.0, f64::NAN]).into_json(),
        json!([255, 1, 1, 0])
    );
    assert_eq!(
        BindValue::typed_elements([65.0, 66.0]).into_json(),
        json!([65, 66])
    );
    assert_eq!(BindValue::from(vec![0, 255]).into_json(), json!([0, 255]));
    assert!(
        BindValue::from_json(json!([1.5])).is_ok(),
        "fractional array bytes are refused by execution, as in the original facade"
    );
    for value in [json!({}), json!([true]), json!([-1]), json!([256])] {
        let error = BindValue::from_json(value).unwrap_err();
        assert_eq!(error.family, "SQL_TYPE_ERROR");
        assert_eq!(error.cause(), error.message);
    }
    assert_eq!(
        Error::from_message("SQL_EXEC_ERROR: failure").to_string(),
        "SQL_EXEC_ERROR: failure"
    );
    assert_eq!(
        Error::from_message("channel closed").to_string(),
        "SQL_ERROR: channel closed"
    );
    assert_eq!(
        Error::new("SQL_COLUMN_NOTFOUND", "Column not found (x)").cause(),
        "Column not found"
    );
}

#[test]
fn json_projection_handles_undefined_aliases_numbers_and_refuses_loss() {
    let graph = StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::Object(vec![
                ("first".into(), Value::Undefined),
                ("a".into(), Value::Reference(1)),
                ("b".into(), Value::Reference(1)),
            ]),
            Object::DenseArray {
                elements: vec![
                    Value::Number(255.0),
                    Value::Number(-0.0),
                    Value::Number(f64::NAN),
                    Value::Undefined,
                    Value::Hole,
                ],
                properties: vec![],
            },
        ],
    };
    assert_eq!(
        graph.to_json().unwrap(),
        json!({"a":[255,0,null,null,null],"b":[255,0,null,null,null]})
    );
    let mut cyclic = graph.clone();
    if let Object::Object(properties) = &mut cyclic.objects[0] {
        properties.push(("self".into(), Value::Reference(0)));
    }
    assert!(cyclic.to_json().is_err());
    let invalid = StructuredValue {
        root: Value::String(vec![0xd800]),
        objects: vec![],
    };
    assert!(invalid.to_json().is_err());
    let sparse = StructuredValue {
        root: Value::Reference(0),
        objects: vec![Object::SparseArray {
            length: 3,
            properties: vec![
                (Value::Number(1.0), "middle".into()),
                ("extra".into(), Value::Boolean(true)),
            ],
        }],
    };
    assert_eq!(sparse.to_json().unwrap(), json!([null, "middle", null]));
    let huge = StructuredValue {
        root: Value::Reference(0),
        objects: vec![Object::SparseArray {
            length: u32::MAX,
            properties: vec![],
        }],
    };
    assert!(huge.to_json().is_err());
    // An acyclic alias tree can expand exponentially despite a small graph.
    let mut exponential = StructuredValue {
        root: Value::Reference(0),
        objects: vec![],
    };
    for id in 0..30 {
        exponential.objects.push(Object::DenseArray {
            elements: vec![Value::Reference(id + 1), Value::Reference(id + 1)],
            properties: vec![],
        });
    }
    exponential.objects.push(Object::Object(vec![]));
    assert!(exponential.to_json().is_err());
}

#[test]
fn rpc_errors_preserve_family_cause_and_original_graph() {
    let bytes = cellar_native::rpc::encode_error(
        "SQL_TYPE_ERROR: unsupported",
        "Error",
        Some("unsupported"),
    )
    .unwrap();
    assert_eq!(bytes[..3], [0, 0xff, 15]);
    let error = cellar_native::rpc::decode_response(&bytes).unwrap_err();
    let remote = error
        .downcast_ref::<cellar_native::rpc::RemoteError>()
        .unwrap();
    assert_eq!(remote.message, "SQL_TYPE_ERROR: unsupported");
    let Object::Error {
        cause: Some(Value::Reference(cause)),
        ..
    } = remote.value.objects[1]
    else {
        panic!("remote error lost its cause")
    };
    assert!(
        matches!(&remote.value.objects[cause as usize], Object::Error { message: Some(message), .. } if String::from_utf16(message).unwrap() == "unsupported")
    );
    for bytes in [&[][..], &[0][..], &[1, 0xff, 15][..], &[2, 0xff, 15][..]] {
        assert!(cellar_native::rpc::decode_response(bytes).is_err());
    }
}
