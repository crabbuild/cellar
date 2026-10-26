// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn requests_refuse_non_arrays_capabilities_and_excess_bytes() {
    assert!(request("method".into(), RpcData::Json("{}".into()), 128).is_err());
    assert!(request("method".into(), RpcData::Json("[]".into()), 1).is_err());
    let invalid = STANDARD.encode([0xff, 15, b'\\', 1]);
    assert!(arguments(&invalid, 128).is_err());
    let result = request("method".into(), RpcData::Json("[null,1]".into()), 128).unwrap();
    assert!(arguments(result["args"].as_str().unwrap(), 128).is_ok());
}

#[test]
fn completions_validate_envelopes_and_strip_nested_error_stacks() {
    let bytes = cellar::native::rpc::encode_error("outer", "TypeError", Some("inner")).unwrap();
    let mut graph = clone::decode(&bytes[1..]).unwrap();
    for object in &mut graph.objects {
        if let clone::Object::Error { stack, .. } = object {
            *stack = Some("private host path".encode_utf16().collect());
        }
    }
    let data = output(
        json!({"value": encoded(&graph, 4096).unwrap(), "error":true}),
        4096,
    )
    .unwrap();
    let RpcData::Structured(bytes) = data else {
        panic!()
    };
    let error = cellar::native::rpc::decode_response(&bytes).unwrap_err();
    let remote = error
        .downcast_ref::<cellar::native::rpc::RemoteError>()
        .unwrap();
    assert_eq!(remote.message, "outer");
    assert!(remote
        .value
        .objects
        .iter()
        .all(|object| !matches!(object, clone::Object::Error { stack: Some(_), .. })));
    assert!(output(
        json!({"value":STANDARD.encode([0xff, 15, b'_']), "error":true}),
        4096
    )
    .is_err());
    assert!(output(
        json!({"value":encoded(&graph, 4096).unwrap(), "error":true, "unknown":1}),
        4096
    )
    .is_err());
    assert!(output(
        json!({"value":encoded(&graph, 4096).unwrap(), "error":true}),
        8
    )
    .is_err());
}
