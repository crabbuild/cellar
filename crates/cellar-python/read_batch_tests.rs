// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde_json::json;

#[test]
fn admission_counts_supplied_keys_and_validates_the_entire_request() {
    for value in [
        Value::Null,
        json!({}),
        json!({"keys":"a"}),
        json!({"keys":["a", 1]}),
        json!({"keys":[], "extra":true}),
        json!({"keys":["a", "a", "a"]}),
    ] {
        assert!(keys(&value, 2).is_err(), "{value}");
    }
    assert!(keys(&json!({"keys":[]}), 0).unwrap().is_empty());
    assert_eq!(keys(&json!({"keys":["a", "a"]}), 2).unwrap().len(), 1);
    // Match single reads: write-only key restrictions do not apply here.
    assert_eq!(
        keys(&json!({"keys":["", "__sys_key"]}), 2).unwrap().len(),
        2
    );
}

#[test]
fn bounded_reply_preserves_missing_null_unicode_and_exact_size() {
    let request = json!({"keys":["missing", "null", "é\"", "null"]});
    let mut calls = 0;
    let reply = encode(keys(&request, 4).unwrap(), 4096, |key| {
        calls += 1;
        Ok(match key {
            "missing" => None,
            "null" => Some(Value::Null),
            _ => Some(json!("escaped\n☃")),
        })
    })
    .unwrap()
    .into_string();
    assert_eq!(calls, 3);
    let expected = json!({"ok":true,"value":{
        "missing":{"present":false,"value":null},
        "null":{"present":true,"value":null},
        "é\"":{"present":true,"value":"escaped\n☃"}
    }});
    assert_eq!(serde_json::from_str::<Value>(&reply).unwrap(), expected);
    for (limit, success) in [(reply.len(), true), (reply.len() - 1, false)] {
        let result = encode(keys(&request, 4).unwrap(), limit, |key| {
            Ok(if key == "missing" {
                None
            } else {
                Some(expected["value"][key]["value"].clone())
            })
        });
        assert_eq!(result.is_ok(), success);
    }
    let empty = encode(BTreeSet::new(), 22, |_| panic!("empty batch read")).unwrap();
    assert_eq!(empty.into_string(), "{\"ok\":true,\"value\":{}}");
}

#[test]
fn result_refusal_stops_collection_and_sanitizes_errors() {
    let request = json!({"keys":["a", "b"]});
    let mut calls = 0;
    let result = encode(keys(&request, 2).unwrap(), 128, |_| {
        calls += 1;
        Ok(Some(json!("secret".repeat(128))))
    });
    assert_eq!(calls, 1);
    let (encoded, body) = crate::failure::result(
        "storage.read_many",
        result.map(crate::operations::Output::ReadBatch),
        128,
    )
    .unwrap();
    assert!(body.is_none());
    assert_eq!(
        serde_json::from_str::<Value>(&encoded).unwrap(),
        json!({"ok":false,"error":"HostError","code":"HostError","outcome":"unknown"})
    );
}

#[test]
fn depth_includes_snapshot_and_result_map() {
    for (nesting, accepted) in [(62, true), (63, false)] {
        let mut value = Value::Null;
        for _ in 0..nesting {
            value = json!([value]);
        }
        let result = encode(BTreeSet::from(["key"]), 4096, |_| Ok(Some(value.take())));
        assert_eq!(result.is_ok(), accepted);
    }
}

#[test]
fn initial_storage_preserves_metadata_and_has_an_exact_combined_budget() {
    let payload =
        json!({"kind":"fetch","object":1,"event":{"url":"https://cell/é","body":""}}).to_string();
    let keys = vec!["missing".into(), "null".into(), "é\"".into()];
    let read = |key: &str| {
        Ok(match key {
            "missing" => None,
            "null" => Some(Value::Null),
            _ => Some(json!("escaped\n☃")),
        })
    };
    let encoded = append(payload.clone(), &keys, 4096, read).unwrap();
    let mut expected: Value = serde_json::from_str(&payload).unwrap();
    expected["initial_storage"] = json!({
        "missing":{"present":false,"value":null},
        "null":{"present":true,"value":null},
        "é\"":{"present":true,"value":"escaped\n☃"}
    });
    assert_eq!(serde_json::from_str::<Value>(&encoded).unwrap(), expected);
    assert_eq!(
        append(payload.clone(), &keys, encoded.len(), read).unwrap(),
        encoded
    );
    assert!(append(payload.clone(), &keys, encoded.len() - 1, read).is_err());
    let mut calls = 0;
    assert!(append(payload.clone(), &keys, payload.len(), |_| {
        calls += 1;
        Ok(None)
    })
    .is_err());
    assert_eq!(calls, 0, "refused metadata must not start storage reads");
    assert!(append(payload, &keys, 256, |_| {
        calls += 1;
        Ok(Some(json!("x".repeat(1024))))
    })
    .is_err());
    assert_eq!(calls, 1, "stop collecting at the first refused row");
}

#[test]
fn initial_storage_uses_the_batch_snapshot_depth_limit() {
    for (nesting, accepted) in [(62, true), (63, false)] {
        let mut value = Value::Null;
        for _ in 0..nesting {
            value = json!([value]);
        }
        let result = append("{\"kind\":\"fetch\"}".into(), &["key".into()], 4096, |_| {
            Ok(Some(value.take()))
        });
        assert_eq!(result.is_ok(), accepted);
    }
}
