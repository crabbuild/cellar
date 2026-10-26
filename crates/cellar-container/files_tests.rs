// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::Validation;

#[test]
fn file_inputs_refuse_invalid_paths_bytes_modes_and_budgets_before_effects() {
    let limits = Limits { max_body_bytes: 3 };
    for (operation, input) in [
        (
            Operation::ReadFile,
            json!({"path":"relative","max_bytes":3}),
        ),
        (Operation::ReadFile, json!({"path":"/file","max_bytes":4})),
        (
            Operation::WriteFile,
            json!({"path":"/","data":"","mode":420}),
        ),
        (
            Operation::WriteFile,
            json!({"path":"/file","data":"!!!!","mode":420}),
        ),
        (
            Operation::WriteFile,
            json!({"path":"/file","data":"AAAAAA==","mode":420}),
        ),
        (
            Operation::WriteFile,
            json!({"path":"/file","data":"","mode":2048}),
        ),
        (Operation::StatFile, json!({"path":"/a/../file"})),
        (
            Operation::StatFile,
            json!({"path":"/file","host_path":"/secret"}),
        ),
        (
            Operation::ListFiles,
            json!({"path":"/dir","max_bytes":3,"max_entries":0}),
        ),
        (
            Operation::ListFiles,
            json!({"path":"/dir","max_bytes":3,"max_entries":4097}),
        ),
    ] {
        assert!(
            validate(operation, &input, &limits).is_err(),
            "{operation:?}: {input}"
        );
        assert!(!operation.is_read(), "file I/O must retain the output gate");
    }
    validate(
        Operation::WriteFile,
        &json!({"path":"/file","data":"AP8q","mode":511}),
        &limits,
    )
    .unwrap();
    validate(
        Operation::ReadFile,
        &json!({"path":"/file","max_bytes":0}),
        &limits,
    )
    .unwrap();
}

fn metadata() -> Value {
    json!({"name":"file","kind":"file","size":3,"mode":420,"modified_ms":null,"link_target":null})
}

#[test]
fn invalid_provider_metadata_has_unknown_outcome_without_refusal_provenance() {
    validate_metadata(&metadata(), 1024).unwrap();
    for (key, value) in [
        ("size", json!(9_007_199_254_740_992_u64)),
        ("modified_ms", json!(-9_007_199_254_740_992_i64)),
        ("mode", json!(0o4755)),
        ("kind", json!("socket")),
        ("name", json!("nul\0name")),
        ("undeclared", json!(true)),
    ] {
        let mut entry = metadata();
        entry[key] = value;
        let error = validate_metadata(&entry, 1024).unwrap_err();
        assert!(!error.is::<Validation>());
    }
    for key in ["modified_ms", "link_target"] {
        let mut value = metadata();
        value.as_object_mut().unwrap().remove(key);
        assert!(validate_metadata(&value, 1024).is_err());
    }
    assert!(validate_metadata(&metadata(), 1).is_err());
    assert!(crate::results::validate(Operation::ReadFile, &json!("AP8q"), 2).is_err());
    assert!(crate::results::validate(Operation::WriteFile, &json!(true), 1024).is_err());
}

#[test]
fn provider_listings_cannot_truncate_or_duplicate_direct_children() {
    validate_listing(&json!([metadata()]), 1024, 1).unwrap();
    assert!(validate_listing(&json!([metadata()]), 1024, 0).is_err());
    assert!(validate_listing(&json!([metadata(), metadata()]), 1024, 2).is_err());
    assert!(validate_listing(&json!([metadata()]), 2, 1).is_err());
    for name in ["", ".", "..", "sub/file"] {
        let mut entry = metadata();
        entry["name"] = json!(name);
        assert!(validate_listing(&json!([entry]), 1024, 1).is_err());
    }
}
