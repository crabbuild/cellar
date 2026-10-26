// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn protobuf_metadata_preserves_numeric_enums_int64_strings_and_nullable_fields() {
    let value =
        metadata(&json!({"name":"binary","type":1,"size":"9007199254740991","mode":0o100640}))
            .unwrap();
    assert_eq!(value["kind"], "file");
    assert_eq!(value["size"], 9_007_199_254_740_991u64);
    assert_eq!(value["mode"], 0o640);
    assert!(value["modified_ms"].is_null() && value["link_target"].is_null());
    let link=metadata(&json!({"name":"link","type":"FILE_TYPE_FILE","isSymlink":true,"symlinkTarget":"target","modifiedTime":"2026-10-25T00:00:00Z"})).unwrap();
    assert_eq!(link["kind"], "symlink");
    assert_eq!(link["link_target"], "target");
    assert!(link["modified_ms"].as_i64().is_some());
    for invalid in [
        json!({"name":"bad","type":4}),
        json!({"name":"bad","size":"-1"}),
        json!({"name":"bad","modifiedTime":"invalid"}),
        json!({"name":"bad","modifiedTime":1}),
        json!({"name":"bad","mode":4294967296u64}),
        json!({"name":"bad","isSymlink":"true"}),
        json!({"name":"bad","symlinkTarget":1}),
        json!({"name":"../bad"}),
    ] {
        assert!(metadata(&invalid).is_err());
    }
}
