// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn protocol_refuses_unknown_fields_operations_and_range_shapes() {
    for (name, input) in [
        ("workers.r2.future", json!({"binding":"DATA","key":"k"})),
        (
            "workers.r2.head",
            json!({"binding":"DATA","key":"k","kind":"delete"}),
        ),
        (
            "workers.r2.head",
            json!({"binding":"DATA","key":"k","extra":1}),
        ),
        (
            "workers.r2.get",
            json!({"binding":"DATA","key":"k","range":{"offset":-1}}),
        ),
        (
            "workers.r2.get",
            json!({"binding":"DATA","key":"k","range":{"unknown":1}}),
        ),
        (
            "workers.r2.create",
            json!({"binding":"DATA","key":"k","options":{"ssecKey":"not supported"}}),
        ),
    ] {
        assert!(parse(name, input).is_err(), "{name}");
    }
    assert!(parse(
        "workers.r2.part",
        json!({"binding":"DATA","key":"k","upload_id":"id","number":1,"body":"AA=="})
    )
    .is_ok());
}

#[test]
fn metadata_refuses_unsupported_keys_and_preserves_actual_values() -> Result<()> {
    let invalid: Metadata = serde_json::from_value(json!({"httpMetadata":{"unsupported":"x"}}))?;
    assert!(invalid.options().is_err());
    let invalid: Metadata = serde_json::from_value(json!({"httpMetadata":{"contentType":42}}))?;
    assert!(invalid.options().is_err());
    let metadata: Metadata = serde_json::from_value(
        json!({"httpMetadata":{"contentType":"application/zstd","cacheExpiry":1234},"customMetadata":{"name":"backup 雪"}}),
    )?;
    let options = metadata.options()?;
    assert_eq!(
        options.http_metadata.content_type.as_deref(),
        Some("application/zstd")
    );
    assert_eq!(options.http_metadata.cache_expiry, Some(1234));
    assert_eq!(
        options.custom_metadata.get("name").map(String::as_str),
        Some("backup 雪")
    );
    Ok(())
}

#[test]
fn decoded_payload_capacity_follows_byte_slices() -> Result<()> {
    let capacity = Arc::new(tokio::sync::Semaphore::new(16));
    let lease = capacity.clone().try_acquire_many_owned(16)?;
    let bytes = bytes::Bytes::from_owner(PartBytes {
        bytes: vec![0, 255, 42],
        _capacity: Arc::new(lease),
    });
    let backend = bytes.slice(1..);
    drop(bytes);
    assert_eq!(backend.as_ref(), &[255, 42]);
    assert_eq!(capacity.available_permits(), 0);
    drop(backend);
    assert_eq!(capacity.available_permits(), 16);
    Ok(())
}
