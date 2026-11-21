// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn payload_transport_failure_keeps_archive_retryable() {
    let transient = || {
        CellGroupPayloadError::Storage(object_store::Error::Generic {
            store: "payload outage",
            source: "temporary GET failure".into(),
        })
    };
    for payload in [
        transient(),
        CellGroupPayloadError::Overloaded,
        CellGroupPayloadError::DeadlineExceeded,
        CellGroupPayloadError::Unconfirmed,
        CellGroupPayloadError::Conflict,
    ] {
        assert!(archive_retryable(&CellGroupArchiveError::Payload(payload)));
    }
    assert!(retryable(&Error::Archive(CellGroupArchiveError::Payload(
        transient()
    ))));
    assert!(retryable(&Error::Binding(
        CellGroupArchiveBindingError::Archive(CellGroupArchiveError::Payload(transient()))
    )));
}

#[test]
fn payload_integrity_and_ownership_failures_stop_archive() {
    for payload in [
        CellGroupPayloadError::Invalid("object digest or length"),
        CellGroupPayloadError::Invalid("namespace"),
        CellGroupPayloadError::Invalid("pool limits"),
        CellGroupPayloadError::Missing,
        CellGroupPayloadError::Retired,
    ] {
        let error = Error::Archive(CellGroupArchiveError::Payload(payload));
        assert!(!retryable(&error));
    }
}
