// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn pending() -> Report {
    serde_json::from_value(
        json!({"pending_tasks":1,"pending_objects":0,"pending_threads":0,
        "objects_created":0,"objects_retired":0,"peak_tasks":1,"budget_exhaustions":0,
        "runtime_failed":false,"bridge":cellar_python::BridgeReport::default(),
        "work":{"active_tasks":1,"active_invocations":1,
        "resident_objects":0,"pending_retirements":0}}),
    )
    .unwrap()
}

#[test]
fn supervision_requires_the_single_known_guest_and_no_native_work() {
    assert!(validate_incomplete(&pending()).is_ok());
    for field in [
        "pending_tasks",
        "native",
        "bytes",
        "reservations",
        "operations",
        "host",
        "runtime",
        "threads",
        "invocations",
        "objects",
        "retirements",
        "unknown_task",
        "unraisable",
        "thread_error",
    ] {
        let mut report = pending();
        match field {
            "pending_tasks" => report.pending_tasks = 0,
            "native" => report.bridge.native_invocations = 1,
            "bytes" => report.bridge.queued_bytes = 1,
            "reservations" => report.bridge.pending_reservations = 1,
            "operations" => report.bridge.operations.pending = 1,
            "host" => report.host_failed = true,
            "runtime" => report.runtime_failed = true,
            "threads" => report.pending_threads = 1,
            "invocations" => report.work.active_invocations = 0,
            "objects" => report.pending_objects = 1,
            "retirements" => report.work.pending_retirements = 1,
            "unknown_task" => report.work.active_tasks = 2,
            "unraisable" => report.unraisable_errors = 1,
            "thread_error" => report.thread_errors = 1,
            _ => unreachable!(),
        }
        assert!(
            validate_incomplete(&report).is_err(),
            "accepted unknown cleanup failure: {field}"
        );
    }
}

#[test]
fn a_generic_child_failure_cannot_publish_an_expected_incomplete_report() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("report.json");
    let (_, receiver) = std::sync::mpsc::channel();
    assert!(finish(
        Err(anyhow::anyhow!("unrelated failure")),
        receiver,
        Some(path.clone())
    )
    .is_err());
    assert!(!path.exists());
    let (_, receiver) = std::sync::mpsc::channel();
    assert!(finish(Ok(pending()), receiver, Some(path.clone())).is_err());
    assert!(!path.exists());
}
