// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

fn empty() -> Report {
    serde_json::from_value(serde_json::json!({
        "pending_tasks":0,"pending_objects":0,"pending_threads":0,
        "objects_created":0,"objects_retired":0,"peak_tasks":0,
        "budget_exhaustions":0,"runtime_failed":false,
        "bridge":BridgeReport::default()
    }))
    .unwrap()
}

#[test]
fn native_admission_cannot_be_hidden_by_empty_python_registries() {
    let mut report = empty();
    assert!(report.complete());
    report.bridge.native_invocations = 1;
    assert!(!report.complete(), "native invocation still owns admission");
}

#[test]
fn bridge_reports_require_the_native_admission_observation() {
    let mut value = serde_json::to_value(BridgeReport::default()).unwrap();
    value.as_object_mut().unwrap().remove("native_invocations");
    assert!(serde_json::from_value::<BridgeReport>(value).is_err());

    let mut value = serde_json::to_value(empty()).unwrap();
    value.as_object_mut().unwrap().remove("bridge");
    assert!(
        serde_json::from_value::<Report>(value).is_err(),
        "missing bridge evidence cannot default to a clean drain"
    );
}
