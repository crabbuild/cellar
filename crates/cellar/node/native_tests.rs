// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::protocol::{NativeDeployment, PythonEnvironmentRef, FEATURE_EMBEDDED_PYTHON_V1};
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn runtime_inventory_changes_refuse_before_any_application_factory() -> anyhow::Result<()> {
    let executable = crate::native_deploy::current_executable_identity()?.clone();
    let config: crate::native_deploy::ApplicationConfig =
        serde_json::from_value(serde_json::json!({
            "script_name":"python-test", "application_version":"v1"
        }))?;
    let mut manifest = config.manifest(vec![executable.clone()])?;
    let reference = PythonEnvironmentRef {
        format: 1,
        target: executable.target.clone(),
        executable_sha256: executable.sha256.clone(),
        inventory_sha256: "a".repeat(64),
        archive_sha256: "b".repeat(64),
        archive_bytes: 1,
        prefix: "/opt/cellar-test/python".into(),
    };
    manifest
        .native
        .as_mut()
        .unwrap()
        .python
        .push(reference.clone());
    manifest
        .required_features
        .push(FEATURE_EMBEDDED_PYTHON_V1.into());
    let package = crate::native_deploy::Package {
        primary: manifest,
        cohosted: vec![],
    };
    let called = Arc::new(AtomicBool::new(false));
    let entered = called.clone();
    let applications =
        CompiledApplications::default().register("python-test", "v1", move || {
            entered.store(true, Ordering::Release);
            anyhow::bail!("factory reached")
        })?;
    assert!(applications
        .check_package(&package)
        .unwrap_err()
        .to_string()
        .contains("inventory differs"));
    assert!(!called.load(Ordering::Acquire));
    let wrong = applications
        .clone()
        .runtime_inventory(FEATURE_EMBEDDED_PYTHON_V1, "c".repeat(64))?;
    assert!(wrong
        .check_package(&package)
        .unwrap_err()
        .to_string()
        .contains("inventory differs"));
    assert!(!called.load(Ordering::Acquire));
    let matching = applications.runtime_inventory(FEATURE_EMBEDDED_PYTHON_V1, "a".repeat(64))?;
    assert!(matching
        .check_package(&package)
        .unwrap_err()
        .to_string()
        .contains("factory reached"));
    assert!(called.load(Ordering::Acquire));
    let mut changed: NativeDeployment = package.primary.native.clone().unwrap();
    changed.python[0].inventory_sha256 = "d".repeat(64);
    assert!(matching
        .validate_runtime_inventory(&changed, &executable)
        .is_err());
    changed.python.clear();
    assert!(matching
        .validate_runtime_inventory(&changed, &executable)
        .is_err());
    assert!(matching
        .runtime_inventory(FEATURE_EMBEDDED_PYTHON_V1, "a".repeat(64))
        .is_err());
    Ok(())
}

#[test]
fn node_runtime_inventory_changes_refuse_before_any_application_factory() -> anyhow::Result<()> {
    let executable = crate::native_deploy::current_executable_identity()?.clone();
    let config: crate::native_deploy::ApplicationConfig =
        serde_json::from_value(serde_json::json!({
            "script_name":"node-test", "application_version":"v1"
        }))?;
    let mut manifest = config.manifest(vec![executable.clone()])?;
    let reference = crate::protocol::NodeEnvironmentRef {
        format: 1,
        target: executable.target.clone(),
        executable_sha256: executable.sha256.clone(),
        inventory_sha256: "a".repeat(64),
        archive_sha256: "b".repeat(64),
        archive_bytes: 1,
        prefix: "/opt/cellar-test/node".into(),
    };
    manifest
        .native
        .as_mut()
        .unwrap()
        .node
        .push(reference.clone());
    manifest
        .required_features
        .push(crate::protocol::FEATURE_NODE_V1.into());
    let package = crate::native_deploy::Package {
        primary: manifest,
        cohosted: vec![],
    };
    let called = Arc::new(AtomicBool::new(false));
    let entered = called.clone();
    let applications = CompiledApplications::default().register("node-test", "v1", move || {
        entered.store(true, Ordering::Release);
        anyhow::bail!("factory reached")
    })?;
    assert!(applications
        .check_package(&package)
        .unwrap_err()
        .to_string()
        .contains("inventory differs"));
    assert!(!called.load(Ordering::Acquire));
    let wrong = applications
        .clone()
        .runtime_inventory(crate::protocol::FEATURE_NODE_V1, "c".repeat(64))?;
    assert!(wrong
        .check_package(&package)
        .unwrap_err()
        .to_string()
        .contains("inventory differs"));
    assert!(!called.load(Ordering::Acquire));
    let matching =
        applications.runtime_inventory(crate::protocol::FEATURE_NODE_V1, "a".repeat(64))?;
    assert!(matching
        .check_package(&package)
        .unwrap_err()
        .to_string()
        .contains("factory reached"));
    assert!(called.load(Ordering::Acquire));
    let mut changed: NativeDeployment = package.primary.native.clone().unwrap();
    changed.node[0].inventory_sha256 = "d".repeat(64);
    assert!(matching
        .validate_runtime_inventory(&changed, &executable)
        .is_err());
    changed.node.clear();
    assert!(matching
        .validate_runtime_inventory(&changed, &executable)
        .is_err());
    assert!(matching
        .runtime_inventory(crate::protocol::FEATURE_NODE_V1, "a".repeat(64))
        .is_err());
    Ok(())
}
