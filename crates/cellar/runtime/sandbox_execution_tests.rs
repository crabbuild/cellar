// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::runtime::rpc_authority::{self, RpcAuthority};

#[test]
fn sandbox_authority_is_host_minted_scope_bound_and_closed_to_handles() -> Result<()> {
    let authority = Authority::new(
        Identity {
            parent_scope: "Parent:one".into(),
            parent_incarnation: "a".repeat(32),
            name: "build".into(),
            generation: 1,
            execution_scope: "Child:one".into(),
            parameters_digest: "b".repeat(64),
            configuration_digest: "c".repeat(64),
        },
        Action::Start,
        true,
    )?;
    authority.validate_target("Child:one", METHOD)?;
    assert!(authority.validate_target("Child:two", METHOD).is_err());
    assert!(authority.validate_target("Child:one", "fetch").is_err());
    assert!(rpc_authority::validate_target(None, "Child:one", METHOD).is_err());
    let encoded = serde_json::to_value(RpcAuthority::Sandbox(authority))?;
    let decoded: RpcAuthority = serde_json::from_value(encoded.clone())?;
    decoded.validate_target("Child:one", METHOD)?;
    let mut forged = encoded;
    forged["action"] = json!({"action":"call","operation":"container.exec"});
    let decoded: RpcAuthority = serde_json::from_value(forged)?;
    assert!(decoded.validate().is_err());
    for operation in [
        "container.start",
        "container.destroy",
        "container.exec.write",
        "container.port.connect",
        "container.port.fetch_stream",
    ] {
        assert!(Action::Call {
            operation: operation.into()
        }
        .validate()
        .is_err());
    }
    Ok(())
}

#[test]
fn sandbox_parameter_identity_sorts_nested_objects_and_bounds_messages() -> Result<()> {
    use crate::application::sandbox_catalog::execution::digest;
    assert_eq!(
        digest(&serde_json::from_str("{\"b\":{\"d\":2,\"c\":1},\"a\":0}")?)?,
        digest(&json!({"a":0,"b":{"c":1,"d":2}}))?
    );
    assert!(digest(&json!({"data":"x".repeat(MAX_SANDBOX_MESSAGE_BYTES)})).is_err());
    Ok(())
}
