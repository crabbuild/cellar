// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::{NativeCellConfiguration, RegistrationError};
use crate::application::{Application, Registry};

fn application(digest: [u8; 32]) -> anyhow::Result<Registry> {
    Registry::new(|_, _| Box::pin(async { Ok(crate::application::Output::Done) })).raft_workflow(
        "Orders",
        digest,
        |_, _, _| Box::pin(async { Ok(cellar_native::clone::StructuredValue::undefined()) }),
    )
}

fn options() -> crate::engine_api::WorkerConfigOptions {
    let mut options = NativeCellConfiguration::default().options("workflow-test");
    options
        .do_classes
        .push(crate::deploy::workflow_class("workflow-test"));
    options
        .workflow_bindings
        .push(crate::engine_api::WorkflowBinding {
            environment: "WF".into(),
            workflow: "orders".into(),
            class: "Orders".into(),
            settings: Default::default(),
        });
    options
}

#[test]
fn workflow_namespace_binds_code_and_admits_only_private_operator() -> anyhow::Result<()> {
    let first = application([1; 32])?;
    let second = application([2; 32])?;
    let options = options();
    first.validate_configuration(&options)?;
    second.validate_configuration(&options)?;
    let name = crate::deploy::workflow_class(&options.script_name);
    let class = first.resolve_raft_class(&name)?;
    assert_ne!(
        class.schema_digest(),
        second.resolve_raft_class(&name)?.schema_digest()
    );
    assert!(class.has_native_ingress());
    assert!(class.has_native_callbacks());
    assert!(class.binding_contract("__wfCreate").is_some());
    assert!(class
        .binding_contract(crate::application::workflow::OPERATOR)
        .is_none());
    assert!(class.workflow_operator_contract().is_some());
    for method in [
        "__wfStatus",
        "__wfHistory",
        "__wfStreamOpen",
        "__wfStreamRead",
    ] {
        let operation = crate::application::workflow::read_operation(method).unwrap();
        assert!(class.binding_contract(operation).is_none());
        assert_eq!(
            class.contract(operation).unwrap().kind,
            super::super::OperationKind::Query
        );
    }
    class.validate_shared_host(&Default::default())?;
    assert!(matches!(
        first.resolve_raft_class("Orders"),
        Err(RegistrationError::UnsupportedFactory(_))
    ));
    Ok(())
}

#[test]
fn workflow_declarations_are_canonical_and_fail_closed() -> anyhow::Result<()> {
    let mut options = options();
    let binding = options.workflow_bindings[0].clone();
    let mut alias = binding.clone();
    alias.environment = "ALIAS".into();
    options.workflow_bindings.push(alias);
    let first = application([1; 32])?;
    first.validate_configuration(&options)?;
    options.workflow_bindings.reverse();
    let second = application([1; 32])?;
    second.validate_configuration(&options)?;
    let name = crate::deploy::workflow_class(&options.script_name);
    assert_eq!(
        first.resolve_raft_class(&name)?.schema_digest(),
        second.resolve_raft_class(&name)?.schema_digest()
    );
    options.workflow_bindings[0].class = "Other".into();
    assert!(application([1; 32])?
        .validate_configuration(&options)
        .is_err());
    options.workflow_bindings = vec![binding.clone(), binding];
    assert!(application([1; 32])?
        .validate_configuration(&options)
        .is_err());
    options.workflow_bindings.truncate(1);
    options.workflow_bindings[0]
        .settings
        .schedules
        .push("* * * * *".into());
    assert!(application([1; 32])?
        .validate_configuration(&options)
        .is_err());
    assert!(application([0; 32]).is_err());
    Ok(())
}
