// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn filesystem_policy_binds_namespace_and_keeps_maintenance_private() -> anyhow::Result<()> {
    use crate::application::filesystem::Policy;
    let base = || RaftClass::new("Files", 1, [181; 32]);
    let writable = base()?.filesystem(Policy::default())?;
    let readonly = base()?.filesystem(Policy {
        read_only: true,
        ..Default::default()
    })?;
    let masked = base()?.filesystem(Policy {
        creation_mask: 0o077,
        ..Default::default()
    })?;
    assert_ne!(base()?.schema_digest(), writable.schema_digest());
    assert_ne!(readonly.schema_digest(), writable.schema_digest());
    assert_ne!(masked.schema_digest(), writable.schema_digest());
    assert!(writable
        .contract(super::super::native_work::OPERATION)
        .is_none());
    assert!(writable
        .system_contract(super::super::native_work::OPERATION)
        .is_some());
    assert!(writable.validate_cell_factory().is_err());
    assert!(base()?
        .filesystem(Policy {
            creation_mask: 0o1000,
            ..Default::default()
        })
        .is_err());
    assert!(base()?
        .filesystem(Policy::default())?
        .filesystem(Policy::default())
        .is_err());
    Ok(())
}

#[test]
fn initializer_contract_binds_schema_and_admits_shared_host() -> anyhow::Result<()> {
    let base = || RaftClass::new("Counter", 1, [81; 32]);
    let ordinary = base()?.command("Add", [82; 32], |_, _| Ok(vec![]))?;
    let initial =
        base()?
            .initializer([90; 32], |_, _| Ok(()))?
            .command("Add", [82; 32], |_, _| Ok(vec![]))?;
    let reordered = base()?
        .command("Add", [82; 32], |_, _| Ok(vec![]))?
        .initializer([90; 32], |_, _| Ok(()))?;
    let different =
        base()?
            .initializer([91; 32], |_, _| Ok(()))?
            .command("Add", [82; 32], |_, _| Ok(vec![]))?;
    assert_eq!(initial.schema_digest(), reordered.schema_digest());
    assert_ne!(initial.schema_digest(), ordinary.schema_digest());
    assert_ne!(initial.schema_digest(), different.schema_digest());
    ordinary.validate_shared_host(&Default::default())?;
    initial.validate_shared_host(&Default::default())?;
    assert!(base()?.initializer([0; 32], |_, _| Ok(())).is_err());
    assert!(base()?
        .initializer([90; 32], |_, _| Ok(()))?
        .initializer([90; 32], |_, _| Ok(()))
        .is_err());
    Ok(())
}

#[test]
fn named_binding_seal_is_private_and_binds_the_schema() -> anyhow::Result<()> {
    use super::super::native_configuration::{AdmittedCellConfiguration, NativeCellConfiguration};
    use crate::raft::execution::binding_seal;
    let plain = RaftClass::new("Counter", 1, [21; 32])?
        .json_cell_rpc("Add", [22; 32])?
        .with_cell_factory(Arc::new(|_, _| {
            anyhow::bail!("seal must never construct a cell")
        }))?;
    assert!(plain.system_contract(binding_seal::OPERATION).is_none());
    let mut options = NativeCellConfiguration::default().options("binding-test");
    options.do_classes = vec!["Counter".into()];
    let class = plain.configured_cell(&AdmittedCellConfiguration::from_options(&options)?);
    let seal = class.system_contract(binding_seal::OPERATION).unwrap();
    assert!(class.contract(binding_seal::OPERATION).is_none());
    assert!(class.validate_operation(&seal).is_err());
    for operation in [
        crate::storage::raft_binding_recovery::CLAIM,
        crate::storage::raft_binding_recovery::SETTLE,
    ] {
        assert!(plain.system_contract(operation).is_none());
        let contract = class.system_contract(operation).unwrap();
        assert!(class.contract(operation).is_none());
        assert!(class.validate_operation(&contract).is_err());
    }
    assert_ne!(class.schema_digest(), plain.schema_digest());
    assert!(class.has_system_commands());
    assert!(class.has_scheduled_work());
    Ok(())
}

#[test]
fn native_callbacks_are_private_and_require_checkpoint_capacity() -> anyhow::Result<()> {
    let class = RaftClass::new("AlarmOnly", 1, [51; 32])?
        .durable_callbacks()?
        .with_cell_factory(Arc::new(|_, _| {
            anyhow::bail!("admission must not construct a cell")
        }))?;
    let operation = super::super::native_work::OPERATION;
    assert!(class.contract(operation).is_none());
    assert!(class
        .validate_operation(&class.system_contract(operation).unwrap())
        .is_err());
    assert!(class.is_cell_rpc(operation));
    let mut host = crate::raft::host::RaftHostOptions::default();
    class.validate_shared_host(&host)?;
    host.execution.max_pending_transactions = 0;
    assert!(class.validate_shared_host(&host).is_err());
    Ok(())
}
