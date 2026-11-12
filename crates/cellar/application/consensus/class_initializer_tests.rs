// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

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
