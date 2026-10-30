// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn initializer_contract_binds_schema_and_refuses_old_host() -> anyhow::Result<()> {
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
    ordinary.validate_host(&Default::default())?;
    assert!(initial.validate_host(&Default::default()).is_err());
    assert!(base()?.initializer([0; 32], |_, _| Ok(())).is_err());
    assert!(base()?
        .initializer([90; 32], |_, _| Ok(()))?
        .initializer([90; 32], |_, _| Ok(()))
        .is_err());
    Ok(())
}
