// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::containers::{BindingIntent, BindingObservation, BindingRecord};

fn database() -> anyhow::Result<(Connection, BindingCheckpoint)> {
    let c = Connection::open_in_memory()?;
    let record = BindingRecord {
        provider: "docker".into(),
        configuration_digest: "a".repeat(64),
        parameters_digest: "b".repeat(64),
        generation: 1,
        request_id: "c".repeat(32),
        resource_id: None,
        intent: BindingIntent::Running,
        observation: BindingObservation::Pending,
    };
    let first = super::super::container_bindings::transaction(&c, "tool", None, 1, record)?;
    c.execute_batch("BEGIN;")?;
    Ok((c, first))
}

#[test]
fn claims_survive_owner_changes_rollback_and_binding_generations() -> anyhow::Result<()> {
    let (c, first) = database()?;
    let a = reserve(&c, "tool", &first, &"a".repeat(64))?;
    assert_eq!(a.sequence, 1);
    assert_eq!(reserve(&c, "tool", &first, &a.owner)?, a);
    c.execute_batch("SAVEPOINT claim;")?;
    let b = reserve(&c, "tool", &first, &"b".repeat(64))?;
    assert_eq!(b.sequence, 2);
    c.execute_batch("ROLLBACK TO claim; RELEASE claim;")?;
    assert_eq!(read(&c, "tool")?, Some(a.clone()));
    assert_eq!(reserve(&c, "tool", &first, &b.owner)?, b);
    // An unproven rollback never permits controller I/O. Once proven, even a
    // return to an earlier owner's digest must allocate a larger sequence.
    assert_eq!(reserve(&c, "tool", &first, &a.owner)?.sequence, 3);
    let mut confirmed = first.record.clone();
    confirmed.resource_id = Some("original-resource".into());
    confirmed.observation = BindingObservation::Confirmed;
    let confirmed =
        super::super::container_bindings::transaction(&c, "tool", Some(1), 1, confirmed)?;
    let updated = reserve(&c, "tool", &confirmed, &a.owner)?;
    assert_eq!((updated.sequence, updated.binding_revision), (4, 2));
    assert!(reserve(&c, "tool", &first, &a.owner).is_err());
    let mut retired = confirmed.record;
    retired.intent = BindingIntent::Deleted;
    retired.observation = BindingObservation::Confirmed;
    let retired = super::super::container_bindings::transaction(&c, "tool", Some(2), 1, retired)?;
    assert!(reserve(&c, "tool", &first, &a.owner).is_err());
    assert!(reserve(&c, "tool", &retired, &a.owner).is_err());
    let mut next = first.record.clone();
    next.generation = 2;
    next.request_id = "d".repeat(32);
    let next = super::super::container_bindings::transaction(&c, "tool", Some(3), 1, next)?;
    let replacement = reserve(&c, "tool", &next, &a.owner)?;
    assert_eq!((replacement.sequence, replacement.generation), (5, 2));
    check_schema(&c)?;
    Ok(())
}

#[test]
fn execution_claims_refuse_forgery_exhaustion_and_corrupt_restoration() -> anyhow::Result<()> {
    let (c, first) = database()?;
    reserve(&c, "tool", &first, &"a".repeat(64))?;
    c.authorizer(Some(authorize_sql))?;
    for sql in [
        "SELECT * FROM __sys_container_execution",
        "DELETE FROM __sys_container_execution",
        "DROP TABLE __sys_container_execution",
    ] {
        assert!(as_user_sql(|| c.execute_batch(sql)).is_err(), "{sql}");
    }
    c.execute(
        "UPDATE __sys_container_execution SET sequence=?1",
        [i64::MAX],
    )?;
    assert!(reserve(&c, "tool", &first, &"b".repeat(64)).is_err());
    assert_eq!(read(&c, "tool")?.unwrap().sequence, i64::MAX as u64);
    for mutation in [
        "DROP TABLE __sys_container_execution_schema",
        "UPDATE __sys_container_execution_schema SET version=2",
        "UPDATE __sys_container_execution SET owner=replace(owner,'a','z')",
        "UPDATE __sys_container_execution SET generation=2",
        "UPDATE __sys_container_execution SET binding_revision=2",
        "UPDATE __sys_container_execution SET binding='orphan'",
        "ALTER TABLE __sys_container_execution ADD COLUMN legacy TEXT",
    ] {
        let (c, first) = database()?;
        reserve(&c, "tool", &first, &"a".repeat(64))?;
        c.execute_batch(mutation)?;
        let before: i64 = c.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        assert!(super::super::schema(&c).is_err(), "{mutation}");
        assert_eq!(
            c.query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0))?,
            before
        );
    }
    Ok(())
}
