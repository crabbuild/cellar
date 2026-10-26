// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::containers::{BindingIntent, BindingObservation};

fn pending() -> BindingRecord {
    BindingRecord {
        provider: "e2b".into(),
        configuration_digest: "a".repeat(64),
        parameters_digest: "d".repeat(64),
        generation: 1,
        request_id: "b".repeat(32),
        resource_id: None,
        intent: BindingIntent::Running,
        observation: BindingObservation::Pending,
    }
}

fn database() -> anyhow::Result<Connection> {
    let connection = Connection::open_in_memory()?;
    connection.execute_batch("BEGIN;")?;
    Ok(connection)
}

#[test]
fn revisions_preserve_unknown_creates_and_require_confirmed_retirement() -> anyhow::Result<()> {
    let c = database()?;
    let first = write(&c, "primary", None, 7, pending())?;
    assert_eq!(first.revision, 1);
    assert!(write(&c, "primary", None, 7, pending()).is_err());
    let mut changed_parameters = first.record.clone();
    changed_parameters.parameters_digest = "f".repeat(64);
    assert!(write(&c, "primary", Some(1), 7, changed_parameters).is_err());
    let mut next = first.record.clone();
    next.generation = 2;
    assert!(write(&c, "primary", Some(1), 7, next).is_err());
    let mut next = first.record.clone();
    next.observation = BindingObservation::Confirmed;
    assert!(write(&c, "primary", Some(1), 7, next.clone()).is_err());
    next.resource_id = Some("remote-id".into());
    let bound = write(&c, "primary", Some(1), 8, next)?;
    let mut changed = bound.record.clone();
    changed.resource_id = Some("foreign-id".into());
    assert!(write(&c, "primary", Some(2), 8, changed).is_err());
    let mut deleting = bound.record;
    deleting.intent = BindingIntent::Deleted;
    deleting.observation = BindingObservation::Pending;
    let deleting = write(&c, "primary", Some(2), 8, deleting)?;
    let mut confirmed = deleting.record;
    confirmed.observation = BindingObservation::Confirmed;
    write(&c, "primary", Some(3), 8, confirmed)?;
    let mut replacement = pending();
    replacement.generation = 2;
    assert!(write(&c, "primary", Some(4), 9, replacement.clone()).is_err());
    replacement.request_id = "c".repeat(32);
    let created = write(&c, "primary", Some(4), 9, replacement)?;
    assert_eq!((created.revision, created.record.generation), (5, 2));
    assert_eq!(read(&c, "primary")?, Some(created));
    Ok(())
}

#[test]
fn reserved_records_refuse_guest_sql_and_enforce_binding_capacity() -> anyhow::Result<()> {
    let c = database()?;
    for n in 0..MAX_BINDINGS {
        write(&c, &format!("binding-{n}"), None, 1, pending())?;
    }
    assert!(write(&c, "overflow", None, 1, pending()).is_err());
    c.authorizer(Some(authorize_sql))?;
    for sql in [
        "SELECT record FROM __sys_container_bindings",
        "DELETE FROM __sys_container_bindings",
        "DROP TABLE __sys_container_bindings",
    ] {
        assert!(as_user_sql(|| c.execute_batch(sql)).is_err(), "{sql}");
    }
    assert_eq!(read(&c, "binding-0")?.unwrap().record, pending());
    assert!(validate_name("../binding").is_err());
    assert!(validate_name(&"x".repeat(129)).is_err());
    Ok(())
}

#[test]
fn unmarked_future_and_corrupt_records_refuse_schema_open_before_mutation() -> anyhow::Result<()> {
    for mutation in [
        "DROP TABLE __sys_container_binding_schema",
        "UPDATE __sys_container_binding_schema SET version=2",
        "UPDATE __sys_container_bindings SET record='{}'",
        "ALTER TABLE __sys_container_bindings ADD COLUMN legacy TEXT",
    ] {
        let c = database()?;
        write(&c, "primary", None, 1, pending())?;
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

#[test]
fn savepoints_support_autocommit_nested_batches_and_preserve_original_failures(
) -> anyhow::Result<()> {
    let c = Connection::open_in_memory()?;
    assert!(transaction(&c, "primary", Some(1), 1, pending()).is_err());
    assert!(!SCHEMA.check(&c)? && c.is_autocommit());
    let first = transaction(&c, "primary", None, 1, pending())?;
    assert!(c.is_autocommit());
    assert!(transaction(&c, "primary", None, 1, pending()).is_err());
    assert_eq!(read(&c, "primary")?, Some(first));
    assert!(c.is_autocommit());
    c.execute_batch("BEGIN;")?;
    transaction(&c, "nested", None, 1, pending())?;
    assert!(!c.is_autocommit());
    c.execute_batch("ROLLBACK;")?;
    assert!(read(&c, "nested")?.is_none());
    c.execute_batch("CREATE TRIGGER reject_binding BEFORE INSERT ON __sys_container_bindings BEGIN SELECT RAISE(ROLLBACK,'binding write interrupted'); END;")?;
    let error = transaction(&c, "failed", None, 1, pending()).unwrap_err();
    assert!(error.downcast_ref::<rusqlite::Error>().is_some());
    assert!(error.to_string().contains("binding write interrupted"));
    assert!(c.is_autocommit());
    Ok(())
}
