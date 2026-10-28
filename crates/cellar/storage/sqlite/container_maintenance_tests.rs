// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
use super::*;
use crate::application::containers::{BindingIntent, BindingObservation};

const SCOPE: &str = "Managed:one";

fn record() -> BindingRecord {
    BindingRecord {
        provider: "e2b".into(),
        configuration_digest: "a".repeat(64),
        parameters_digest: "b".repeat(64),
        generation: 1,
        request_id: "c".repeat(32),
        resource_id: None,
        intent: BindingIntent::Running,
        observation: BindingObservation::Pending,
    }
}

fn policy() -> MaintenancePolicy {
    MaintenancePolicy {
        inspect_interval_ms: 100,
        maximum_lifetime_ms: 2000,
        attempt_timeout_ms: 1000,
        idle_timeout_ms: Some(400),
    }
}

fn open(dir: &tempfile::TempDir, epoch: u64) -> anyhow::Result<String> {
    let path = dir
        .path()
        .join("cell.sqlite")
        .to_string_lossy()
        .into_owned();
    open_at_epoch(SCOPE, &path, epoch, true, None, false)?;
    Ok(path)
}

fn reserve(
    record: BindingRecord,
    expected: Option<u64>,
    now: i64,
) -> anyhow::Result<crate::application::containers::BindingCheckpoint> {
    super::super::container_bindings::compare_exchange(
        SCOPE,
        "container",
        expected,
        activation_epoch(SCOPE).unwrap(),
        record,
        Some(policy()),
        now,
    )
}

#[test]
fn reservation_idle_activity_and_completion_preserve_independent_deadlines() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let dir = tempfile::tempdir()?;
    let path = open(&dir, 1)?;
    set_alarm(SCOPE, 9000)?;
    reserve(record(), None, 1000)?;
    let first = list(SCOPE)?.remove(0);
    assert_eq!(
        (first.expires_ms, first.deadline()?, first.due_ms),
        (3000, 1400, 1100)
    );
    assert_eq!(get_alarm(SCOPE), Some(9000));
    assert_eq!(persisted_alarm(&path, SCOPE, None).unwrap().0, 1100);
    let claimed = claim(SCOPE, &first, 1100)?.unwrap();
    activity(SCOPE, "container", 1, 1300)?;
    assert!(claim(SCOPE, &first, 1400)?.is_none());
    finish(SCOPE, &claimed, false)?;
    let active = list(SCOPE)?.remove(0);
    assert_eq!(active.activity_ms, 1300);
    assert_eq!(active.deadline()?, 1700);
    assert_eq!(active.expires_ms, 3000);
    assert_eq!(active.failures, 0);
    assert!(!activity(SCOPE, "container", 1, 1700)?);
    assert_eq!(list(SCOPE)?, vec![active.clone()]);
    let claimed = claim(SCOPE, &active, 1400)?.unwrap();
    finish(SCOPE, &claimed, false)?;
    let failed = list(SCOPE)?.remove(0);
    assert_eq!(failed.failures, 1);
    finish(SCOPE, &claimed, true)?;
    assert_eq!(list(SCOPE)?, vec![failed]);
    close(SCOPE);
    Ok(())
}

#[test]
fn schedules_survive_business_clear_and_recovery_until_exact_retirement() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let dir = tempfile::tempdir()?;
    let path = open(&dir, 1)?;
    set_alarm(SCOPE, 9000)?;
    let initial = reserve(record(), None, 1000)?;
    let old = list(SCOPE)?.remove(0);
    delete_all_with_alarm(SCOPE, true)?;
    assert!(get_alarm(SCOPE).is_none());
    assert_eq!(list(SCOPE)?, vec![old.clone()]);
    close(SCOPE);
    assert_eq!(persisted_alarm(&path, SCOPE, None).unwrap().0, 1100);
    open(&dir, 2)?;
    assert_eq!(list(SCOPE)?, vec![old.clone()]);
    let mut retired = initial.record;
    retired.intent = BindingIntent::Deleted;
    retired.observation = BindingObservation::Confirmed;
    let retired = reserve(retired, Some(initial.revision), 1500)?;
    assert!(list(SCOPE)?.is_empty());
    let mut next = record();
    next.generation = 2;
    next.request_id = "d".repeat(32);
    reserve(next, Some(retired.revision), 2000)?;
    assert_eq!(list(SCOPE)?[0].generation, 2);
    let replacement = list(SCOPE)?;
    assert!(!activity(SCOPE, "container", 1, 2100)?);
    assert_eq!(list(SCOPE)?, replacement);
    finish(SCOPE, &old, false)?;
    assert_eq!(list(SCOPE)?[0].failures, 0);
    close(SCOPE);
    Ok(())
}

#[test]
fn failed_discovery_write_rolls_back_the_binding_and_schedule_marker() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let dir = tempfile::tempdir()?;
    open(&dir, 1)?;
    set_alarm(SCOPE, 9000)?;
    with(SCOPE,|c|as_framework_sql(||c.execute_batch("CREATE TRIGGER refuse_provider_work BEFORE INSERT ON __sys_work WHEN NEW.reason='resource-reconciliation' BEGIN SELECT RAISE(ABORT,'fixture refusal'); END;"))).unwrap()?;
    assert!(reserve(record(), None, 1000).is_err());
    assert!(super::super::container_bindings::load(SCOPE, "container")?.is_none());
    assert!(list(SCOPE)?.is_empty());
    assert_eq!(get_alarm(SCOPE), Some(9000));
    assert!(!with(SCOPE, |c| SCHEMA.check(c)).unwrap()?);
    close(SCOPE);
    Ok(())
}

#[test]
fn maintenance_is_private_and_recovery_rejects_changed_layouts() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let dir = tempfile::tempdir()?;
    open(&dir, 1)?;
    reserve(record(), None, 1000)?;
    with(SCOPE, |c| -> anyhow::Result<()> {
        for sql in [
            "SELECT * FROM __sys_container_binding_maintenance",
            "DELETE FROM __sys_container_binding_maintenance",
            "UPDATE __sys_container_binding_maintenance SET expires_ms=0",
            "DROP TABLE __sys_container_binding_maintenance_schema",
        ] {
            assert!(as_user_sql(|| c.execute_batch(sql)).is_err(), "{sql}");
        }
        as_framework_sql(|| -> anyhow::Result<()> {
            c.execute_batch("UPDATE __sys_container_binding_maintenance SET expires_ms=999999")?;
            assert!(check_schema(c).is_err());
            c.execute_batch("UPDATE __sys_container_binding_maintenance SET expires_ms=3000")?;
            check_schema(c)?;
            c.execute_batch("UPDATE __sys_container_binding_maintenance_schema SET version=2")?;
            assert!(check_schema(c).is_err());
            Ok(())
        })
    })
    .unwrap()?;
    close(SCOPE);
    Ok(())
}

#[test]
fn adopting_old_unsupervised_resources_does_not_grant_a_fresh_lifetime() -> anyhow::Result<()> {
    let cells = Cells::default();
    let _installed = cells.install();
    let dir = tempfile::tempdir()?;
    open(&dir, 1)?;
    let record = record();
    super::super::container_bindings::compare_exchange(
        SCOPE,
        "container",
        None,
        1,
        record.clone(),
        None,
        1000,
    )?;
    recover(SCOPE, "container", &record, policy(), 5000)?;
    let task = list(SCOPE)?.remove(0);
    assert_eq!((task.expires_ms, task.due_ms), (5000, 5000));
    recover(SCOPE, "container", &record, policy(), 6000)?;
    assert_eq!(list(SCOPE)?.remove(0), task);
    close(SCOPE);
    Ok(())
}
