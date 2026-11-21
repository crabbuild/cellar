// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Provider deadlines share native wake discovery, never the application alarm.
use super::*;
use crate::application::consensus::Durability;
use crate::application::containers::{BindingRecord, MaintenancePolicy, MAX_BINDINGS};
use cellar_logic::work::Reason;

const TABLE: &str = "__sys_container_binding_maintenance";
const SCHEMA: event_schema::Schema = event_schema::Schema {
    marker: "__sys_container_binding_maintenance_schema",
    ddl: include_str!("container_maintenance.sql"),
    required: &[(TABLE, "binding,generation,version,created_ms,activity_ms,logical_ms,expires_ms,due_ms,policy,failed,minimum_durability")],
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Task {
    pub binding: String,
    pub generation: u64,
    pub version: u64,
    pub created_ms: i64,
    pub activity_ms: i64,
    pub logical_ms: i64,
    pub expires_ms: i64,
    pub due_ms: i64,
    pub policy: MaintenancePolicy,
    pub failures: u32,
    pub minimum: Durability,
}

impl Task {
    pub fn deadline(&self) -> anyhow::Result<i64> {
        Ok(self
            .policy
            .idle_timeout_ms
            .map(|ms| add(self.activity_ms, ms))
            .transpose()?
            .map_or(self.expires_ms, |idle| idle.min(self.expires_ms)))
    }
}

fn add(now: i64, millis: u64) -> anyhow::Result<i64> {
    now.checked_add(i64::try_from(millis)?)
        .ok_or_else(|| anyhow::anyhow!("provider deadline exhausted"))
}

fn decode(row: &rusqlite::Row<'_>) -> rusqlite::Result<Task> {
    let minimum = match row.get::<_, u8>(10)? {
        0 => Durability::Quorum,
        1 => Durability::S3,
        other => return Err(rusqlite::Error::IntegralValueOutOfRange(10, other.into())),
    };
    let policy: String = row.get(8)?;
    let policy = serde_json::from_str::<MaintenancePolicy>(&policy)
        .and_then(|policy| policy.validate().map_err(serde::de::Error::custom))
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                8,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    Ok(Task {
        binding: row.get(0)?,
        generation: row.get(1)?,
        version: row.get(2)?,
        created_ms: row.get(3)?,
        activity_ms: row.get(4)?,
        logical_ms: row.get(5)?,
        expires_ms: row.get(6)?,
        due_ms: row.get(7)?,
        policy,
        failures: row.get(9)?,
        minimum,
    })
}

fn tasks(connection: &Connection) -> anyhow::Result<Vec<Task>> {
    if !SCHEMA.check(connection)? {
        return Ok(Vec::new());
    }
    let tasks = connection
        .prepare_cached(
            "SELECT binding,generation,version,created_ms,activity_ms,logical_ms,
                    expires_ms,due_ms,policy,failed,minimum_durability
             FROM __sys_container_binding_maintenance ORDER BY due_ms,binding LIMIT 65",
        )?
        .query_map([], decode)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    event_schema::compatible(
        tasks.len() <= MAX_BINDINGS,
        "provider maintenance count exceeds limit",
    )?;
    Ok(tasks)
}

pub(super) fn check_schema(connection: &Connection) -> anyhow::Result<()> {
    if !SCHEMA.check(connection)? {
        return Ok(());
    }
    let tasks = tasks(connection)?;
    for task in tasks {
        super::container_bindings::validate_name(&task.binding)?;
        event_schema::compatible(
            task.generation > 0
                && task.version > 0
                && task.created_ms >= 0
                && task.activity_ms >= task.created_ms
                && task.logical_ms >= task.activity_ms
                && task.expires_ms >= task.created_ms
                && task.expires_ms <= add(task.created_ms, task.policy.maximum_lifetime_ms)?
                && task.due_ms >= 0
                && task.failures <= 16,
            "invalid provider maintenance fields",
        )?;
        let record = super::container_bindings::read(connection, &task.binding)?
            .ok_or_else(|| anyhow::anyhow!("maintenance has no retained binding"))?;
        event_schema::compatible(
            record.record.generation == task.generation,
            "maintenance does not name a retained binding generation",
        )?;
    }
    Ok(())
}

fn rearm(connection: &Connection, scope: &str) -> anyhow::Result<()> {
    let shared = scope == RAFT_STORAGE_SCOPE;
    if shared {
        connection.execute_batch(super::work::SCHEMA)?;
    }
    let next: Option<i64> = connection.query_row(
        "SELECT min(due_ms) FROM __sys_container_binding_maintenance",
        [],
        |row| row.get(0),
    )?;
    let current: Option<Option<i64>> = connection
        .query_row(
            "SELECT at_ms FROM __sys_work WHERE scope=?1 AND reason='resource-reconciliation'",
            [scope],
            |row| row.get(0),
        )
        .optional()?;
    if next == current.flatten() {
        if shared {
            if let Some(next) = next {
                crate::application::consensus::native_work::arm_maintenance(
                    connection,
                    "resource-reconciliation",
                    next,
                )?;
            } else {
                crate::application::consensus::native_work::cancel(
                    connection,
                    "resource-reconciliation",
                )?;
            }
        }
        return Ok(());
    }
    if let Some(next) = next {
        super::work::set_in(connection, scope, Reason::ResourceReconciliation, next)?;
    } else {
        connection.execute(
            "UPDATE __sys_work SET at_ms=NULL
             WHERE scope=?1 AND reason='resource-reconciliation' AND at_ms IS NOT NULL",
            [scope],
        )?;
    }
    if shared {
        // Both ledgers belong to this same private transaction. A claimed wake
        // gets a new generation, so its completion cannot erase a replacement.
        if let Some(next) = next {
            crate::application::consensus::native_work::arm_maintenance(
                connection,
                "resource-reconciliation",
                next,
            )?;
        } else {
            crate::application::consensus::native_work::cancel(
                connection,
                "resource-reconciliation",
            )?;
        }
    }
    Ok(())
}

fn save(connection: &Connection, task: &Task) -> anyhow::Result<()> {
    let policy = serde_json::to_string(&task.policy)?;
    let minimum = match task.minimum {
        Durability::Quorum => 0,
        Durability::S3 => 1,
    };
    connection.execute(
        "INSERT INTO __sys_container_binding_maintenance
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
         ON CONFLICT(binding) DO UPDATE SET
             generation=excluded.generation,version=excluded.version,
             created_ms=excluded.created_ms,activity_ms=excluded.activity_ms,
             logical_ms=excluded.logical_ms,expires_ms=excluded.expires_ms,
             due_ms=excluded.due_ms,policy=excluded.policy,failed=excluded.failed,
             minimum_durability=excluded.minimum_durability",
        rusqlite::params![
            task.binding,
            task.generation,
            task.version,
            task.created_ms,
            task.activity_ms,
            task.logical_ms,
            task.expires_ms,
            task.due_ms,
            policy,
            task.failures,
            minimum
        ],
    )?;
    Ok(())
}

pub(super) fn changed(
    connection: &Connection,
    scope: &str,
    binding: &str,
    record: &BindingRecord,
    policy: Option<MaintenancePolicy>,
    now: i64,
    minimum: Durability,
) -> anyhow::Result<()> {
    let present = SCHEMA.check(connection)?;
    if !present && policy.is_none() {
        return Ok(());
    }
    SCHEMA.open(connection)?;
    let prior = tasks(connection)?
        .into_iter()
        .find(|task| task.binding == binding);
    if let Some(prior) = &prior {
        anyhow::ensure!(
            prior.generation == record.generation
                || (scope == RAFT_STORAGE_SCOPE
                    && prior.generation.checked_add(1) == Some(record.generation)
                    && !record.is_retired()),
            "maintenance generation changed without retirement"
        );
    }
    if record.is_retired() && scope != RAFT_STORAGE_SCOPE {
        connection.execute(
            "DELETE FROM __sys_container_binding_maintenance WHERE binding=?1 AND generation=?2",
            rusqlite::params![binding, record.generation],
        )?;
    } else if let Some(mut task) = prior.filter(|task| task.generation == record.generation) {
        // A shared checkpoint can commit retirement before its S3 proof returns.
        // Keep this bounded obligation discoverable until the exact retired
        // checkpoint is proven. A successor confirms it without provider work.
        task.version = task
            .version
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("maintenance revision exhausted"))?;
        task.logical_ms = task.logical_ms.max(now);
        task.minimum = task.minimum.max(minimum);
        if record.intent == crate::application::containers::BindingIntent::Deleted {
            task.due_ms = task.logical_ms;
        }
        save(connection, &task)?;
    } else if let Some(policy) = policy.filter(|_| !record.is_retired()) {
        let policy = policy.validate()?;
        let mut task = Task {
            binding: binding.into(),
            generation: record.generation,
            version: 1,
            created_ms: now,
            activity_ms: now,
            logical_ms: now,
            expires_ms: add(now, policy.maximum_lifetime_ms)?,
            due_ms: now,
            policy,
            failures: 0,
            minimum,
        };
        task.due_ms = add(now, policy.inspect_interval_ms)?.min(task.deadline()?);
        save(connection, &task)?;
    }
    rearm(connection, scope)
}

pub(super) fn invocation_minimum(scope: &str) -> anyhow::Result<Durability> {
    if scope == RAFT_STORAGE_SCOPE {
        Ok(crate::storage::capture_raft_dispatch()?
            .ok_or(crate::application::consensus::CommandFailure::Unavailable)?
            .binding_source()?
            .durability)
    } else {
        Ok(Durability::Quorum)
    }
}

/// Read the coordinator's next obligation's retained source policy. A caller's
/// explicit S3 minimum cannot be replaced by a weaker namespace default.
pub(crate) fn next_minimum(connection: &Connection) -> anyhow::Result<Durability> {
    Ok(tasks(connection)?
        .first()
        .map_or(Durability::Quorum, |task| task.minimum))
}

fn confirm_retired_in(
    connection: &Connection,
    scope: &str,
    binding: &str,
    expected: &crate::application::containers::BindingCheckpoint,
) -> anyhow::Result<bool> {
    anyhow::ensure!(
        scope == RAFT_STORAGE_SCOPE,
        "retirement confirmation requires shared storage"
    );
    if !expected.record.is_retired()
        || super::container_bindings::read(connection, binding)?.as_ref() != Some(expected)
        || !SCHEMA.check(connection)?
    {
        return Ok(false);
    }
    let changed = connection.execute(
        "DELETE FROM __sys_container_binding_maintenance WHERE binding=?1 AND generation=?2",
        rusqlite::params![binding, expected.record.generation],
    )? != 0;
    rearm(connection, scope)?;
    Ok(changed)
}

/// The caller owns a current proof for this exact retired checkpoint. Removal
/// remains private until its ordinary result/checkpoint proves the cleanup.
pub(crate) fn confirm_retired(
    scope: &str,
    binding: &str,
    expected: &crate::application::containers::BindingCheckpoint,
) -> anyhow::Result<bool> {
    mutate(scope, |connection| {
        confirm_retired_in(connection, scope, binding, expected)
    })
}

pub(crate) fn list(scope: &str) -> anyhow::Result<Vec<Task>> {
    require_sql_healthy(scope).map_err(anyhow::Error::msg)?;
    with(scope, |c| as_framework_sql(|| tasks(c)))
        .ok_or_else(|| anyhow::anyhow!("cell storage is not local"))?
}

pub(crate) fn recover(
    scope: &str,
    binding: &str,
    record: &BindingRecord,
    policy: MaintenancePolicy,
    now: i64,
) -> anyhow::Result<()> {
    let minimum = invocation_minimum(scope)?;
    mutate(scope, |connection| {
        if !tasks(connection)?
            .iter()
            .any(|task| task.binding == binding)
            && !record.is_retired()
        {
            changed(
                connection,
                scope,
                binding,
                record,
                Some(policy),
                now,
                minimum,
            )?;
            // A prior unmanaged reservation has no provable start time.
            // Do not grant a new lifetime merely by enabling supervision.
            connection.execute(
                "UPDATE __sys_container_binding_maintenance
                 SET expires_ms=?2,due_ms=?2 WHERE binding=?1",
                rusqlite::params![binding, now],
            )?;
            rearm(connection, scope)?;
        }
        Ok(())
    })
}

pub(crate) fn claim(scope: &str, expected: &Task, now: i64) -> anyhow::Result<Option<Task>> {
    mutate(scope, |c| {
        let current = tasks(c)?
            .into_iter()
            .find(|task| task.binding == expected.binding);
        if current.as_ref() != Some(expected) {
            return Ok(None);
        }
        let mut task = expected.clone();
        task.logical_ms = task.logical_ms.max(now);
        task.version = task
            .version
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("maintenance revision exhausted"))?;
        task.due_ms = add(task.logical_ms, task.policy.inspect_interval_ms)?;
        let deadline = task.deadline()?;
        if deadline > task.logical_ms {
            task.due_ms = task.due_ms.min(deadline);
        }
        save(c, &task)?;
        rearm(c, scope)?;
        Ok(Some(task))
    })
}

pub(crate) fn finish(scope: &str, expected: &Task, success: bool) -> anyhow::Result<()> {
    mutate(scope, |c| {
        let current = tasks(c)?
            .into_iter()
            .find(|task| task.binding == expected.binding);
        if current.as_ref() != Some(expected) {
            return Ok(());
        }
        let mut task = expected.clone();
        task.version = task
            .version
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("maintenance revision exhausted"))?;
        task.failures = if success {
            0
        } else {
            (task.failures + 1).min(16)
        };
        if !success {
            let delay = task
                .policy
                .inspect_interval_ms
                .saturating_mul(1 << task.failures)
                .min(60_000);
            task.due_ms = add(task.logical_ms, delay)?;
            let deadline = task.deadline()?;
            if deadline > task.logical_ms {
                task.due_ms = task.due_ms.min(deadline);
            }
        }
        save(c, &task)?;
        rearm(c, scope)
    })
}

pub(crate) fn activity(
    scope: &str,
    binding: &str,
    generation: u64,
    now: i64,
) -> anyhow::Result<bool> {
    mutate(scope, |c| {
        let Some(mut task) = tasks(c)?.into_iter().find(|task| task.binding == binding) else {
            return Ok(false);
        };
        if task.generation != generation || task.deadline()? <= task.logical_ms.max(now) {
            // A late completion may report its result, but cannot revive an
            // already expired generation or postpone the cleanup obligation.
            return Ok(false);
        }
        task.logical_ms = task.logical_ms.max(now);
        task.activity_ms = task.logical_ms;
        task.version = task
            .version
            .checked_add(1)
            .filter(|v| *v <= i64::MAX as u64)
            .ok_or_else(|| anyhow::anyhow!("maintenance revision exhausted"))?;
        task.due_ms = task.due_ms.min(task.deadline()?);
        save(c, &task)?;
        rearm(c, scope)?;
        Ok(true)
    })
}

pub(crate) fn publish(scope: &str) -> Option<i64> {
    super::publish_alarm(scope)
}

fn mutate<T>(
    scope: &str,
    write: impl FnOnce(&Connection) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    with_application_storage_write(scope, |connection| {
        as_framework_sql(|| {
            as_storage_transaction(|| {
                connection.execute_batch("SAVEPOINT cellar_provider_maintenance;")
            })?;
            let result = write(connection);
            let cleanup = if result.is_err() && connection.is_autocommit() {
                Ok(())
            } else {
                as_storage_transaction(|| {
                    if result.is_err() {
                        connection.execute_batch("ROLLBACK TO cellar_provider_maintenance;")?;
                    }
                    connection.execute_batch("RELEASE cellar_provider_maintenance;")
                })
            };
            match (result, cleanup) {
                (Ok(value), Ok(())) => Ok(value),
                (Err(error), Ok(())) => Err(error),
                (Ok(_), Err(error)) => Err(error.into()),
                (Err(error), Err(cleanup)) => {
                    Err(error.context(format!("maintenance savepoint cleanup failed: {cleanup}")))
                }
            }
        })
    })
}

pub(crate) fn refresh(scope: &str) -> anyhow::Result<()> {
    mutate(scope, |c| rearm(c, scope))
}

#[cfg(test)]
#[path = "container_maintenance_tests.rs"]
mod tests;
