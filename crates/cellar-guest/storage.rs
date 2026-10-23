use anyhow::{ensure, Context as _, Result};
use cellar::sdk::Context;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    present: bool,
    #[serde(default)]
    value: Value,
}

#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct Batch {
    checks: BTreeMap<String, Snapshot>,
    puts: BTreeMap<String, Value>,
    deletes: Vec<String>,
    alarm_at_ms: Option<i64>,
    delete_alarm: bool,
}

#[derive(Debug)]
pub struct Conflict;

impl std::fmt::Display for Conflict {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("storage check failed")
    }
}

impl std::error::Error for Conflict {}

fn validate(value: Value, maximum: usize) -> Result<Batch> {
    let batch: Batch = serde_json::from_value(value)?;
    ensure!(
        batch
            .checks
            .len()
            .saturating_add(batch.puts.len())
            .saturating_add(batch.deletes.len())
            .saturating_add(usize::from(
                batch.alarm_at_ms.is_some() || batch.delete_alarm
            ))
            <= maximum,
        "guest storage batch exceeds operation limit"
    );
    ensure!(
        !(batch.alarm_at_ms.is_some() && batch.delete_alarm),
        "conflicting alarm operations"
    );
    ensure!(
        batch.alarm_at_ms.is_none_or(|time| time >= 0),
        "alarm time must be nonnegative"
    );
    let mut deleted = HashSet::new();
    for key in &batch.deletes {
        ensure!(
            deleted.insert(key) && !batch.puts.contains_key(key),
            "duplicate storage mutation"
        );
    }
    for key in batch
        .checks
        .keys()
        .chain(batch.puts.keys())
        .chain(batch.deletes.iter())
    {
        ensure!(
            !key.is_empty() && key.len() <= 2048 && !key.starts_with("__sys_"),
            "invalid guest storage key"
        );
    }
    Ok(batch)
}

pub fn apply(context: &Context, value: Value, maximum: usize) -> Result<Value> {
    let batch = validate(value, maximum).context(crate::failure::Validation)?;
    context.storage()?.transaction_sync(|tx| {
        for (key, snapshot) in &batch.checks {
            let current = tx.get_json(key)?;
            if current.is_some() != snapshot.present
                || (snapshot.present && current.as_ref() != Some(&snapshot.value))
            {
                return Err(Conflict.into());
            }
        }
        for (key, value) in &batch.puts {
            tx.put_json(key, value)?;
        }
        for key in &batch.deletes {
            tx.delete(key)?;
        }
        if let Some(time) = batch.alarm_at_ms {
            tx.set_alarm(time)?;
        }
        if batch.delete_alarm {
            tx.delete_alarm()?;
        }
        Ok(json!(null))
    })
}
