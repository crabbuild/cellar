// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The pending occurrence and alarm decision share the cell's SQLite/output gate.
use super::*;
use crate::application::storage::SqlValue;
use anyhow::ensure;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct State {
    pub retry: i64,
    pub owed: Option<Owed>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Owed {
    pub occurrence: i64,
    pub indices: Vec<usize>,
    pub failed: Vec<usize>,
    pub armed_for: i64,
    pub attempt: i64,
    pub running: bool,
    pub in_flight: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    format: u32,
    configuration: String,
    state: State,
}

pub(super) fn load(storage: &Storage, configuration: &str, expressions: usize) -> Result<State> {
    storage.system_sql_schema("CREATE TABLE IF NOT EXISTS __sys_cron_state (id INTEGER PRIMARY KEY CHECK(id=1), document TEXT NOT NULL)")?;
    let rows = storage.system_sql("SELECT document FROM __sys_cron_state WHERE id=1", &[])?;
    let Some(row) = rows.first() else {
        return Ok(State::default());
    };
    let Some(SqlValue::Text(raw)) = row.get("document") else {
        return Err(anyhow!("invalid persisted cron state"));
    };
    let document: Document = serde_json::from_str(raw)?;
    ensure!(document.format == 1, "unsupported cron state format");
    // A changed deployment/schedule deliberately replaces the old configuration.
    // Runtime generation/activation identity is excluded from this fingerprint.
    if document.configuration != configuration {
        return Ok(State::default());
    }
    let state = document.state;
    ensure!(state.retry >= 0, "invalid cron retry count");
    if let Some(owed) = &state.owed {
        ensure!(
            owed.attempt > 0 && owed.occurrence >= 0 && owed.armed_for >= 0,
            "invalid persisted cron occurrence"
        );
        let mut seen = std::collections::BTreeSet::new();
        ensure!(
            owed.indices
                .iter()
                .chain(&owed.failed)
                .all(|index| *index < expressions && seen.insert(*index)),
            "invalid persisted cron expression set"
        );
        ensure!(
            !owed.in_flight || (owed.running && !owed.indices.is_empty()),
            "invalid persisted cron progress"
        );
    }
    Ok(state)
}

pub(super) fn save(storage: &Storage, configuration: &str, state: &State) -> Result<()> {
    storage.system_sql("INSERT INTO __sys_cron_state(id,document) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET document=excluded.document",
        &[serde_json::to_string(&Document { format:1, configuration:configuration.into(), state:state.clone() })?.into()])?;
    Ok(())
}

/// Public get_alarm intentionally hides an alarm during its handler. Arming
/// needs the stored row to distinguish that from native retry exhaustion.
pub(super) fn has_alarm(storage: &Storage, scope: &str) -> Result<bool> {
    Ok(!storage
        .system_sql(
            "SELECT 1 FROM __sys_cell_alarm WHERE scope=?",
            &[scope.to_string().into()],
        )?
        .is_empty())
}
