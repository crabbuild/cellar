// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Discover work only from the published committed image. Both adapters share
//! one client allocator, one metadata reservation and one owned scheduler turn.
use super::{alarm, outbox, RaftOutbox};
use crate::{application::consensus::RaftClass, raft::budget::Permit};
use anyhow::{ensure, Result};
use cellar_logic::consensus::{CommandFailure, CommandId, Durability};
use rusqlite::OptionalExtension;
use std::sync::Arc;

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "effect_scan_tests.rs"]
mod tests;

pub(crate) enum DueEffect {
    Alarm(alarm::DueAlarm),
    Delivery {
        selected: outbox::Selected,
        minimum: Durability,
        definition: Arc<RaftOutbox>,
    },
}

impl DueEffect {
    pub fn minimum(&self) -> Durability {
        match self {
            Self::Alarm(due) => due.minimum,
            Self::Delivery { minimum, .. } => *minimum,
        }
    }

    pub fn command(&self) -> Result<(String, Vec<u8>)> {
        Ok(match self {
            Self::Alarm(due) => (due.operation.clone(), due.input.clone()),
            Self::Delivery { selected, .. } => {
                (outbox::CLAIM.into(), serde_json::to_vec(selected)?)
            }
        })
    }
}

pub(crate) struct EffectScan {
    pub next: CommandId,
    pub due: Option<DueEffect>,
    pub _reservation: Permit,
}

fn scan_connection(
    connection: &rusqlite::Connection,
    scope: &cellar_logic::cell_group::CellHistory,
    class: &RaftClass,
    now: u64,
    prefer_delivery: bool,
    reservation: Permit,
) -> Result<EffectScan> {
    let client = super::system_client(scope)?;
    let last:Option<Vec<u8>>=connection.query_row("SELECT CASE WHEN typeof(last_sequence)='blob' AND length(last_sequence)=8 THEN last_sequence ELSE NULL END FROM __sys_raft_clients WHERE client_id=?1",[client.bytes().as_slice()],|row|row.get(0)).optional()?;
    let last = last
        .map(|bytes| {
            bytes
                .try_into()
                .map(u64::from_be_bytes)
                .map_err(|_| CommandFailure::CorruptState)
        })
        .transpose()?
        .unwrap_or(0);
    let next = CommandId::new(
        client,
        last.checked_add(1).ok_or(CommandFailure::Overloaded)?,
    )?;
    let delivery = || -> Result<Option<DueEffect>> {
        let Some(definition) = class.outbox_definition() else {
            return Ok(None);
        };
        Ok(
            outbox::due(connection, definition, now)?.map(|(selected, minimum)| {
                DueEffect::Delivery {
                    selected,
                    minimum,
                    definition: definition.clone(),
                }
            }),
        )
    };
    let alarm = || -> Result<Option<DueEffect>> {
        Ok(alarm::due(connection, class, now)?.map(DueEffect::Alarm))
    };
    let due = if prefer_delivery {
        delivery()?.or(alarm()?)
    } else {
        alarm()?.or(delivery()?)
    };
    ensure!(
        due.is_none() || class.has_system_commands(),
        CommandFailure::CorruptState
    );
    Ok(EffectScan {
        next,
        due,
        _reservation: reservation,
    })
}

/// Alternate adapter preference using the last committed private operation.
/// Placement, process restarts and the number of other cells do not affect it.
pub(crate) fn scan_connection_fair(
    connection: &rusqlite::Connection,
    scope: &cellar_logic::cell_group::CellHistory,
    class: &RaftClass,
    now: u64,
    reservation: Permit,
) -> Result<EffectScan> {
    let client = super::system_client(scope)?;
    let previous: Option<String> = connection.query_row(
        "SELECT CASE WHEN typeof(r.operation)='text' AND length(CAST(r.operation AS BLOB)) BETWEEN 1 AND 128 THEN r.operation ELSE NULL END FROM __sys_raft_clients c JOIN __sys_raft_receipts r ON r.client_id=c.client_id AND r.sequence=c.last_sequence WHERE c.client_id=?1",
        [client.bytes().as_slice()], |row| row.get(0),
    ).optional()?;
    let prefer_delivery =
        previous.is_some_and(|operation| operation.starts_with("__sys_raft_alarm_"));
    scan_connection(connection, scope, class, now, prefer_delivery, reservation)
}
