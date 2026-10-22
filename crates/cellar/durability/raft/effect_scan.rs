//! Discover work only from the published committed image. Both adapters share
//! one client allocator, one metadata reservation and one owned scheduler turn.
use super::{alarm, outbox, RaftOutbox};
use crate::{
    application::consensus::RaftClass,
    raft::{apply::PublishedImage, budget::Permit},
};
use anyhow::{ensure, Result};
use cellar_logic::consensus::{CommandFailure, CommandId, Durability, LogPosition};
use rusqlite::OptionalExtension;
use std::sync::Arc;

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
    pub position: LogPosition,
    pub next: CommandId,
    pub due: Option<DueEffect>,
    pub _reservation: Permit,
}

pub(crate) fn scan(
    image: PublishedImage,
    class: &RaftClass,
    services: cellar_ltx::generation::ReadServices,
    now: u64,
    prefer_delivery: bool,
    reservation: Permit,
) -> Result<EffectScan> {
    let database = crate::storage::open_immutable_generation(
        image.pin(),
        crate::storage::ImmutableQueryOptions {
            services,
            sqlite_vec: false,
        },
    )?;
    let connection = database.connection();
    let client = super::system_client(image.scope())?;
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
        position: image.position(),
        next,
        due,
        _reservation: reservation,
    })
}
