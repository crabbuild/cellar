// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Operator input files and environment settings are process-boundary effects.
#![allow(clippy::disallowed_methods)]

//! Source diagnostics and audited retry through authenticated owner routing.

use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::cli_output::{Format, Output, Record};
use crate::operator_cell::{Fleet, Reachable, Subject};
use crate::runtime::source_control::{Request, Retry};
use crate::runtime::source_destruction::{Destroy, Recreate};
use anyhow::{bail, Context, Result};
use std::borrow::Cow;
use std::io::Read;
use std::time::Duration;

#[path = "outbox_census.rs"]
mod census;

const HELP: &str = "cellar outbox census [CLASS] [--limit N] [--cursor TOKEN]
cellar outbox inspect SCOPE [--limit N] [--cursor CURSOR]
cellar outbox retry SCOPE --expected REVISION.json --operation UUID --reason TEXT
cellar outbox destroy SCOPE --incarnation UUID --operation UUID --reason TEXT --data-loss TEXT --confirm-destroy
cellar outbox recreate SCOPE --incarnation UUID --operation UUID --reason TEXT --confirm-recreate
cellar outbox status SCOPE --operation UUID
cellar outbox audit SCOPE [--limit N] [--after POSITION]

Inspect returns metadata only. Each page contains at most 100 lanes.
Save a lane's revision object to REVISION.json before retry. Retry requires the
exact source incarnation and producer revision; it never skips or removes an
event. Repeat the same operation, reason and revision after an ambiguous reply.
Administrative receipts remain retained; use status or paginated audit to inspect
them. All commands route through the authoritative owner and its output gates.";

struct Reply(serde_json::Value);

impl Record for Reply {
    fn json(&self) -> serde_json::Value {
        self.0.clone()
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(serde_json::to_string_pretty(&self.0).expect("JSON value serializes"))
    }
}

struct Command {
    scope: String,
    fleet: FleetFlags,
    request: Request,
    json: bool,
    unsafe_public_advertise: bool,
}

fn parse(arguments: Vec<String>) -> Result<Option<Command>> {
    let mut arguments = arguments.into_iter();
    let Some(verb) = arguments.next() else {
        return Ok(None);
    };
    if matches!(verb.as_str(), "--help" | "-h" | "help") {
        return Ok(None);
    }
    if !matches!(
        verb.as_str(),
        "inspect" | "retry" | "destroy" | "recreate" | "status" | "audit"
    ) {
        bail!("unknown outbox command {verb}");
    }
    let scope = arguments
        .next()
        .context("outbox command requires a source cell scope")?;
    if matches!(scope.as_str(), "--help" | "-h") {
        return Ok(None);
    }
    anyhow::ensure!(
        cellar_logic::cell::valid_cell_scope(&scope)
            && !crate::application::event_stream::is_event_scope(&scope),
        "invalid source cell scope"
    );
    let mut fleet = FleetFlags::default();
    let mut json = false;
    let mut unsafe_public_advertise = false;
    let mut operation = None;
    let mut incarnation = None;
    let mut data_loss = None;
    let mut confirmed = false;
    let mut reason = None;
    let mut expected = None;
    let mut limit = None;
    let mut cursor = None;
    let mut after = None;
    while let Some(flag) = arguments.next() {
        match flag.as_str() {
            "--help" | "-h" => return Ok(None),
            "--json" => {
                json = true;
                continue;
            }
            "--confirm-destroy" if verb == "destroy" => {
                confirmed = true;
                continue;
            }
            "--confirm-recreate" if verb == "recreate" => {
                confirmed = true;
                continue;
            }
            "--unsafe-public-advertise" => {
                unsafe_public_advertise = true;
                continue;
            }
            _ => {}
        }
        let mut value = |flag: &str| {
            arguments
                .next()
                .with_context(|| format!("{flag} requires a value"))
        };
        if fleet.consume(&flag, &mut value)? {
            continue;
        }
        match flag.as_str() {
            "--operation"
                if matches!(verb.as_str(), "retry" | "destroy" | "recreate" | "status") =>
            {
                operation = Some(value(&flag)?)
            }
            "--reason" if matches!(verb.as_str(), "retry" | "destroy" | "recreate") => {
                reason = Some(value(&flag)?)
            }
            "--incarnation" if matches!(verb.as_str(), "destroy" | "recreate") => {
                incarnation = Some(value(&flag)?)
            }
            "--data-loss" if verb == "destroy" => data_loss = Some(value(&flag)?),
            "--expected" if verb == "retry" => expected = Some(value(&flag)?),
            "--limit" if matches!(verb.as_str(), "inspect" | "audit") => {
                limit = Some(
                    value(&flag)?
                        .parse::<usize>()
                        .context("--limit requires an integer")?,
                )
            }
            "--cursor" if verb == "inspect" => cursor = Some(value(&flag)?),
            "--after" if verb == "audit" => after = Some(value(&flag)?),
            _ => bail!("invalid outbox {verb} option {flag}"),
        }
    }
    anyhow::ensure!(
        !matches!(verb.as_str(), "destroy" | "recreate") || confirmed,
        "outbox {verb} requires --confirm-{verb}"
    );
    let request = match verb.as_str() {
        "inspect" => Request::Inspect {
            limit: limit.unwrap_or(100),
            cursor,
        },
        "audit" => Request::Audit {
            limit: limit.unwrap_or(100),
            after,
        },
        "status" => Request::Status {
            operation: operation.context("outbox status requires --operation")?,
        },
        "destroy" => Request::Destroy {
            change: Destroy {
                source: scope.clone(),
                incarnation: incarnation.context("outbox destroy requires --incarnation")?,
                operation: operation.context("outbox destroy requires --operation")?,
                reason: reason.context("outbox destroy requires --reason")?,
                data_loss_acknowledgment: data_loss
                    .context("outbox destroy requires --data-loss TEXT")?,
            },
        },
        "recreate" => Request::Recreate {
            change: Recreate {
                source: scope.clone(),
                incarnation: incarnation.context("outbox recreate requires --incarnation")?,
                operation: operation.context("outbox recreate requires --operation")?,
                reason: reason.context("outbox recreate requires --reason")?,
            },
        },
        "retry" => {
            let operation = operation.context("outbox retry requires --operation")?;
            let reason = reason.context("outbox retry requires --reason")?;
            let expected = expected.context("outbox retry requires --expected REVISION.json")?;
            let mut bytes = Vec::new();
            std::fs::File::open(expected)?
                .take(4097)
                .read_to_end(&mut bytes)?;
            anyhow::ensure!(
                bytes.len() <= 4096,
                "outbox retry revision file is too large"
            );
            let expected: crate::sdk::OutboxRevision =
                serde_json::from_slice(&bytes).context("decode outbox retry revision")?;
            anyhow::ensure!(
                expected.source == scope,
                "retry revision names another source"
            );
            Request::Retry {
                change: Retry {
                    operation,
                    reason,
                    expected,
                },
            }
        }
        _ => unreachable!("validated command"),
    };
    request.validate()?;
    Ok(Some(Command {
        scope,
        fleet,
        request,
        json,
        unsafe_public_advertise,
    }))
}

pub async fn run(arguments: Vec<String>) -> Result<()> {
    if arguments.first().is_some_and(|verb| verb == "census") {
        return census::run(arguments.into_iter().skip(1).collect()).await;
    }
    let Some(command) = parse(arguments)? else {
        return Output::new(Format::Text).line(format_args!(
            "{HELP}\n\n{FLEET_HELP}\n  --json              Print one JSON result object"
        ));
    };
    let storage = command.fleet.resolve("cellar outbox")?;
    let source = Reachable::open_source(
        Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise: command.unsafe_public_advertise,
        },
        Subject {
            noun: "outbox",
            source: "outbox",
            timeout: Duration::from_secs(60),
        },
        command.scope,
    )
    .await?;
    let reply = source.call(serde_json::to_value(command.request)?).await?;
    let mut output = Output::new(if command.json {
        Format::Json
    } else {
        Format::Text
    });
    output.row(&Reply(reply))?;
    output.finish()
}
