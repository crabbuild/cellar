// Bounded operator files are process-boundary inputs, not node storage effects.
#![allow(clippy::disallowed_methods)]

use crate::application::event_stream::{
    Change, ChangeAction, Cursor, Retirement, RetirementMode, SubscriptionIdentity,
};
use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::cli_output::{Format, Output, Record};
use crate::operator_cell::{Fleet, Reachable, Subject};
use crate::runtime::stream_control::{Command, Request, MAX_READ_BYTES, MAX_REQUEST_BYTES};
use anyhow::{bail, Context, Result};
use std::borrow::Cow;
use std::io::Read;
use std::time::Duration;

const HELP: &str = "cellar stream describe --binding BINDING.json
cellar stream sources --binding BINDING.json [--cursor TOKEN] [--limit N]
cellar stream read --binding BINDING.json [--cursor TOKEN] [--limit N] [--max-bytes N] [--payload]
cellar stream status --binding BINDING.json --operation UUID
cellar stream audit --binding BINDING.json [--after POSITION] [--limit N]
cellar stream retire --binding BINDING.json --revision N --operation UUID --reason TEXT --procedure TEXT --confirm-retire
cellar stream retire --binding BINDING.json --revision N --operation UUID --reason TEXT --force --data-loss TEXT --confirm-retire
cellar stream retirement-status --binding BINDING.json --operation UUID
cellar subscription list --binding BINDING.json [--cursor TOKEN] [--limit N]
cellar subscription describe NAME --binding BINDING.json
cellar subscription pause|resume NAME --binding BINDING.json --expected IDENTITY.json --operation UUID --reason TEXT
cellar subscription reset NAME --binding BINDING.json --expected IDENTITY.json --operation UUID --reason TEXT --cursor TOKEN --confirm-reset
cellar subscription delete NAME --binding BINDING.json --expected IDENTITY.json --operation UUID --reason TEXT --confirm-delete [--force]

The binding file uses the deployed stream Binding shape. The receiver checks
its current principal/grant independently of these supplied permissions.
Read returns metadata; --payload explicitly includes event bodies. Pages contain
at most 100 records and reads copy at most 256 KiB of retained event bytes.
Subscription lists include retained deleted incarnations, ordered by UUID.
Their stream-scoped cursor observes live pages, not a stable snapshot.
Source lists show destination-observed producer lanes and retained seals; they
cannot discover undispatched sources or prove that every producer is drained.
Save describe's identity object before a subscription change. Reset/delete
require explicit destructive intent. Exact change replay requires the same
identity, arguments, operation and operator label. Use stream status or audit
after an ambiguous reply; a successful replay returns the original receipt.";

struct Reply(serde_json::Value);

impl Record for Reply {
    fn json(&self) -> serde_json::Value {
        self.0.clone()
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Owned(serde_json::to_string_pretty(&self.0).expect("JSON value serializes"))
    }
}

struct Options {
    request: Request,
    fleet: FleetFlags,
    json: bool,
    unsafe_public_advertise: bool,
}

fn file<T: serde::de::DeserializeOwned>(path: &str, limit: usize) -> Result<T> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= limit,
        "operator input file exceeds {limit} bytes"
    );
    serde_json::from_slice(&bytes).context("decode operator input file")
}

fn parse(arguments: Vec<String>, subscription: bool) -> Result<Option<Options>> {
    let mut args = arguments.into_iter();
    let Some(verb) = args.next() else {
        return Ok(None);
    };
    if matches!(verb.as_str(), "--help" | "-h" | "help") {
        return Ok(None);
    }
    let change = subscription && matches!(verb.as_str(), "pause" | "resume" | "reset" | "delete");
    anyhow::ensure!(
        change
            || if subscription {
                matches!(verb.as_str(), "describe" | "list")
            } else {
                matches!(
                    verb.as_str(),
                    "describe"
                        | "sources"
                        | "read"
                        | "status"
                        | "audit"
                        | "retire"
                        | "retirement-status"
                )
            },
        "unknown stream/subscription command {verb}"
    );
    let name = if subscription && verb != "list" {
        Some(args.next().context("subscription name required")?)
    } else {
        None
    };
    if name
        .as_deref()
        .is_some_and(|name| matches!(name, "--help" | "-h"))
    {
        return Ok(None);
    }
    let mut fleet = FleetFlags::default();
    let (mut json, mut unsafe_public_advertise, mut payload, mut force, mut confirmed) =
        (false, false, false, false, false);
    let (mut binding, mut expected, mut operation, mut reason, mut cursor) =
        (None, None, None, None, None);
    let (mut limit, mut max_bytes, mut after) = (100usize, MAX_READ_BYTES, "0".to_owned());
    let (mut revision, mut procedure, mut data_loss) = (None, None, None);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--help" | "-h" => return Ok(None),
            "--json" => {
                json = true;
                continue;
            }
            "--unsafe-public-advertise" => {
                unsafe_public_advertise = true;
                continue;
            }
            "--payload" if !subscription && verb == "read" => {
                payload = true;
                continue;
            }
            "--force" if (change && verb == "delete") || (!subscription && verb == "retire") => {
                force = true;
                continue;
            }
            "--confirm-reset" if change && verb == "reset" => {
                confirmed = true;
                continue;
            }
            "--confirm-delete" if change && verb == "delete" => {
                confirmed = true;
                continue;
            }
            "--confirm-retire" if !subscription && verb == "retire" => {
                confirmed = true;
                continue;
            }
            _ => {}
        }
        let mut value = |flag: &str| {
            args.next()
                .with_context(|| format!("{flag} requires a value"))
        };
        if fleet.consume(&flag, &mut value)? {
            continue;
        }
        match flag.as_str() {
            "--binding" => binding = Some(value(&flag)?),
            "--expected" if change => expected = Some(value(&flag)?),
            "--operation"
                if change
                    || (!subscription
                        && matches!(verb.as_str(), "status" | "retire" | "retirement-status")) =>
            {
                operation = Some(value(&flag)?)
            }
            "--reason" if change || (!subscription && verb == "retire") => {
                reason = Some(value(&flag)?)
            }
            "--revision" if !subscription && verb == "retire" => {
                revision = Some(
                    value(&flag)?
                        .parse::<i64>()
                        .context("invalid retirement revision")?,
                )
            }
            "--procedure" if !subscription && verb == "retire" => procedure = Some(value(&flag)?),
            "--data-loss" if !subscription && verb == "retire" => data_loss = Some(value(&flag)?),
            "--cursor"
                if (!subscription && matches!(verb.as_str(), "read" | "sources"))
                    || (subscription && verb == "list")
                    || (change && verb == "reset") =>
            {
                cursor = Some(value(&flag)?)
            }
            "--limit"
                if (!subscription && matches!(verb.as_str(), "read" | "audit" | "sources"))
                    || (subscription && verb == "list") =>
            {
                limit = value(&flag)?.parse().context("invalid --limit")?
            }
            "--max-bytes" if !subscription && verb == "read" => {
                max_bytes = value(&flag)?.parse().context("invalid --max-bytes")?
            }
            "--after" if !subscription && verb == "audit" => after = value(&flag)?,
            _ => bail!("invalid {verb} option {flag}"),
        }
    }
    let command = if change {
        if matches!(verb.as_str(), "reset" | "delete") {
            anyhow::ensure!(confirmed, "{verb} requires --confirm-{verb}");
        }
        let expected: SubscriptionIdentity = file(
            &expected.context("change requires --expected IDENTITY.json")?,
            4096,
        )?;
        anyhow::ensure!(
            name.as_deref() == Some(expected.name.as_str()),
            "expected identity names another subscription"
        );
        let action = match verb.as_str() {
            "pause" => ChangeAction::Pause,
            "resume" => ChangeAction::Resume,
            "reset" => ChangeAction::Reset {
                cursor: Cursor::decode(&cursor.context("reset requires --cursor")?)?,
            },
            "delete" => ChangeAction::Delete { force },
            _ => unreachable!("validated change"),
        };
        Command::Change {
            change: Change {
                operation: operation.context("change requires --operation")?,
                expected,
                action,
                reason: reason.context("change requires --reason")?,
            },
        }
    } else if subscription && verb == "list" {
        Command::Subscriptions { cursor, limit }
    } else if let Some(name) = name {
        Command::Subscription { name }
    } else {
        match verb.as_str() {
            "describe" => Command::Describe,
            "sources" => Command::Sources { cursor, limit },
            "read" => Command::Read {
                cursor,
                limit,
                max_bytes,
                payload,
            },
            "status" => Command::Status {
                operation: operation.context("status requires --operation")?,
            },
            "audit" => Command::Audit { after, limit },
            "retire" => {
                anyhow::ensure!(confirmed, "retire requires --confirm-retire");
                let mode = if force {
                    anyhow::ensure!(
                        procedure.is_none(),
                        "forced retirement cannot attest --procedure"
                    );
                    RetirementMode::Force {
                        data_loss_acknowledgment: data_loss
                            .context("forced retirement requires --data-loss TEXT")?,
                    }
                } else {
                    anyhow::ensure!(data_loss.is_none(), "--data-loss requires --force");
                    RetirementMode::Coordinated {
                        procedure: procedure
                            .context("coordinated retirement requires --procedure TEXT")?,
                    }
                };
                Command::Retire {
                    request: Retirement {
                        operation: operation.context("retire requires --operation")?,
                        revision: revision.context("retire requires --revision")?,
                        reason: reason.context("retire requires --reason")?,
                        mode,
                    },
                }
            }
            "retirement-status" => Command::RetirementStatus {
                operation: operation.context("retirement-status requires --operation")?,
            },
            _ => unreachable!("validated command"),
        }
    };
    let request = Request {
        binding: file(
            &binding.context("--binding BINDING.json is required")?,
            MAX_REQUEST_BYTES,
        )?,
        command,
    };
    request.prepare(
        if subscription {
            "subscriptions"
        } else {
            "streams"
        }
        .into(),
    )?;
    anyhow::ensure!(
        serde_json::to_vec(&request)?.len() <= MAX_REQUEST_BYTES,
        "stream operator request exceeds limit"
    );
    Ok(Some(Options {
        request,
        fleet,
        json,
        unsafe_public_advertise,
    }))
}

pub async fn run(arguments: Vec<String>, subscription: bool) -> Result<()> {
    let Some(options) = parse(arguments, subscription)? else {
        return Output::new(Format::Text).line(format_args!(
            "{HELP}\n\n{FLEET_HELP}\n  --json              Print one JSON result object"
        ));
    };
    let storage = options.fleet.resolve("cellar stream/subscription")?;
    let noun = if subscription {
        "subscriptions"
    } else {
        "streams"
    };
    let target = Reachable::open_stream(
        Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise: options.unsafe_public_advertise,
        },
        Subject {
            noun,
            source: "events-operator",
            timeout: Duration::from_secs(60),
        },
        options.request.scope()?,
    )
    .await?;
    let reply = target.call(serde_json::to_value(options.request)?).await?;
    let mut output = Output::new(if options.json {
        Format::Json
    } else {
        Format::Text
    });
    output.row(&Reply(reply))?;
    output.finish()
}
