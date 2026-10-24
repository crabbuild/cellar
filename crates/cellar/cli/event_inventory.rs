// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Whole-fleet descriptor discovery and bounded owner metadata pages.
#![allow(clippy::disallowed_methods)]
use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::event_inventory::{InventoryReply, InventoryRequest};
use crate::operator_cell::{Fleet, Reachable, Subject};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::time::Duration;

const HELP:&str="cellar event inventory [--cursor TOKEN] [--limit N] [--json]\ncellar event inventory SCOPE --page resources|producers|history [--cursor TOKEN] [--limit N] [--json]\n\nWithout SCOPE: one bounded root-directory page and four signed owner probes.\nDiscovers catalog namespaces, retained stream identities and current sources.\nWith SCOPE: at most 100 retained resource/producer/source-history records.\nMetadata excludes event bodies, configuration grants, tokens and full receipts.\nFailed probes remain unavailable; a failed exit never means an empty source.\nCold reads can restore cells and progress existing work. Pages are live, not a snapshot or drain proof.\nFacet inventory remains separate; source history is retained operation metadata.";

struct Options {
    fleet: FleetFlags,
    scope: Option<String>,
    page: Option<String>,
    cursor: Option<String>,
    limit: usize,
    json: bool,
    unsafe_public_advertise: bool,
}

fn parse(arguments: Vec<String>) -> Result<Option<Options>> {
    let mut o = Options {
        fleet: FleetFlags::default(),
        scope: None,
        page: None,
        cursor: None,
        limit: 100,
        json: false,
        unsafe_public_advertise: false,
    };
    let mut args = arguments.into_iter();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "help" | "--help" | "-h" => return Ok(None),
            "--json" => {
                o.json = true;
                continue;
            }
            "--unsafe-public-advertise" => {
                o.unsafe_public_advertise = true;
                continue;
            }
            _ => {}
        }
        let mut value = |name: &str| {
            args.next()
                .with_context(|| format!("{name} requires a value"))
        };
        if o.fleet.consume(&flag, &mut value)? {
            continue;
        }
        match flag.as_str() {
            "--limit" => o.limit = value(&flag)?.parse()?,
            "--cursor" => o.cursor = Some(value(&flag)?),
            "--page" => o.page = Some(value(&flag)?),
            _ => {
                ensure!(
                    !flag.starts_with('-') && o.scope.is_none(),
                    "invalid inventory option"
                );
                o.scope = Some(flag);
            }
        }
    }
    ensure!(
        (1..=100).contains(&o.limit) && o.cursor.as_ref().is_none_or(|s| s.len() <= 16384),
        "inventory bounds exceeded"
    );
    if let Some(scope) = &o.scope {
        request(&o)?.validate(scope)?;
    } else {
        ensure!(o.page.is_none(), "--page requires SCOPE");
    }
    Ok(Some(o))
}

fn request(options: &Options) -> Result<InventoryRequest> {
    let cursor = options.cursor.clone();
    let limit = options.limit;
    Ok(match options.page.as_deref() {
        None => {
            ensure!(cursor.is_none(), "descriptor request has no cursor");
            InventoryRequest::Describe
        }
        Some("resources") => InventoryRequest::Resources { limit, cursor },
        Some("producers") => InventoryRequest::Producers { limit, cursor },
        Some("history") => InventoryRequest::History { limit, cursor },
        _ => anyhow::bail!("unknown inventory page"),
    })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    fleet: String,
    token: String,
}

fn identity(storage: &crate::cli_options::Storage) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(
        &json!({"bucket":storage.bucket,"endpoint":storage.endpoint,"region":storage.region}),
    )?)))
}

fn decode(cursor: Option<&str>, fleet: &str) -> Result<Option<String>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    ensure!(
        cursor.len() <= 16384,
        "fleet inventory cursor exceeds bounds"
    );
    let raw = cursor
        .strip_prefix("inventory1:")
        .context("unsupported fleet inventory cursor")?;
    let bytes = URL_SAFE_NO_PAD.decode(raw)?;
    ensure!(
        URL_SAFE_NO_PAD.encode(&bytes) == raw,
        "noncanonical fleet cursor"
    );
    let c: Cursor = serde_json::from_slice(&bytes)?;
    ensure!(
        c.fleet == fleet && !c.token.is_empty() && c.token.len() <= 8192,
        "inventory cursor names another fleet or exceeds bounds"
    );
    Ok(Some(c.token))
}

fn encode(token: Option<String>, fleet: &str) -> Result<Option<String>> {
    token
        .map(|token| {
            ensure!(
                !token.is_empty() && token.len() <= 8192,
                "provider inventory token exceeds bounds"
            );
            let cursor = format!(
                "inventory1:{}",
                URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor {
                    fleet: fleet.into(),
                    token
                })?)
            );
            decode(Some(&cursor), fleet)?;
            Ok(cursor)
        })
        .transpose()
}

async fn probe(
    storage: &crate::cli_options::Storage,
    unsafe_public_advertise: bool,
    scope: &str,
    request: &InventoryRequest,
) -> Result<InventoryReply> {
    let target = Reachable::open_inventory(
        Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise,
        },
        Subject {
            noun: "event inventory",
            source: "event-inventory",
            timeout: Duration::from_secs(15),
        },
        scope.into(),
    )
    .await?;
    let reply: InventoryReply =
        serde_json::from_value(target.call(serde_json::to_value(request)?).await?)?;
    reply.validate(scope, request)?;
    Ok(reply)
}

pub(super) async fn run(arguments: Vec<String>) -> Result<()> {
    use std::io::Write;
    let Some(options) = parse(arguments)? else {
        return crate::cli_output::Output::new(crate::cli_output::Format::Text)
            .line(format_args!("{HELP}\n\n{FLEET_HELP}"));
    };
    let storage = options.fleet.clone().resolve("cellar event inventory")?;
    let value = if let Some(scope) = &options.scope {
        serde_json::to_value(
            probe(
                &storage,
                options.unsafe_public_advertise,
                scope,
                &request(&options)?,
            )
            .await?,
        )?
    } else {
        let fleet = identity(&storage)?;
        let token = decode(options.cursor.as_deref(), &fleet)?;
        let bucket = storage.open().await?;
        let page = bucket
            .common_prefixes_page("cells/", None, token.clone(), options.limit)
            .await?;
        ensure!(
            page.prefixes.len() <= options.limit
                && (page.page_token.is_none() || page.page_token != token),
            "provider inventory page exceeded bounds or repeated cursor"
        );
        let scanned = page.prefixes.len();
        let next = encode(page.page_token, &fleet)?;
        let scopes = crate::cell_cli::cell_scopes_from_prefixes(page.prefixes, None, None)
            .collect::<Vec<_>>();
        let invalid = scanned - scopes.len();
        let mut seen = std::collections::HashSet::new();
        let mut duplicate = 0usize;
        let scopes = scopes
            .into_iter()
            .filter(|s| {
                if seen.insert(s.clone()) {
                    true
                } else {
                    duplicate += 1;
                    false
                }
            })
            .collect::<Vec<_>>();
        let storage = &storage;
        let unsafe_public_advertise = options.unsafe_public_advertise;
        let mut probes = stream::iter(scopes.into_iter().map(|scope| async move {
            let row = match probe(
                storage,
                unsafe_public_advertise,
                &scope,
                &InventoryRequest::Describe,
            )
            .await
            {
                Ok(reply) => json!({"scope":scope,"status":"observed","inventory":reply}),
                Err(_) => json!({"scope":scope,"status":"unavailable"}),
            };
            row
        }))
        .buffered(4);
        let mut rows = Vec::new();
        while let Some(row) = probes.next().await {
            rows.push(row);
        }
        let missing =
            invalid + duplicate + rows.iter().filter(|r| r["status"] == "unavailable").count();
        json!({"format":1,"scanned_prefixes":scanned,"records":rows,"next":next,"unavailable":missing,"complete_probes":missing==0})
    };
    let text = if options.json {
        serde_json::to_string(&value)?
    } else {
        serde_json::to_string_pretty(&value)?
    };
    writeln!(std::io::stdout().lock(), "{text}")?;
    ensure!(
        value.get("complete_probes").is_none_or(|b| b == true),
        "inventory page has unavailable probes; records and continuation were printed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fleet_cursor_cannot_move_between_buckets_or_continuation_kinds() -> Result<()> {
        let token = encode(Some("provider/opaque".into()), "fleet-a")?.unwrap();
        assert_eq!(
            decode(Some(&token), "fleet-a")?,
            Some("provider/opaque".into())
        );
        assert!(decode(Some(&token), "fleet-b").is_err());
        assert!(decode(Some("census1:abc"), "fleet-a").is_err());
        assert!(parse(vec!["--page".into(), "history".into()]).is_err());
        assert!(parse(vec!["--limit".into(), "101".into()]).is_err());
        Ok(())
    }
}
