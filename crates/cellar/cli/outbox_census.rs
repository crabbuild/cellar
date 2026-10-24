// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded bucket-directory census, with authoritative source-owner observations.

use super::{Fleet, FleetFlags, Format, Output, Reachable, Reply, Subject, FLEET_HELP};
use crate::application::{OutboxOptions, OutboxPage};
use crate::bucket::Bucket;
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;

const HELP: &str = "cellar outbox census [CLASS] [--limit N] [--cursor TOKEN] [--json]

One bounded bucket-directory page, followed by signed source-owner inspection.
At most 100 directory prefixes and four concurrent source probes per call.
Records distinguish source, empty and unavailable; unavailable is never empty.
Catalog and stream cells are excluded. Other framework cells remain candidates.
Totals are live owner observations,
not a snapshot, drain proof or aggregate metric. Inspection can restore a cold
source and allow its existing durable work to progress. No event bodies or lanes
are returned. Continue until next is null; an empty page can still have next.";

struct Options {
    fleet: FleetFlags,
    class: Option<String>,
    limit: usize,
    cursor: Option<String>,
    json: bool,
    unsafe_public_advertise: bool,
}

fn parse(arguments: Vec<String>) -> Result<Option<Options>> {
    let mut options = Options {
        fleet: FleetFlags::default(),
        class: None,
        limit: 100,
        cursor: None,
        json: false,
        unsafe_public_advertise: false,
    };
    let mut args = arguments.into_iter();
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            "--json" => {
                options.json = true;
                continue;
            }
            "--unsafe-public-advertise" => {
                options.unsafe_public_advertise = true;
                continue;
            }
            _ => {}
        }
        let mut value = |name: &str| {
            args.next()
                .with_context(|| format!("{name} requires a value"))
        };
        if options.fleet.consume(&flag, &mut value)? {
            continue;
        }
        match flag.as_str() {
            "--limit" => options.limit = value(&flag)?.parse().context("invalid census limit")?,
            "--cursor" => options.cursor = Some(value(&flag)?),
            _ => {
                ensure!(
                    !flag.starts_with('-') && options.class.is_none(),
                    "invalid census option {flag}"
                );
                options.class = Some(flag);
            }
        }
    }
    ensure!(
        (1..=100).contains(&options.limit),
        "census limit must be between 1 and 100"
    );
    if let Some(class) = &options.class {
        ensure!(
            !class.contains(':')
                && cellar_logic::cell::valid_cell_scope(class)
                && !matches!(
                    class.as_str(),
                    crate::deploy::EVENT_CATALOG_CLASS | crate::deploy::EVENT_STREAM_CLASS
                ),
            "invalid census source class"
        );
    }
    ensure!(
        options
            .cursor
            .as_ref()
            .is_none_or(|cursor| cursor.len() <= 16384),
        "census cursor exceeds limit"
    );
    Ok(Some(options))
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    scope: String,
    token: String,
}

fn selector(bucket: &Bucket, class: Option<&str>, endpoint: Option<&str>) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&json!({
        "backend":format!("{:?}",bucket.backend),"bucket":bucket.name,"prefix":bucket.prefix,
        "class":class,"endpoint":endpoint
    }))?)))
}

fn decode(cursor: Option<&str>, scope: &str) -> Result<Option<String>> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    ensure!(cursor.len() <= 16384, "census cursor exceeds limit");
    let raw = cursor
        .strip_prefix("census1:")
        .context("unsupported census cursor")?;
    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .context("invalid census cursor encoding")?;
    ensure!(
        URL_SAFE_NO_PAD.encode(&bytes) == raw,
        "noncanonical census cursor"
    );
    let cursor: Cursor = serde_json::from_slice(&bytes).context("invalid census cursor fields")?;
    ensure!(
        cursor.scope == scope && !cursor.token.is_empty() && cursor.token.len() <= 8192,
        "census cursor names another fleet/class or exceeds bounds"
    );
    Ok(Some(cursor.token))
}

fn encode(token: Option<String>, scope: &str) -> Result<Option<String>> {
    token
        .map(|token| {
            ensure!(
                !token.is_empty() && token.len() <= 8192,
                "store continuation exceeds census bounds"
            );
            Ok(format!(
                "census1:{}",
                URL_SAFE_NO_PAD.encode(serde_json::to_vec(&Cursor {
                    scope: scope.into(),
                    token
                })?)
            ))
        })
        .transpose()
}

fn project(scope: &str, value: Value) -> Result<Value> {
    let page: OutboxPage = serde_json::from_value(value)?;
    page.validate(
        scope,
        &OutboxOptions {
            limit: 1,
            cursor: None,
        },
    )?;
    Ok(
        json!({"source":scope,"status":if page.incarnation.is_some(){"source"}else{"empty"},
        "incarnation":page.incarnation,"pending_rows":page.pending_rows.to_string(),
        "pending_bytes":page.pending_bytes.to_string(),"observation":page.observation}),
    )
}

async fn probe(
    storage: &crate::cli_options::Storage,
    unsafe_public_advertise: bool,
    scope: String,
) -> Value {
    let result = async {
        let source = Reachable::open_source(
            Fleet {
                bucket: &storage.bucket,
                endpoint: storage.endpoint.as_deref(),
                region: &storage.region,
                unsafe_public_advertise,
            },
            Subject {
                noun: "outbox census",
                source: "outbox-census",
                timeout: Duration::from_secs(15),
            },
            scope.clone(),
        )
        .await?;
        project(
            &scope,
            source.call(json!({"op":"inspect","limit":1})).await?,
        )
    }
    .await;
    result.unwrap_or_else(|error: anyhow::Error| {
        json!({
            "source":scope,"status":"unavailable",
            "error":error.to_string().chars().take(512).collect::<String>()
        })
    })
}

pub(super) async fn run(arguments: Vec<String>) -> Result<()> {
    let Some(options) = parse(arguments)? else {
        return Output::new(Format::Text).line(format_args!("{HELP}\n\n{FLEET_HELP}"));
    };
    let storage = options.fleet.resolve("cellar outbox census")?;
    let bucket = storage.open().await?;
    let scope = selector(
        &bucket,
        options.class.as_deref(),
        storage.endpoint.as_deref(),
    )?;
    let token = decode(options.cursor.as_deref(), &scope)?;
    let prefix = options
        .class
        .as_ref()
        .map_or_else(|| "cells/".into(), |class| format!("cells/{class}:"));
    // Exactly one provider page: filtering must never turn a bounded request
    // into a walk over arbitrarily many reserved/malformed directory entries.
    let page = bucket
        .common_prefixes_page(&prefix, None, token, options.limit)
        .await?;
    ensure!(
        page.prefixes.len() <= options.limit,
        "store exceeded census page bound"
    );
    let scanned = page.prefixes.len();
    let next = encode(page.page_token, &scope)?;
    let candidates =
        crate::cell_cli::cell_scopes_from_prefixes(page.prefixes, options.class.as_deref(), None)
            .filter(|scope| !crate::application::event_stream::is_event_scope(scope))
            .collect::<Vec<_>>();
    let mut probes = stream::iter(
        candidates
            .into_iter()
            .map(|scope| probe(&storage, options.unsafe_public_advertise, scope)),
    )
    .buffered(4);
    let mut records = Vec::new();
    while let Some(row) = probes.next().await {
        records.push(row);
    }
    let unavailable = records
        .iter()
        .filter(|row| row["status"] == "unavailable")
        .count();
    let result = json!({"format":1,"scanned_prefixes":scanned,"records":records,"next":next,"complete_probes":unavailable==0});
    let mut output = Output::new(if options.json {
        Format::Json
    } else {
        Format::Text
    });
    output.row(&Reply(result))?;
    output.finish()?;
    // Preserve usable rows/cursor while making partial failure visible to
    // automation. A successful exit cannot stand for unobserved source state.
    ensure!(unavailable==0,"{unavailable} census source probes unavailable; partial page and continuation were printed");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn census_bounds_and_source_selection() {
        for arguments in [
            vec!["--limit", "0"],
            vec!["--limit", "101"],
            vec!["Order:one"],
            vec!["__EventStream"],
            vec!["--all"],
        ] {
            assert!(parse(arguments.into_iter().map(str::to_owned).collect()).is_err());
        }
        let options = parse(vec!["Order".into(), "--limit".into(), "1".into()])
            .unwrap()
            .unwrap();
        assert_eq!(options.class.as_deref(), Some("Order"));
    }

    #[test]
    fn cursor_binds_fleet_prefix_and_class_without_assuming_provider_order() {
        let cursor = encode(Some("opaque/provider-token".into()), "a")
            .unwrap()
            .unwrap();
        assert_eq!(
            decode(Some(&cursor), "a").unwrap().as_deref(),
            Some("opaque/provider-token")
        );
        assert!(decode(Some(&cursor), "b").is_err());
        assert!(decode(Some("census2:abc"), "a").is_err());
        assert!(encode(Some("a".repeat(8193)), "a").is_err());
    }
}
