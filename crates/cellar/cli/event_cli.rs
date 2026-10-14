//! Bounded fleet state collection from independently routed cold or warm owners.
#![allow(clippy::disallowed_methods)]
use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::event_state::{StateSample, StateTotals};
use crate::operator_cell::{Fleet, Reachable, Subject};
use anyhow::{ensure, Result};
use futures_util::{stream, StreamExt};
use std::time::Duration;

#[path = "event_inventory.rs"]
mod inventory;

const HELP: &str = "cellar event inventory [SCOPE] [--page resources|producers|history] [--cursor TOKEN] [--limit N] [--json]\ncellar event metrics [--max-roots N] [--json]\n\nCollect current root-cell durable state through signed owner RPCs.\nAt most 100 provider prefixes per page and four concurrent probes.\nDefault bound: 10,000 roots/pages; hard bound: 100,000.\nCold inspection may restore cells and progress existing work.\nResults are live observations across sample times, not a snapshot or drain proof.\nFailed probes or exhausted bounds yield complete=false and a failed exit.\nPhysical bytes cover observed root SQLite pages, excluding facets/WAL/LTX/bucket.\nJSON uses exact decimal gauges; default output is Prometheus text without identities.";

struct Options {
    fleet: FleetFlags,
    max_roots: usize,
    json: bool,
    unsafe_public_advertise: bool,
}

fn parse(arguments: Vec<String>) -> Result<Option<Options>> {
    let mut args = arguments.into_iter();
    let Some(verb) = args.next() else {
        return Ok(None);
    };
    if matches!(verb.as_str(), "help" | "--help" | "-h") {
        return Ok(None);
    }
    ensure!(verb == "metrics", "unknown event command");
    let mut options = Options {
        fleet: FleetFlags::default(),
        max_roots: 10_000,
        json: false,
        unsafe_public_advertise: false,
    };
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--help" | "-h" => return Ok(None),
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
                .ok_or_else(|| anyhow::anyhow!("{name} requires a value"))
        };
        if options.fleet.consume(&flag, &mut value)? {
            continue;
        }
        match flag.as_str() {
            "--max-roots" => options.max_roots = value(&flag)?.parse()?,
            _ => anyhow::bail!("invalid event metrics option {flag}"),
        }
    }
    ensure!(
        (1..=100_000).contains(&options.max_roots),
        "event metrics root/page bound must be 1..=100000"
    );
    Ok(Some(options))
}

async fn probe(
    storage: &crate::cli_options::Storage,
    unsafe_public_advertise: bool,
    scope: &str,
) -> Result<StateSample> {
    let target = Reachable::open_state(
        Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise,
        },
        Subject {
            noun: "event state",
            source: "event-metrics",
            timeout: Duration::from_secs(15),
        },
        scope.into(),
    )
    .await?;
    let sample: StateSample = serde_json::from_value(target.call(serde_json::json!({})).await?)?;
    sample.values(scope)?;
    Ok(sample)
}

pub async fn run(arguments: Vec<String>) -> Result<()> {
    if arguments.first().is_some_and(|s| s == "inventory") {
        return inventory::run(arguments.into_iter().skip(1).collect()).await;
    }
    use std::io::Write;
    let Some(options) = parse(arguments)? else {
        return crate::cli_output::Output::new(crate::cli_output::Format::Text)
            .line(format_args!("{HELP}\n\n{FLEET_HELP}"));
    };
    let storage = options.fleet.resolve("cellar event metrics")?;
    let bucket = storage.open().await?;
    let mut token = None;
    let mut scanned = 0usize;
    let mut pages = 0usize;
    let mut totals = StateTotals::default();
    let mut seen = std::collections::HashSet::new();
    let mut exhausted = false;
    let mut listing_failed = false;
    loop {
        if scanned >= options.max_roots || pages >= options.max_roots {
            break;
        }
        let limit = (options.max_roots - scanned).min(100);
        let page = match bucket
            .common_prefixes_page("cells/", None, token.clone(), limit)
            .await
        {
            Ok(page) => page,
            Err(_) => {
                listing_failed = true;
                break;
            }
        };
        if page.prefixes.len() > limit {
            listing_failed = true;
            break;
        }
        pages += 1;
        scanned += page.prefixes.len();
        let count = page.prefixes.len();
        let scopes = crate::cell_cli::cell_scopes_from_prefixes(page.prefixes, None, None)
            .collect::<Vec<_>>();
        // Invalid directory entries are unobserved, never silently empty cells.
        totals.failed += count - scopes.len();
        let scopes = scopes
            .into_iter()
            .filter(|scope| {
                if seen.insert(scope.clone()) {
                    true
                } else {
                    totals.failed += 1;
                    false
                }
            })
            .collect::<Vec<_>>();
        let mut probes = stream::iter(scopes.into_iter().map(|scope| async {
            let result = probe(&storage, options.unsafe_public_advertise, &scope).await;
            (scope, result)
        }))
        .buffered(4);
        while let Some((scope, result)) = probes.next().await {
            match result.and_then(|sample| totals.add(&sample, &scope)) {
                Ok(()) => {}
                Err(_) => totals.failed += 1,
            }
        }
        let next = page.page_token;
        if next.is_none() {
            exhausted = true;
            break;
        }
        if next == token {
            listing_failed = true;
            break;
        }
        token = next;
    }
    let complete = exhausted && !listing_failed && totals.failed == 0;
    let text = if options.json {
        let mut value = totals.json(complete);
        value["scanned_prefixes"] = serde_json::json!(scanned);
        value["pages"] = serde_json::json!(pages);
        value["directory_exhausted"] = serde_json::json!(exhausted);
        value["listing_failed"] = serde_json::json!(listing_failed);
        serde_json::to_string(&value)? + "\n"
    } else {
        totals.prometheus(complete)
    };
    std::io::stdout().lock().write_all(text.as_bytes())?;
    ensure!(
        complete,
        "event metrics collection incomplete; observed totals and coverage were printed"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_bounds_cannot_be_unrestricted() {
        for n in ["0", "100001", "-1"] {
            assert!(parse(vec!["metrics".into(), "--max-roots".into(), n.into()]).is_err());
        }
        assert!(parse(vec!["metrics".into(), "--all".into()]).is_err());
        assert!(
            parse(vec!["metrics".into(), "--json".into()])
                .unwrap()
                .unwrap()
                .json
        );
    }
}
