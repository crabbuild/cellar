//! Process-boundary parsing for platform namespace inventory.

use super::{Fleet, FleetFlags, Format, Output, Reachable, Reply, Subject, FLEET_HELP};
use crate::application::event_stream::CatalogNamespace;
use crate::runtime::catalog_control::{Request, MAX_REQUEST_BYTES};
use anyhow::{bail, Context, Result};
use std::time::Duration;

const HELP: &str =
    "cellar stream list --tenant TENANT --application APPLICATION [--cursor TOKEN] [--limit N]

Lists catalog identity/lifecycle metadata, including retained deleted resources.
Requires fleet administration, separately from an application's stream grants.
Pages contain at most 100 records; namespace-scoped cursors observe live state.
Configuration grants, credentials and event bodies are not returned.";

pub(super) async fn run(arguments: Vec<String>) -> Result<()> {
    let mut args = arguments.into_iter();
    let mut tenant = None;
    let mut application = None;
    let mut cursor = None;
    let mut limit = 100;
    let mut fleet = FleetFlags::default();
    let mut json = false;
    let mut unsafe_public_advertise = false;
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--help" | "-h" | "help" => {
                return Output::new(Format::Text).line(format_args!("{HELP}\n\n{FLEET_HELP}"))
            }
            "--json" => {
                json = true;
                continue;
            }
            "--unsafe-public-advertise" => {
                unsafe_public_advertise = true;
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
            "--tenant" => tenant = Some(value(&flag)?),
            "--application" => application = Some(value(&flag)?),
            "--cursor" => cursor = Some(value(&flag)?),
            "--limit" => limit = value(&flag)?.parse().context("invalid catalog limit")?,
            _ => bail!("invalid catalog list option {flag}"),
        }
    }
    let request = Request {
        namespace: CatalogNamespace {
            tenant: tenant.context("catalog list requires --tenant")?,
            application: application.context("catalog list requires --application")?,
        },
        cursor,
        limit,
    };
    request.prepare("catalogs-operator".into())?;
    anyhow::ensure!(
        serde_json::to_vec(&request)?.len() <= MAX_REQUEST_BYTES,
        "catalog operator request exceeds limit"
    );
    let storage = fleet.resolve("cellar stream list")?;
    let target = Reachable::open_catalog(
        Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise,
        },
        Subject {
            noun: "catalogs",
            source: "catalogs-operator",
            timeout: Duration::from_secs(60),
        },
        request.scope()?,
    )
    .await?;
    let reply = target.call(serde_json::to_value(request)?).await?;
    let mut output = Output::new(if json { Format::Json } else { Format::Text });
    output.row(&Reply(reply))?;
    output.finish()
}
